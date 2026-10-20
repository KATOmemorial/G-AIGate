package openai

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"gaigate/internal/provider"
)

// Count every input byte of an event, including ignored lines and its blank
// delimiter (LF or CRLF). The reader has a separate fixed 4 KiB read-ahead bound.
const maxEventInput = 1 << 20

type sseParser struct{ reader *bufio.Reader }

func newSSEParser(r io.Reader) *sseParser {
	return &sseParser{reader: bufio.NewReaderSize(r, 4096)}
}

// next skips events without data. Each returned payload owns its storage.
// EOF never dispatches an unterminated event, including a partial [DONE].
func (p *sseParser) next() ([]byte, error) {
	var data []byte
	hasData, input := false, 0
	for {
		line, err := p.line(&input)
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			if hasData {
				return data, nil
			}
			input = 0
			continue
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		if !bytes.Equal(field, []byte("data")) {
			continue
		}
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		if hasData {
			data = append(data, '\n')
		}
		data = append(data, value...)
		hasData = true
	}
}

func (p *sseParser) line(input *int) ([]byte, error) {
	var line []byte
	for {
		part, err := p.reader.ReadSlice('\n')
		*input += len(part)
		if *input > maxEventInput {
			return nil, errors.New("openai: SSE event exceeds 1 MiB")
		}
		line = append(line, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("openai: read SSE event: %w", err)
		}
		line = line[:len(line)-1] // LF
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		return line, nil
	}
}

func parseEvent(raw []byte) (provider.StreamEvent, error) {
	if bytes.Equal(raw, []byte("[DONE]")) {
		return provider.StreamEvent{Kind: provider.Done, Raw: raw}, nil
	}
	fields, err := jsonObject(raw)
	if err != nil {
		return provider.StreamEvent{}, err
	}
	usage, err := parseUsage(fields)
	if err != nil {
		return provider.StreamEvent{}, err
	}
	if usage == nil {
		return provider.StreamEvent{Kind: provider.Chunk, Raw: raw}, nil
	}
	var choices []json.RawMessage
	if err := json.Unmarshal(fields["choices"], &choices); err != nil || choices == nil || len(choices) != 0 {
		return provider.StreamEvent{}, errors.New("openai: usage requires an independent choices=[] frame")
	}
	return provider.StreamEvent{Kind: provider.UsageDone, Raw: raw, Usage: usage}, nil
}
