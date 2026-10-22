package access

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"gaigate/internal/provider"
)

const chatBodyLimit = 10 * 1024 * 1024

var errChatInput = errors.New("invalid chat request")

func readChatRequest(c *gin.Context) (*provider.ChatRequest, int, string) {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, http.StatusUnsupportedMediaType, "gaigate_unsupported_media_type"
	}
	body := http.MaxBytesReader(c.Writer, c.Request.Body, chatBodyLimit)
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, http.StatusRequestEntityTooLarge, "gaigate_request_too_large"
		}
		return nil, http.StatusBadRequest, "gaigate_invalid_request"
	}
	req, err := decodeChatRequest(data)
	if err != nil {
		return nil, http.StatusBadRequest, "gaigate_invalid_request"
	}
	return req, 0, ""
}

// Tokenizing only the top level detects duplicate decoded field names without
// narrowing opaque message content or unknown extension values.
func decodeChatRequest(data []byte) (*provider.ChatRequest, error) {
	fields, err := uniqueJSONObject(data)
	if err != nil {
		return nil, err
	}
	req := &provider.ChatRequest{Extra: fields}
	if !nonemptyJSONString(fields["model"], &req.Model) {
		return nil, errChatInput
	}
	if value, exists := fields["stream"]; exists && !jsonBool(value, &req.Stream) {
		return nil, errChatInput
	}
	if err := json.Unmarshal(fields["messages"], &req.Messages); err != nil || len(req.Messages) == 0 {
		return nil, errChatInput
	}
	for _, raw := range req.Messages {
		var message map[string]json.RawMessage
		var role string
		if err := json.Unmarshal(raw, &message); err != nil || message == nil || !nonemptyJSONString(message["role"], &role) {
			return nil, errChatInput
		}
	}
	if raw, exists := fields["stream_options"]; exists && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		var options map[string]json.RawMessage
		if err := json.Unmarshal(raw, &options); err != nil || options == nil {
			return nil, errChatInput
		}
		var include bool
		if value, exists := options["include_usage"]; exists && !jsonBool(value, &include) {
			return nil, errChatInput
		}
	}
	delete(fields, "model")
	delete(fields, "stream")
	delete(fields, "messages")
	return req, nil
}

func nonemptyJSONString(raw json.RawMessage, value *string) bool {
	return json.Unmarshal(raw, value) == nil && *value != ""
}

func jsonBool(raw json.RawMessage, value *bool) bool {
	return !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && json.Unmarshal(raw, value) == nil
}

func uniqueJSONObject(data []byte) (map[string]json.RawMessage, error) {
	if !utf8.Valid(data) {
		return nil, errChatInput
	}
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errChatInput
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return nil, errChatInput
		}
		name, ok := token.(string)
		if !ok {
			return nil, errChatInput
		}
		if _, exists := fields[name]; exists {
			return nil, errChatInput
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, errChatInput
		}
		fields[name] = value
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') {
		return nil, errChatInput
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return nil, errChatInput
	}
	return fields, nil
}
