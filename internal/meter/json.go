package meter

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"unicode/utf8"

	"gaigate/internal/provider"
)

// UseNumber preserves large JSON numbers; invalid input is never repaired with
// replacement characters. Errors deliberately exclude decoder diagnostics.
func decode(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, errInput
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if d.Decode(&v) != nil {
		return nil, errInput
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, errInput
	}
	return v, nil
}

func canonicalChars(v any) (int64, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return 0, errInput
	}
	return int64(utf8.RuneCount(b)), nil
}

func add(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, errOverflow
	}
	return a + b, nil
}

func valueChars(v any) (int64, error) {
	if s, ok := v.(string); ok {
		return int64(utf8.RuneCountInString(s)), nil
	}
	return canonicalChars(v)
}

func messageChars(m map[string]any) (int64, error) {
	var total int64
	for key, value := range m {
		if key == "role" || key == "index" || key == "finish_reason" {
			continue
		}
		var n int64
		var err error
		switch key {
		case "content":
			n, err = valueChars(value)
		case "tool_calls":
			n, err = toolChars(value)
		default:
			n, err = canonicalChars(value)
		}
		if err != nil {
			return 0, err
		}
		total, err = add(total, n)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func toolChars(v any) (int64, error) {
	calls, ok := v.([]any)
	if !ok {
		return canonicalChars(v)
	}
	var total int64
	for _, call := range calls {
		fields, ok := call.(map[string]any)
		if !ok {
			n, err := canonicalChars(call)
			if err != nil {
				return 0, err
			}
			total, err = add(total, n)
			if err != nil {
				return 0, err
			}
			continue
		}
		for key, value := range fields {
			if key == "index" || key == "id" || key == "type" {
				continue
			}
			var n int64
			var err error
			if fn, ok := value.(map[string]any); key == "function" && ok {
				for k, v := range fn {
					var part int64
					if k == "name" || k == "arguments" {
						part, err = valueChars(v)
					} else {
						part, err = canonicalChars(v)
					}
					if err != nil {
						return 0, err
					}
					n, err = add(n, part)
					if err != nil {
						return 0, err
					}
				}
			} else {
				n, err = canonicalChars(value)
			}
			if err != nil {
				return 0, err
			}
			total, err = add(total, n)
			if err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func outputChars(root map[string]any, field string) (int64, error) {
	v, exists := root["choices"]
	if !exists {
		return 0, nil
	}
	choices, ok := v.([]any)
	if !ok {
		return 0, errInput
	}
	var total int64
	for _, choice := range choices {
		c, ok := choice.(map[string]any)
		if !ok {
			return 0, errInput
		}
		v, exists := c[field]
		if !exists || v == nil {
			continue
		}
		m, ok := v.(map[string]any)
		if !ok {
			return 0, errInput
		}
		n, err := messageChars(m)
		if err != nil {
			return 0, err
		}
		total, err = add(total, n)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func bodyUsage(root map[string]any) (*provider.Usage, error) {
	v := root["usage"]
	if v == nil {
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errUsage
	}
	var u provider.Usage
	for key, target := range map[string]*int64{"prompt_tokens": &u.InputTokens, "completion_tokens": &u.OutputTokens} {
		n, ok := m[key].(json.Number)
		if !ok {
			return nil, errUsage
		}
		value, err := n.Int64()
		if err != nil {
			return nil, errUsage
		}
		*target = value
	}
	if !validUsage(u) {
		return nil, errUsage
	}
	return &u, nil
}

func object(raw []byte) (map[string]any, error) {
	v, err := decode(raw)
	if err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok || m == nil {
		return nil, errInput
	}
	return m, nil
}
