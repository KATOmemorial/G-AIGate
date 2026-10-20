package openai

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"

	"gaigate/internal/provider"
)

func completionURL(base string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(base, "#") {
		return "", errors.New("openai: invalid endpoint base URL")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", errors.New("openai: invalid endpoint port")
		}
	}
	// Identify the API root using decoded path semantics, while appending to the
	// escaped path so encoded prefixes (including encoded separators) survive.
	prefix := strings.TrimRight(u.EscapedPath(), "/")
	if !strings.HasSuffix(strings.TrimRight(u.Path, "/"), "/v1") {
		prefix += "/v1"
	}
	u.RawPath = prefix + "/chat/completions"
	u.Path, err = url.PathUnescape(u.RawPath)
	if err != nil {
		return "", errors.New("openai: invalid endpoint path")
	}
	return u.String(), nil
}

func serialize(ep *provider.Endpoint, req *provider.ChatRequest, stream bool) ([]byte, error) {
	if ep == nil || req == nil {
		return nil, errors.New("openai: endpoint and request are required")
	}
	fields := make(map[string]json.RawMessage, len(req.Extra)+3)
	for key, raw := range req.Extra {
		fields[key] = raw
	}
	for _, raw := range req.Messages {
		if _, err := jsonObject(raw); err != nil {
			return nil, errors.New("openai: each message must be a JSON object")
		}
	}
	model, err := json.Marshal(ep.Model)
	if err != nil {
		return nil, err
	}
	messages, err := json.Marshal(req.Messages)
	if err != nil {
		return nil, err
	}
	fields["model"], fields["messages"] = model, messages
	fields["stream"] = json.RawMessage("false")
	if stream {
		fields["stream"] = json.RawMessage("true")
		options := make(map[string]json.RawMessage)
		if raw, ok := fields["stream_options"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			options, err = jsonObject(raw)
			if err != nil {
				return nil, errors.New("openai: stream_options must be an object or null")
			}
		}
		options["include_usage"] = json.RawMessage("true")
		fields["stream_options"], err = json.Marshal(options)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(fields)
}
