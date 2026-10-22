package channel

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
)

const adminBodyLimit = 64 * 1024

var (
	errBodyTooLarge = errors.New("channel: request body too large")
	errMediaType    = errors.New("channel: application/json required")
)

func readAdminObject(c *gin.Context, allowed, required []string) (map[string]json.RawMessage, error) {
	media, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || media != "application/json" {
		return nil, errMediaType
	}
	body := http.MaxBytesReader(c.Writer, c.Request.Body, adminBodyLimit)
	defer func() { _ = body.Close() }()
	raw, err := io.ReadAll(body)
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			return nil, errBodyTooLarge
		}
		return nil, errInvalidInput
	}
	return strictObject(raw, allowed, required)
}

// Tokens expose duplicate names after JSON unescaping. Use this at every
// management-object level; decoding straight into a struct would lose them.
func strictObject(raw []byte, allowed, required []string) (map[string]json.RawMessage, error) {
	if !utf8.Valid(raw) {
		return nil, errInvalidInput
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errInvalidInput
	}
	allow := make(map[string]bool, len(allowed))
	for _, field := range allowed {
		allow[field] = true
	}
	fields := make(map[string]json.RawMessage, len(allowed))
	for d.More() {
		token, err := d.Token()
		name, ok := token.(string)
		if err != nil || !ok || !allow[name] || fields[name] != nil {
			return nil, errInvalidInput
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errInvalidInput
		}
		fields[name] = value
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') {
		return nil, errInvalidInput
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return nil, errInvalidInput
	}
	for _, field := range required {
		if fields[field] == nil {
			return nil, errInvalidInput
		}
	}
	return fields, nil
}

func decodeField[T any](fields map[string]json.RawMessage, name string, value *T) error {
	if raw, present := fields[name]; present {
		if err := json.Unmarshal(raw, value); err != nil {
			return errInvalidInput
		}
	}
	return nil
}

var configFields = []string{"name", "provider", "base_url", "priority", "weight", "enabled", "models"}

func readChannelConfig(c *gin.Context, create bool) (CreateChannelInput, error) {
	allowed := append([]string(nil), configFields...)
	required := configFields
	if create {
		allowed = append(allowed, "keys")
		required = []string{"name", "provider", "base_url", "models", "keys"}
	}
	fields, err := readAdminObject(c, allowed, required)
	if err != nil {
		return CreateChannelInput{}, err
	}
	in := CreateChannelInput{Weight: 1, Enabled: true}
	for _, err := range []error{
		decodeField(fields, "name", &in.Name), decodeField(fields, "provider", &in.Provider),
		decodeField(fields, "base_url", &in.BaseURL), decodeField(fields, "priority", &in.Priority),
		decodeField(fields, "weight", &in.Weight), decodeField(fields, "enabled", &in.Enabled),
	} {
		if err != nil {
			return CreateChannelInput{}, err
		}
	}
	var models []json.RawMessage
	if err := json.Unmarshal(fields["models"], &models); err != nil || len(models) < 1 || len(models) > 64 {
		return CreateChannelInput{}, errInvalidInput
	}
	for _, raw := range models {
		m, err := strictObject(raw, []string{"model", "upstream_model"}, []string{"model", "upstream_model"})
		if err != nil {
			return CreateChannelInput{}, err
		}
		var mapping ModelMappingInput
		if decodeField(m, "model", &mapping.Model) != nil || decodeField(m, "upstream_model", &mapping.UpstreamModel) != nil {
			return CreateChannelInput{}, errInvalidInput
		}
		in.Models = append(in.Models, mapping)
	}
	if create {
		// Decode each string separately: encoding/json accepts null into a string.
		var keys []json.RawMessage
		if err := json.Unmarshal(fields["keys"], &keys); err != nil || len(keys) < 1 || len(keys) > 64 {
			return CreateChannelInput{}, errInvalidInput
		}
		for _, raw := range keys {
			var key string
			if json.Unmarshal(raw, &key) != nil || !validRawKey(key) {
				return CreateChannelInput{}, errInvalidInput
			}
			in.Keys = append(in.Keys, key)
		}
	}
	if err := validateConfig(updateInput(in)); err != nil {
		return CreateChannelInput{}, err
	}
	return in, nil
}

func updateInput(in CreateChannelInput) UpdateChannelInput {
	return UpdateChannelInput{Name: in.Name, Provider: in.Provider, BaseURL: in.BaseURL,
		Priority: in.Priority, Weight: in.Weight, Enabled: in.Enabled, Models: in.Models}
}
