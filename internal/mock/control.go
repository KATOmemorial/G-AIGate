package mock

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
)

const maxControlBody = 64 << 10

// The patch is decoded from present fields, starting from fresh defaults. The
// response is a separate DTO: neither durations nor the Engine's raw-key map
// cross the HTTP boundary in their internal representation.
type controlRequest struct {
	key      string
	scenario Scenario
	params   ScenarioParams
}

type controlParams struct {
	Chunks           int    `json:"chunks"`
	ChunkInterval    string `json:"chunk_interval"`
	TTFBDelay        string `json:"ttfb_delay"`
	DowntimeMode     string `json:"downtime_mode"`
	FailAfterChunks  int    `json:"fail_after_chunks"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
}

type controlResponse struct {
	KeyID     string        `json:"key_id"`
	KeyPrefix string        `json:"key_prefix"`
	Scenario  Scenario      `json:"scenario"`
	Params    controlParams `json:"params"`
}

// RegisterControlRoutes installs the control plane on the data plane's Engine.
// Set completes synchronously before a successful response is written.
func RegisterControlRoutes(r gin.IRoutes, e Engine) {
	r.PUT("/admin/scenario", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeMockError(c, http.StatusUnsupportedMediaType, "mock_unsupported_media_type", "Content-Type must be application/json")
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxControlBody)
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeMockError(c, http.StatusRequestEntityTooLarge, "mock_request_too_large", "request body exceeds 64KiB")
			} else {
				writeMockError(c, http.StatusBadRequest, "mock_invalid_request", "cannot read request body")
			}
			return
		}
		req, err := parseControlRequest(body)
		if err != nil {
			writeMockError(c, http.StatusBadRequest, "mock_invalid_request", "invalid scenario control request")
			return
		}
		if err := e.Set(req.key, req.scenario, req.params); err != nil {
			var invalid invalidConfigError
			if errors.As(err, &invalid) {
				writeMockError(c, http.StatusBadRequest, "mock_invalid_request", "invalid scenario or parameters")
			} else {
				// RequestLog records 5xx at Error level. Do not log the error's
				// text: an implementation could include the key or request data.
				writeMockError(c, http.StatusInternalServerError, "mock_internal", "cannot update scenario")
			}
			return
		}
		c.JSON(http.StatusOK, controlDTO(req.key, req.scenario, req.params))
	})
	r.GET("/admin/scenarios", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		snapshot := e.Snapshot()
		items := make([]controlResponse, 0, len(snapshot))
		for key, config := range snapshot {
			items = append(items, controlDTO(key, config.Scenario, config.Params))
		}
		sort.Slice(items, func(i, j int) bool { return items[i].KeyID < items[j].KeyID })
		c.JSON(http.StatusOK, struct {
			Scenarios []controlResponse `json:"scenarios"`
		}{Scenarios: items})
	})
}

func controlDTO(key string, s Scenario, p ScenarioParams) controlResponse {
	hash := sha256.Sum256([]byte(key))
	prefix := ""
	if len([]rune(key)) > 8 {
		prefix = string([]rune(key)[:8])
	}
	return controlResponse{
		KeyID: hex.EncodeToString(hash[:]), KeyPrefix: prefix, Scenario: s,
		Params: controlParams{
			Chunks: p.Chunks, ChunkInterval: p.ChunkInterval.String(), TTFBDelay: p.TTFBDelay.String(),
			DowntimeMode: p.DowntimeMode, FailAfterChunks: p.FailAfterChunks,
			PromptTokens: p.PromptTokens, CompletionTokens: p.CompletionTokens,
		},
	}
}

func parseControlRequest(body []byte) (controlRequest, error) {
	fields, err := controlObject(body, "key", "scenario", "params")
	if err != nil {
		return controlRequest{}, err
	}
	req := controlRequest{params: DefaultScenarioParams()}
	if err := json.Unmarshal(fields["key"], &req.key); err != nil {
		return controlRequest{}, fmt.Errorf("decode key: %w", err)
	}
	if err := json.Unmarshal(fields["scenario"], &req.scenario); err != nil {
		return controlRequest{}, fmt.Errorf("decode scenario: %w", err)
	}
	if raw, ok := fields["params"]; ok {
		if err := overlayControlParams(raw, &req.params); err != nil {
			return controlRequest{}, err
		}
	}
	if err := validateKey(req.key); err != nil {
		return controlRequest{}, err
	}
	if err := validateScenario(req.scenario, req.params); err != nil {
		return controlRequest{}, err
	}
	return req, nil
}

func overlayControlParams(raw []byte, p *ScenarioParams) error {
	fields, err := controlObject(raw, "chunks", "chunk_interval", "ttfb_delay", "downtime_mode", "fail_after_chunks", "prompt_tokens", "completion_tokens")
	if err != nil {
		return err
	}
	for field, value := range fields {
		switch field {
		case "chunks":
			err = json.Unmarshal(value, &p.Chunks)
		case "chunk_interval":
			err = controlDuration(value, &p.ChunkInterval)
		case "ttfb_delay":
			err = controlDuration(value, &p.TTFBDelay)
		case "downtime_mode":
			err = json.Unmarshal(value, &p.DowntimeMode)
		case "fail_after_chunks":
			err = json.Unmarshal(value, &p.FailAfterChunks)
		case "prompt_tokens":
			err = json.Unmarshal(value, &p.PromptTokens)
		case "completion_tokens":
			err = json.Unmarshal(value, &p.CompletionTokens)
		}
		if err != nil {
			return fmt.Errorf("decode control parameter: %w", err)
		}
	}
	return nil
}

func controlDuration(raw []byte, target *time.Duration) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	*target = d
	return nil
}

// Decode one strict object without losing duplicate keys or routing integers
// through float64. Escaped key spellings are compared after JSON decoding.
// This strictness is control-only; chat keeps its existing compatibility rules.
func controlObject(raw []byte, allowed ...string) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("control value must be an object")
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return nil, err
		}
		field, ok := token.(string)
		if !ok {
			return nil, errors.New("invalid object key")
		}
		if _, duplicate := fields[field]; duplicate {
			return nil, errors.New("duplicate control field")
		}
		known := false
		for _, name := range allowed {
			known = known || field == name
		}
		if !known {
			return nil, errors.New("unknown control field")
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errors.New("null control field")
		}
		fields[field] = value
	}
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("trailing control JSON")
	}
	return fields, nil
}
