package mock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/oklog/ulid/v2"
)

const maxChatBody = 1 << 20

type chatRequest struct {
	model        string
	stream       bool
	includeUsage bool
}

type mockError struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error"`
}

type completionMetadata struct {
	ID      string `json:"id"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
}

type chatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type chatMessage struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content"`
}

type chatChoice struct {
	Index        int          `json:"index"`
	Delta        *chatMessage `json:"delta,omitempty"`
	Message      *chatMessage `json:"message,omitempty"`
	FinishReason *string      `json:"finish_reason"`
}

type chatCompletion struct {
	completionMetadata
	Object  string       `json:"object"`
	Choices []chatChoice `json:"choices"`
	Usage   *chatUsage   `json:"usage"`
}

// chatRun holds request-local metadata and execution results. It is never stored
// in Engine; one resolved parameter value is used throughout the request.
type chatRun struct {
	meta          completionMetadata
	scenario      Scenario
	chunksWritten int
	outcome       string
}

// RegisterChatRoutes installs only the chat data plane. Use gin.New without a
// recovery middleware that swallows http.ErrAbortHandler (12.1 §3.5).
func RegisterChatRoutes(r gin.IRoutes, e Engine) {
	r.POST("/v1/chat/completions", func(c *gin.Context) {
		started := time.Now()
		run := chatRun{
			meta: completionMetadata{
				ID: "chatcmpl-" + ulid.Make().String(), Created: started.Unix(),
			},
			outcome: "error",
		}
		run.serve(c, e)
	})
}

func (run *chatRun) serve(c *gin.Context, e Engine) {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeMockError(c, http.StatusUnsupportedMediaType, "mock_unsupported_media_type", "Content-Type must be application/json")
		return
	}
	key, err := bearerKey(c.GetHeader("Authorization"))
	if err != nil {
		writeMockError(c, http.StatusUnauthorized, "mock_unauthorized", "a valid Bearer test key is required")
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxChatBody)
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeMockError(c, http.StatusRequestEntityTooLarge, "mock_request_too_large", "request body exceeds 1MiB")
		} else if !run.canceled(c.Request.Context()) {
			writeMockError(c, http.StatusBadRequest, "mock_invalid_request", "cannot read request body")
		}
		return
	}
	req, err := parseChatRequest(body)
	if err != nil {
		writeMockError(c, http.StatusBadRequest, "mock_invalid_request", "invalid chat request")
		return
	}
	run.meta.Model = req.model
	s, p, err := e.Resolve(key, c.GetHeader("X-Mock-Scenario"))
	if err != nil {
		writeMockError(c, http.StatusBadRequest, "mock_invalid_request", "invalid scenario or parameters")
		return
	}
	run.scenario = s
	ctx := c.Request.Context()
	if run.canceled(ctx) {
		return
	}
	switch s {
	case Slow:
		if err := waitContext(ctx, p.TTFBDelay); err != nil {
			run.canceled(ctx)
			return
		}
	case Downtime:
		if p.DowntimeMode == "refuse" {
			run.abort()
		}
		writeMockError(c, http.StatusServiceUnavailable, "mock_unavailable", "mock upstream unavailable")
		return
	case RateLimited:
		c.Header("Retry-After", "1")
		writeMockError(c, http.StatusTooManyRequests, "mock_rate_limited", "mock upstream rate limited")
		return
	case Interrupted:
		if !req.stream {
			run.abort()
		}
	}
	if req.stream {
		run.chunksWritten, err = streamChat(ctx, c.Writer, run.meta, req.includeUsage, p, s == Interrupted)
	} else {
		err = writeCompletion(ctx, c.Writer, run.meta, p)
	}
	if err != nil {
		run.canceled(ctx)
		return
	}
	if s == Interrupted {
		run.abort()
	}
	run.outcome = "completed"
}

func (run *chatRun) canceled(ctx context.Context) bool {
	if ctx.Err() != nil {
		run.outcome = "canceled"
		return true
	}
	return false
}

func (run *chatRun) abort() {
	run.outcome = "interrupted"
	// net/http catches this sentinel and closes only the current HTTP/1.1
	// connection, omitting the terminating chunk and suppressing a panic stack.
	panic(http.ErrAbortHandler)
}

func bearerKey(header string) (string, error) {
	scheme, key, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", errors.New("invalid Bearer authorization")
	}
	if err := validateKey(key); err != nil {
		return "", fmt.Errorf("invalid Bearer key: %w", err)
	}
	return key, nil
}

func parseChatRequest(body []byte) (chatRequest, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return chatRequest{}, fmt.Errorf("decode chat object: %w", err)
	}
	var req chatRequest
	if fields == nil || json.Unmarshal(fields["model"], &req.model) != nil || strings.TrimSpace(req.model) == "" {
		return chatRequest{}, errors.New("model must be a nonempty string")
	}
	var messages []map[string]json.RawMessage
	if err := json.Unmarshal(fields["messages"], &messages); err != nil {
		return chatRequest{}, fmt.Errorf("decode messages: %w", err)
	}
	if len(messages) == 0 {
		return chatRequest{}, errors.New("messages must be a nonempty object array")
	}
	for _, message := range messages {
		if message == nil {
			return chatRequest{}, errors.New("each message must be an object")
		}
	}
	if raw, ok := fields["stream"]; ok {
		if err := parseBool(raw, &req.stream); err != nil {
			return chatRequest{}, fmt.Errorf("decode stream: %w", err)
		}
	}
	if raw, ok := fields["stream_options"]; ok {
		var options map[string]json.RawMessage
		if err := json.Unmarshal(raw, &options); err != nil {
			return chatRequest{}, fmt.Errorf("decode stream_options: %w", err)
		}
		if options == nil {
			return chatRequest{}, errors.New("stream_options must be an object")
		}
		if raw, ok := options["include_usage"]; ok {
			if err := parseBool(raw, &req.includeUsage); err != nil {
				return chatRequest{}, fmt.Errorf("decode include_usage: %w", err)
			}
		}
	}
	return req, nil
}

func parseBool(raw json.RawMessage, target *bool) error {
	var value *bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	if value == nil {
		return errors.New("null is not a boolean")
	}
	*target = *value
	return nil
}

func writeMockError(c *gin.Context, status int, code, message string) {
	var response mockError
	response.Error.Message, response.Error.Type, response.Error.Code = message, "mock_error", code
	c.Header("Content-Type", "application/json; charset=utf-8")
	c.Status(status)
	// A write failure ends this handler; never attempt another response.
	if err := json.NewEncoder(c.Writer).Encode(response); err != nil {
		c.Abort()
	}
}

func usageFor(p ScenarioParams) *chatUsage {
	return &chatUsage{PromptTokens: p.PromptTokens, CompletionTokens: p.CompletionTokens, TotalTokens: p.PromptTokens + p.CompletionTokens}
}

func writeCompletion(ctx context.Context, w http.ResponseWriter, meta completionMetadata, p ScenarioParams) error {
	var content strings.Builder
	for i := 0; i < p.Chunks; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		fmt.Fprintf(&content, "mock-%d ", i+1)
	}
	stop := "stop"
	response := chatCompletion{
		completionMetadata: meta, Object: "chat.completion",
		Choices: []chatChoice{{Index: 0, Message: &chatMessage{Role: "assistant", Content: content.String()}, FinishReason: &stop}},
		Usage:   usageFor(p),
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return json.NewEncoder(w).Encode(response)
}

func streamChat(ctx context.Context, w http.ResponseWriter, meta completionMetadata, includeUsage bool, p ScenarioParams, interrupted bool) (int, error) {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	limit := p.Chunks
	if interrupted {
		limit = p.FailAfterChunks
	}
	written := 0
	for i := 0; i < limit; i++ {
		if i > 0 {
			if err := waitContext(ctx, p.ChunkInterval); err != nil {
				return written, err
			}
		}
		delta := &chatMessage{Content: fmt.Sprintf("mock-%d ", i+1)}
		if i == 0 {
			delta.Role = "assistant"
		}
		choice := chatChoice{Index: 0, Delta: delta}
		if !interrupted && i == p.Chunks-1 {
			stop := "stop"
			choice.FinishReason = &stop
		}
		frame := chatCompletion{completionMetadata: meta, Object: "chat.completion.chunk", Choices: []chatChoice{choice}}
		if err := writeSSE(ctx, w, frame); err != nil {
			return written, err
		}
		written++
	}
	if interrupted {
		return written, nil
	}
	if includeUsage {
		frame := chatCompletion{completionMetadata: meta, Object: "chat.completion.chunk", Choices: []chatChoice{}, Usage: usageFor(p)}
		if err := writeSSE(ctx, w, frame); err != nil {
			return written, err
		}
	}
	return written, writeSSEBytes(ctx, w, []byte("[DONE]"))
}

func writeSSE(ctx context.Context, w http.ResponseWriter, payload chatCompletion) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode SSE: %w", err)
	}
	return writeSSEBytes(ctx, w, data)
}

func writeSSEBytes(ctx context.Context, w http.ResponseWriter, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	frame := append(append([]byte("data: "), data...), '\n', '\n')
	n, err := w.Write(frame)
	if err != nil {
		return fmt.Errorf("write SSE: %w", err)
	}
	if n != len(frame) {
		return io.ErrShortWrite
	}
	// Gin's Flush() drops errors. Unwrap it before selecting FlushError on the
	// original net/http writer; the preceding Write already commits Gin's header.
	if unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter }); ok {
		w = unwrapper.Unwrap()
	}
	if err := http.NewResponseController(w).Flush(); err != nil {
		return fmt.Errorf("flush SSE: %w", err)
	}
	return nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if duration == 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
