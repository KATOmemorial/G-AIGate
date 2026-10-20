// Package provider defines the shared contracts for upstream providers (13.2).
// HTTP, parsing and request orchestration belong to adapter packages.
package provider

import (
	"context"
	"encoding/json"
)

// Endpoint is the selected channel, decrypted key and mapped upstream model.
type Endpoint struct {
	ChannelID int64
	Provider  string
	BaseURL   string
	APIKey    string
	Model     string
}

// Usage contains authoritative upstream token counts.
type Usage struct{ InputTokens, OutputTokens int64 }

// ChatRequest preserves message objects and unknown top-level JSON fields.
// Adapters treat the entire request, including RawMessage storage, as read-only.
type ChatRequest struct {
	Model    string
	Stream   bool
	Messages []json.RawMessage
	Extra    map[string]json.RawMessage
}

// ChatResponse retains the complete original upstream JSON bytes.
type ChatResponse struct {
	Body  json.RawMessage
	Usage *Usage
}

type StreamEventKind uint8

const (
	Chunk StreamEventKind = iota
	UsageDone
	Done
	Error
)

// StreamEvent separates content, usage, explicit completion and interruption.
type StreamEvent struct {
	Kind  StreamEventKind
	Raw   []byte
	Usage *Usage
	Err   error `json:"-"`
}

// Stream owns its producer; Close cancels, closes the body and waits for exit.
type Stream interface {
	Events() <-chan StreamEvent
	Close() error
}

// Provider performs one attempt without routing or automatic retries.
type Provider interface {
	Name() string
	Chat(context.Context, *Endpoint, *ChatRequest) (*ChatResponse, *Usage, error)
	ChatStream(context.Context, *Endpoint, *ChatRequest) (Stream, error)
}
