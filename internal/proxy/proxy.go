// Package proxy forwards one upstream attempt without routing or retrying.
package proxy

import (
	"context"
	"net/http"
	"time"

	"gaigate/internal/provider"
)

type ResultKind string

const (
	ResultSuccess           ResultKind = "success"
	ResultUpstreamError     ResultKind = "upstream_error"
	ResultStreamInterrupted ResultKind = "stream_interrupted"
	ResultTTFBTimeout       ResultKind = "ttfb_timeout"
	ResultOverallTimeout    ResultKind = "overall_timeout"
	ResultClientCanceled    ResultKind = "client_canceled"
	ResultWriteError        ResultKind = "write_error"
)

type ForwardResult struct {
	Kind        ResultKind
	WroteHeader bool
	FirstByteMs int64
	Chunks      int
	Usage       *provider.Usage
}

type ForwardOptions struct {
	TTFBTimeout, OverallTimeout time.Duration
}

type StreamOpener func(context.Context) (provider.Stream, error)
type ResponseOpener func(context.Context) (*provider.ChatResponse, *provider.Usage, error)

type Forwarder interface {
	Forward(context.Context, http.ResponseWriter, StreamOpener, ForwardOptions,
		func(provider.StreamEvent)) (ForwardResult, error)
	ForwardNonStream(context.Context, http.ResponseWriter, ResponseOpener,
		ForwardOptions) (ForwardResult, error)
}

type forwarder struct{}

func NewForwarder() Forwarder { return &forwarder{} }

// ForwardNonStream is deliberately unavailable at the M1 checkpoint.
// TODO(15.1): Complete the direct Chat/JSON path and its validation in M2.
func (*forwarder) ForwardNonStream(context.Context, http.ResponseWriter, ResponseOpener,
	ForwardOptions) (ForwardResult, error) {
	return ForwardResult{Kind: ResultUpstreamError, FirstByteMs: -1},
		failure("proxy: non-stream forwarding is pending M2")
}
