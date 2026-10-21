package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"gaigate/internal/provider"
)

func (*forwarder) ForwardNonStream(ctx context.Context, w http.ResponseWriter, open ResponseOpener,
	opts ForwardOptions) (result ForwardResult, err error) {
	started := time.Now()
	result = ForwardResult{Kind: ResultUpstreamError, FirstByteMs: -1}
	if ctx == nil || nilValue(w) || open == nil || opts.TTFBTimeout <= 0 || opts.OverallTimeout <= 0 {
		return result, failure("proxy: invalid forwarding arguments")
	}
	if kind, cause := canceled(ctx); cause != nil {
		result.Kind = kind
		return result, cause
	}
	life := newLifetime(ctx, started, opts)
	defer life.finish(&result, &err)
	flush, err := errorFlusher(w)
	if err != nil {
		return result, err
	}
	downstream, err := newDownstream(w)
	if err != nil {
		return result, err
	}
	defer func() {
		life.closeUpstream()
		if cleanupErr := downstream.finish(life); cleanupErr != nil {
			if err == nil {
				result.Kind = ResultWriteError
			}
			err = failure("proxy: downstream cleanup failed", err, cleanupErr)
		}
	}()
	if kind, cause := life.check(); cause != nil {
		result.Kind = kind
		return result, cause
	}
	response, usage, err := open(life.upstream)
	if kind, cause := life.check(); cause != nil {
		result.Kind = kind
		return result, failure("proxy: opening ended after cancellation", cause, err)
	}
	if err != nil {
		return result, failure("proxy: upstream opening failed", err)
	}
	if !validResponse(response, usage) {
		return result, failure("proxy: invalid upstream JSON response", provider.ErrUpstreamUnavailable)
	}
	if err := life.ready(&result); err != nil {
		return result, err
	}
	if err := downstream.prepare(life); err != nil {
		return result, err
	}
	if kind, cause := life.check(); cause != nil {
		result.Kind = kind
		return result, cause
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Del("Content-Length")
	result.WroteHeader = true
	downstream.watch(life)
	w.WriteHeader(http.StatusOK)
	n, writeErr := w.Write(response.Body)
	if writeErr == nil && n != len(response.Body) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		if kind, cause := life.check(); cause != nil {
			result.Kind = kind
			return result, cause
		}
		writeErr = flush()
	}
	if writeErr != nil {
		result.Kind = ResultWriteError
		return result, failure("proxy: downstream output failed", writeErr)
	}
	result.Kind = ResultSuccess
	if usage != nil {
		copyUsage := *usage
		result.Usage = &copyUsage
	}
	return result, nil
}

// Check all three representations without changing Body or manufacturing usage.
// The provider normally guarantees this, but an injected opener is a boundary.
func validResponse(response *provider.ChatResponse, usage *provider.Usage) bool {
	if response == nil {
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(response.Body, &fields) != nil || fields == nil {
		return false
	}
	if raw, ok := fields["error"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}
	var bodyUsage *provider.Usage
	if raw, ok := fields["usage"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil || fields == nil {
			return false
		}
		counts := [2]int64{}
		for i, name := range []string{"prompt_tokens", "completion_tokens"} {
			var count *int64
			if json.Unmarshal(fields[name], &count) != nil || count == nil || *count < 0 {
				return false
			}
			counts[i] = *count
		}
		bodyUsage = &provider.Usage{InputTokens: counts[0], OutputTokens: counts[1]}
	}
	return equalUsage(bodyUsage, usage) && equalUsage(response.Usage, usage)
}

func equalUsage(a, b *provider.Usage) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
