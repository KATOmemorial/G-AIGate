// Package openai implements the frozen OpenAI-compatible HTTP subset (13.2).
package openai

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"gaigate/internal/provider"
)

type adapter struct {
	client       *http.Client
	streamBuffer int
}

// New copies client policy without modifying the caller or shared Transport.
// Context owns the attempt budget. A caller's cookie jar is not upstream auth.
func New(httpClient *http.Client, streamBuffer int) (provider.Provider, error) {
	if httpClient == nil || streamBuffer <= 0 {
		return nil, errors.New("openai: client and positive stream buffer are required")
	}
	client := *httpClient
	client.Timeout = 0
	client.Jar = nil
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &adapter{client: &client, streamBuffer: streamBuffer}, nil
}

func (*adapter) Name() string { return "openai" }

func (a *adapter) Chat(ctx context.Context, ep *provider.Endpoint, req *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
	resp, err := a.request(ctx, ep, req, false)
	if err != nil {
		return nil, nil, err
	}
	// Response read/validation determines the result; closing is cleanup only.
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	if err != nil {
		return nil, nil, transportError(err)
	}
	if len(body) > maxResponseBody {
		return nil, nil, protocolError(errors.New("openai: response exceeds 10 MiB"))
	}
	fields, err := jsonObject(body)
	if err != nil {
		return nil, nil, protocolError(err)
	}
	usage, err := parseUsage(fields)
	if err != nil {
		return nil, nil, protocolError(err)
	}
	return &provider.ChatResponse{Body: body, Usage: usage}, usage, nil
}

// TODO(13.2): M2 implements SSE parsing, bounded worker and waiting Close.
func (*adapter) ChatStream(context.Context, *provider.Endpoint, *provider.ChatRequest) (provider.Stream, error) {
	return nil, errors.New("openai: ChatStream is not implemented in M1 (13.2 M2 pending)")
}

// request transfers ownership of a successful response body to its caller.
func (a *adapter) request(ctx context.Context, ep *provider.Endpoint, req *provider.ChatRequest, stream bool) (*http.Response, error) {
	if ep == nil || req == nil {
		return nil, errors.New("openai: endpoint and request are required")
	}
	endpoint, err := completionURL(ep.BaseURL)
	if err != nil {
		return nil, err
	}
	body, err := serialize(ep, req, stream)
	if err != nil {
		return nil, fmt.Errorf("openai: serialize request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("openai: cannot construct HTTP request")
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+ep.APIKey)
	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, transportError(err)
	}
	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		return nil, httpError(resp, ep.APIKey)
	}
	expectedType := "application/json"
	if stream {
		expectedType = "text/event-stream"
	}
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != expectedType {
		_ = resp.Body.Close()
		return nil, protocolError(errors.New("openai: unexpected response Content-Type"))
	}
	return resp, nil
}
