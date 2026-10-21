package proxy_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/provider/openai"
	"gaigate/internal/proxy"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type byteCountBody struct {
	io.Reader
	read, closes int
}

func (b *byteCountBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}
func (b *byteCountBody) Close() error { b.closes++; return nil }

func TestNonStreamProviderBodyLimitThroughForward(t *testing.T) {
	const limit = 10 << 20
	for _, size := range []int{limit, limit + 4096} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			raw := `{"padding":"` + strings.Repeat("x", size-len(`{"padding":""}`)) + `"}`
			body := &byteCountBody{Reader: strings.NewReader(raw)}
			p, err := openai.New(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: body}, nil
			})}, 1)
			require.NoError(t, err)
			w := newDeadlineOutput("")
			result, err := proxy.NewForwarder().ForwardNonStream(t.Context(), writerChain(t, w.outputWriter, "WrapWriter"), func(ctx context.Context) (*provider.ChatResponse, *provider.Usage, error) {
				return p.Chat(ctx, &provider.Endpoint{BaseURL: "http://fixture", Model: "m"}, upstreamRequest())
			}, proxy.ForwardOptions{TTFBTimeout: 2 * time.Second, OverallTimeout: 3 * time.Second})
			require.Equal(t, 1, body.closes)
			require.Equal(t, min(size, limit+1), body.read, "oversize response only reads 10 MiB + 1")
			require.Zero(t, result.Chunks)
			require.Nil(t, result.Usage)
			if size == limit {
				require.NoError(t, err)
				require.Equal(t, proxy.ResultSuccess, result.Kind)
				require.Equal(t, raw, w.body.String())
				require.True(t, result.WroteHeader)
			} else {
				require.Error(t, err)
				var upstreamError *provider.UpstreamError
				require.ErrorAs(t, err, &upstreamError)
				require.Equal(t, proxy.ResultUpstreamError, result.Kind)
				assertUncommitted(t, w.outputWriter, result)
			}
		})
	}
}

func TestAttemptTTFBCoversConnectWait(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint(streaming), func(t *testing.T) {
			entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				defer close(exited)
				close(entered)
				// net/http detaches the dial context for connection reuse. The
				// fixture explicitly owns and joins it; no production worker added.
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-release:
					return nil, errors.New("fixture dial released")
				}
			}}
			defer transport.CloseIdleConnections()
			var releaseOnce bool
			defer func() {
				if !releaseOnce {
					close(release)
				}
				<-exited
			}()
			p, err := openai.New(&http.Client{Transport: transport}, 1)
			require.NoError(t, err)
			w := newDeadlineOutput("")
			var result proxy.ForwardResult
			var forwardErr error
			var group errgroup.Group
			started := time.Now()
			group.Go(func() error {
				opts := proxy.ForwardOptions{TTFBTimeout: 100 * time.Millisecond, OverallTimeout: time.Second}
				ep := &provider.Endpoint{BaseURL: "http://fixture", Model: "m"}
				if streaming {
					result, forwardErr = proxy.NewForwarder().Forward(t.Context(), w, func(ctx context.Context) (provider.Stream, error) { return p.ChatStream(ctx, ep, upstreamRequest()) }, opts, nil)
				} else {
					result, forwardErr = proxy.NewForwarder().ForwardNonStream(t.Context(), w, func(ctx context.Context) (*provider.ChatResponse, *provider.Usage, error) {
						return p.Chat(ctx, ep, upstreamRequest())
					}, opts)
				}
				return nil
			})
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("DialContext was not entered")
			}
			require.NoError(t, group.Wait())
			close(release)
			releaseOnce = true
			select {
			case <-exited:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("owned dial fixture did not exit")
			}
			require.ErrorIs(t, forwardErr, context.DeadlineExceeded)
			require.Equal(t, proxy.ResultTTFBTimeout, result.Kind)
			assertUncommitted(t, w.outputWriter, result)
			require.Less(t, time.Since(started)-100*time.Millisecond, 500*time.Millisecond)
			t.Logf("TTFB=100ms Overall=1s stage=connect actual=%s fixture dial explicitly joined", time.Since(started))
		})
	}
}
