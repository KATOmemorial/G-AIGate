package channel

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/middleware"
	"gaigate/internal/store"
)

type cancellationService struct {
	AdminService
	wait func(context.Context) error
}

func (s cancellationService) Get(ctx context.Context, _ int64) (*ChannelDetail, error) {
	if s.wait != nil {
		return nil, s.wait(ctx)
	}
	return httpFake().detail, nil
}

func (s cancellationService) Delete(ctx context.Context, _ int64) error {
	return s.wait(ctx)
}

// A write-half close leaves the client able to read the response. net/http,
// rather than an identity stub or test-injected cancel function, cancels the
// admitted server request after observing that real TCP EOF.
func TestAdminHTTPCanceledParentAbortsRealTCP(t *testing.T) {
	for _, mode := range []string{"probe error", "late probe success", "late delete success", "service error"} {
		t.Run(mode, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if len(groups) == 0 && a.Key == slog.TimeKey {
					a.Key = "ts"
				}
				return a
			}}))
			previous := slog.Default()
			slog.SetDefault(logger)
			defer slog.SetDefault(previous)

			entered := make(chan struct{}, 1)
			waitCanceled := func(ctx context.Context) error {
				entered <- struct{}{}
				timer := time.NewTimer(3 * time.Second)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
					return errors.New("cancellation watchdog expired")
				}
			}
			type completion struct {
				parentErr error
				written   bool
			}
			finished := make(chan completion, 1)
			r := gin.New()
			r.Use(func(c *gin.Context) {
				defer func() { finished <- completion{c.Request.Context().Err(), c.Writer.Written()} }()
				c.Next()
			}, middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover())
			r.Use(func(c *gin.Context) {
				c.Writer = middleware.WrapWriter(c.Writer)
				c.Next()
			})
			service := cancellationService{}
			method, path, body := "POST", "/1/test", `{"model":"a"}`
			switch mode {
			case "late delete success":
				method, path, body = "DELETE", "/1", ""
				service.wait = func(ctx context.Context) error { _ = waitCanceled(ctx); return nil }
			case "service error":
				method, path, body = "GET", "/1", ""
				service.wait = waitCanceled
			}
			tester := &testerFake{test: func(ctx context.Context, _ *store.Channel, _ string) (ProbeResult, error) {
				err := waitCanceled(ctx)
				if mode == "late probe success" {
					return ProbeResult{OK: true, StatusCode: 200, Reason: "success"}, nil
				}
				return ProbeResult{}, err
			}}
			RegisterAdminRoutes(r, func(c *gin.Context) { c.Next() }, func(context.Context) bool { return true }, service, tester)
			server := httptest.NewServer(r)
			defer server.Close()
			conn, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), time.Second)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
			_, err = fmt.Fprintf(conn, "%s %s%s HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", method, adminPrefix, path, len(body), body)
			require.NoError(t, err)
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("request did not enter service/tester before half close")
			}
			require.NoError(t, conn.(*net.TCPConn).CloseWrite())
			wire := &countingBody{reader: conn}
			response, readErr := http.ReadResponse(bufio.NewReader(wire), nil)
			if response != nil {
				_ = response.Body.Close()
			}
			require.Error(t, readErr, "cancellation must end the transport, not send a normal HTTP response")
			assert.Nil(t, response)
			assert.Zero(t, wire.read, "no status line, headers, JSON or implicit 200/204 may reach the client")
			var networkErr net.Error
			if errors.As(readErr, &networkErr) {
				assert.False(t, networkErr.Timeout(), "a client watchdog is not a server abort")
			}
			select {
			case result := <-finished:
				assert.ErrorIs(t, result.parentErr, context.Canceled)
				assert.False(t, result.written)
			case <-time.After(time.Second):
				t.Fatal("server handler did not finish after parent cancellation")
			}
			// finished is sent after AccessLog/Recover have finished unwinding;
			// reading this buffer therefore does not race with their writes.
			assert.Equal(t, 1, strings.Count(logs.String(), `"msg":"request aborted"`))
			assert.Equal(t, 1, strings.Count(logs.String(), `"msg":"request_done"`))
			assert.Contains(t, logs.String(), `"wrote_header":false`)
			t.Logf("TCP half close: canceled parent, zero HTTP bytes, transport=%v, handler joined", readErr)
		})
	}
}
