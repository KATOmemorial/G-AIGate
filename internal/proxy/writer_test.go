package proxy_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestForwardGinPreservesStateAndBottomFlushErrors(t *testing.T) {
	for _, failFlush := range []int{0, 1, 2} {
		t.Run(string(rune('0'+failFlush)), func(t *testing.T) {
			bottom := newOutput()
			bottom.failFlush, bottom.cause = failFlush, io.ErrClosedPipe
			c, _ := gin.CreateTestContext(bottom)
			c.Writer.WriteHeader(http.StatusAccepted) // Gin stages only; no real commitment.
			require.False(t, c.Writer.Written())
			require.Zero(t, bottom.status)
			_, hasUnwrap := c.Writer.(interface{ Unwrap() http.ResponseWriter })
			require.True(t, hasUnwrap, "actual Gin concrete writer exposes Unwrap")
			s := scripted(chunk(`{}`), done())
			result, err := proxy.NewForwarder().Forward(t.Context(), c.Writer, s.open, options, nil)
			require.True(t, result.WroteHeader)
			require.True(t, c.Writer.Written())
			require.Equal(t, http.StatusOK, c.Writer.Status())
			require.Equal(t, bottom.body.Len(), c.Writer.Size(), "body must pass through Gin accounting")
			require.Equal(t, http.StatusOK, bottom.status)
			if failFlush == 0 {
				require.NoError(t, err)
				require.Equal(t, 2, bottom.flushes)
				require.Equal(t, proxy.ResultSuccess, result.Kind)
			} else {
				require.ErrorIs(t, err, io.ErrClosedPipe)
				require.Equal(t, failFlush, bottom.flushes)
				require.Equal(t, failFlush, bottom.writes)
				require.Equal(t, proxy.ResultWriteError, result.Kind)
			}
			require.Equal(t, 1, s.closes)
		})
	}
}

func TestForwardRealWrapWriterRejectsBeforeCommitAndHandlerCanMapError(t *testing.T) {
	bottom := newOutput()
	bottom.failFlush, bottom.cause = 1, io.ErrClosedPipe
	c, _ := gin.CreateTestContext(bottom)
	c.Writer = middleware.WrapWriter(c.Writer)
	_, hasUnwrap := c.Writer.(interface{ Unwrap() http.ResponseWriter })
	require.False(t, hasUnwrap, "real wrapper embeds an interface lacking Unwrap")
	c.Writer.WriteHeader(http.StatusAccepted)
	require.False(t, c.Writer.Written())
	calls := 0
	s := scripted(done())
	result, err := proxy.NewForwarder().Forward(t.Context(), c.Writer, func(ctx context.Context) (provider.Stream, error) { calls++; return s.open(ctx) }, options, nil)
	require.ErrorIs(t, err, http.ErrNotSupported)
	assertUncommitted(t, bottom, result)
	require.Zero(t, calls)
	require.False(t, c.Writer.Written())
	require.Equal(t, -1, c.Writer.Size())
	require.Zero(t, s.closes)
	// This is a negative capability test, not a successful wrapped forwarding.
	c.JSON(http.StatusInternalServerError, gin.H{"error": "writer capability missing"})
	require.Equal(t, http.StatusInternalServerError, bottom.status)
	require.JSONEq(t, `{"error":"writer capability missing"}`, bottom.body.String())
}

func TestForwardGinAccessLogUsesActualWriteStatus(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			a.Key = "ts"
		}
		return a
	}}))
	router := gin.New()
	router.Use(middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover())
	var result proxy.ForwardResult
	var forwardErr error
	router.GET("/stream", func(c *gin.Context) {
		result, forwardErr = proxy.NewForwarder().Forward(c.Request.Context(), c.Writer, scripted(chunk(`{}`), done()).open, options, nil)
	})
	bottom := newOutput()
	bottom.failFlush, bottom.cause = 1, io.ErrClosedPipe
	router.ServeHTTP(bottom, httptest.NewRequest(http.MethodGet, "/stream", nil))
	require.ErrorIs(t, forwardErr, io.ErrClosedPipe)
	require.True(t, result.WroteHeader)
	require.Equal(t, 1, bottom.writes)
	require.Equal(t, 1, bottom.flushes)
	require.Contains(t, logs.String(), `"msg":"request_done"`)
	require.Contains(t, logs.String(), `"status_code":200`)
	require.Contains(t, logs.String(), `"status_class":"2xx"`)
	require.Contains(t, logs.String(), `"ts":`)
	require.Equal(t, 1, bytes.Count(logs.Bytes(), []byte(`"msg":"request_done"`)))
}

// Native HTTP/1.1 writers expose FlushError; Recorder's void Flush cannot
// substitute for that capability. This fixture validates the actual server.
func TestForwardNativeHTTPWriter(t *testing.T) {
	type outcome struct {
		result proxy.ForwardResult
		err    error
		closes int
	}
	finished := make(chan outcome, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := scripted(chunk("{\n \"n\":1\n}"), done())
		result, err := proxy.NewForwarder().Forward(r.Context(), w, s.open, options, nil)
		finished <- outcome{result, err, s.closes}
	}))
	defer server.Close()
	response, err := server.Client().Get(server.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, []string{"{\n \"n\":1\n}", "[DONE]"}, payloads(t, string(body)))
	require.Equal(t, "text/event-stream; charset=utf-8", response.Header.Get("Content-Type"))
	got := <-finished
	require.NoError(t, got.err)
	require.Equal(t, proxy.ResultSuccess, got.result.Kind)
	require.True(t, got.result.WroteHeader)
	require.Equal(t, 1, got.result.Chunks)
	require.Equal(t, 1, got.closes)
}
