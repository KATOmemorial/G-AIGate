package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSummaryPreservesContextAndFallback(t *testing.T) {
	base := WithModel(WithChannelID(WithUserID(context.Background(), 1), 2), "initial")
	user, channel, model := completedFields(base)
	assert.Equal(t, int64(1), user)
	assert.Equal(t, int64(2), channel)
	assert.Equal(t, "initial", model)
	ctx := withRequestSummary(base)
	child := WithModel(WithChannelID(WithUserID(ctx, 3), 4), "derived")
	assert.EqualValues(t, 1, UserIDFromContext(ctx))
	assert.EqualValues(t, 2, ChannelIDFromContext(ctx))
	assert.Equal(t, "initial", ModelFromContext(ctx))
	assert.EqualValues(t, 3, UserIDFromContext(child))
	assert.EqualValues(t, 4, ChannelIDFromContext(child))
	assert.Equal(t, "derived", ModelFromContext(child))
	user, channel, model = completedFields(ctx)
	assert.EqualValues(t, 3, user)
	assert.EqualValues(t, 4, channel)
	assert.Equal(t, "derived", model)
	independent := withRequestSummary(ctx)
	_ = WithChannelID(independent, 99)
	_, channel, _ = completedFields(ctx)
	assert.EqualValues(t, 4, channel, "a fresh request must not inherit the parent's mutable summary")
	_ = WithUserID(ctx, 0)
	user, _, _ = completedFields(ctx)
	assert.Zero(t, user, "explicit zero is also a summary update")
}

func TestSummaryConcurrentUpdatesAndSnapshots(t *testing.T) {
	ctx := withRequestSummary(context.Background())
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range 200 {
				child := WithModel(WithChannelID(WithUserID(ctx, int64(i)), int64(i)), "concurrent")
				_ = UserIDFromContext(child)
				_, _, _ = completedFields(ctx)
			}
		}()
	}
	workers.Wait()
	_ = WithModel(WithChannelID(WithUserID(ctx, 101), 202), "final")
	user, channel, model := completedFields(ctx)
	assert.EqualValues(t, 101, user)
	assert.EqualValues(t, 202, channel)
	assert.Equal(t, "final", model)
	assert.Zero(t, UserIDFromContext(ctx), "the summary is not an identity/context value source")
}

type unformattablePanic struct{}

func (unformattablePanic) String() string { panic("panic value must never be formatted") }

func TestRecoverAbortAndCommittedPanicRealHTTP(t *testing.T) {
	for _, value := range []any{http.ErrAbortHandler, "private-panic-credential", unformattablePanic{}, []string{"private-panic-credential"}} {
		for _, committed := range []bool{false, true} {
			t.Run(strings.Join([]string{panicCaseName(value), map[bool]string{true: "committed", false: "precommit"}[committed]}, "/"), func(t *testing.T) {
				var logs bytes.Buffer
				logger := newCapturingLogger(&logs)
				old := slog.Default()
				slog.SetDefault(logger)
				defer slog.SetDefault(old)
				r := gin.New()
				r.Use(RequestID(), AccessLog(logger), Recover())
				stats := make(chan struct {
					status, size int
					written      bool
				}, 1)
				r.Use(func(c *gin.Context) {
					c.Writer = WrapWriter(c.Writer)
					defer func() {
						stats <- struct {
							status, size int
							written      bool
						}{c.Writer.Status(), c.Writer.Size(), c.Writer.Written()}
					}()
					c.Next()
				})
				r.GET("/failure", func(c *gin.Context) {
					if committed {
						_, _ = c.Writer.WriteString("data: partial\n\n")
						c.Writer.Flush()
					}
					panic(value)
				})
				srv := httptest.NewServer(r)
				defer srv.Close()
				resp, err := srv.Client().Get(srv.URL + "/failure")
				isAbort := value == http.ErrAbortHandler
				if isAbort && !committed {
					require.Error(t, err)
					assert.Nil(t, resp)
				} else {
					require.NoError(t, err)
					body, readErr := io.ReadAll(resp.Body)
					require.NoError(t, resp.Body.Close())
					if committed {
						assert.Equal(t, 200, resp.StatusCode)
						assert.ErrorIs(t, readErr, io.ErrUnexpectedEOF)
						assert.Equal(t, "data: partial\n\n", string(body))
					} else {
						require.NoError(t, readErr)
						assert.Equal(t, 500, resp.StatusCode)
						var envelope map[string]any
						require.NoError(t, json.Unmarshal(body, &envelope))
						assert.Equal(t, resp.Header.Get("X-Request-Id"), envelope["request_id"])
					}
				}
				gotStats := <-stats
				if committed {
					assert.True(t, gotStats.written)
					assert.Equal(t, len("data: partial\n\n"), gotStats.size)
					assert.Equal(t, 200, gotStats.status)
				}
				// net/http has recovered the sentinel before closing the response.
				// Server.Close joins active handlers before inspecting shared logs.
				srv.Close()
				assert.Equal(t, 1, strings.Count(logs.String(), `"msg":"request_done"`))
				assert.NotContains(t, logs.String(), "private-panic-credential")
				assert.NotContains(t, logs.String(), "panic value must never be formatted")
				entry := findLogEntry(t, &logs, "request_done")
				assert.Contains(t, entry, "ts")
				assert.NotContains(t, entry, "time")
				if committed {
					assert.EqualValues(t, 200, entry["status_code"])
					assert.Equal(t, "2xx", entry["status_class"])
				}
				if isAbort {
					assert.Equal(t, "http_abort", findLogEntry(t, &logs, "request aborted")["reason"])
				} else {
					fault := findLogEntry(t, &logs, "panic recovered")
					assert.Equal(t, "panic", fault["reason"])
					assert.Contains(t, fault["stack"], "recover.go")
				}
			})
		}
	}
}

func panicCaseName(value any) string {
	switch value.(type) {
	case string:
		return "secret string"
	case unformattablePanic:
		return "Stringer"
	case []string:
		return "slice"
	default:
		return "ErrAbortHandler"
	}
}
