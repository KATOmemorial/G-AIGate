package mock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scripted failures complement real TCP tests by proving that production stops
// at the exact failed write/flush, even before the request context is canceled.
type failingStreamWriter struct {
	header               http.Header
	writes, flushes      int
	failWrite, failFlush int
	shortWrite           bool
	err                  error
	onFlush              func()
	data                 bytes.Buffer
}

func (w *failingStreamWriter) Header() http.Header { return w.header }
func (w *failingStreamWriter) WriteHeader(int)     {}

func (w *failingStreamWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.writes == w.failWrite {
		if w.shortWrite {
			return len(data) - 1, nil
		}
		return 0, w.err
	}
	return w.data.Write(data)
}

func (w *failingStreamWriter) FlushError() error {
	w.flushes++
	if w.flushes == w.failFlush {
		return w.err
	}
	if w.onFlush != nil {
		w.onFlush()
	}
	return nil
}

func TestChatProducerStopsOnWriteAndFlushFailure(t *testing.T) {
	failed := errors.New("scripted transport failure")
	for _, phase := range []string{"write", "flush", "short write"} {
		for _, failAt := range []int{1, 2, 4, 5} { // content, content, usage, DONE
			t.Run(fmt.Sprintf("%s/%d", phase, failAt), func(t *testing.T) {
				w := &failingStreamWriter{header: make(http.Header), err: failed}
				if phase == "flush" {
					w.failFlush = failAt
				} else {
					w.failWrite, w.shortWrite = failAt, phase == "short write"
				}
				c, _ := gin.CreateTestContext(w)
				p := DefaultScenarioParams()
				p.Chunks, p.ChunkInterval = 3, 0
				written, err := streamChat(t.Context(), c.Writer, completionMetadata{ID: "chatcmpl-test", Model: "test"}, true, p, false)
				if phase == "short write" {
					require.ErrorIs(t, err, io.ErrShortWrite)
				} else {
					require.ErrorIs(t, err, failed)
				}
				assert.Equal(t, min(failAt-1, p.Chunks), written)
				assert.Equal(t, failAt, w.writes, "no writes after the failed frame")
				wantFlushes := failAt - 1
				if phase == "flush" {
					wantFlushes = failAt
				}
				assert.Equal(t, wantFlushes, w.flushes)
				if failAt < 5 {
					assert.NotContains(t, w.data.String(), "[DONE]")
				}
			})
		}
	}
}

func TestChatRunRecordsExecutionOutcome(t *testing.T) {
	for _, phase := range []string{"completed", "canceled", "error", "interrupted"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			w := &failingStreamWriter{header: make(http.Header), err: errors.New("flush failed")}
			scenario := Normal
			switch phase {
			case "canceled":
				w.onFlush = cancel
			case "error":
				w.failFlush = 2
			case "interrupted":
				scenario = Interrupted
			}
			e := NewEngine()
			p := DefaultScenarioParams()
			p.Chunks, p.FailAfterChunks, p.ChunkInterval = 3, 1, 0
			require.NoError(t, e.Set("sk-test", scenario, p))
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test","messages":[{}],"stream":true}`))
			c.Request.Header.Set("Content-Type", "application/json")
			c.Request.Header.Set("Authorization", "Bearer sk-test")
			run := chatRun{meta: completionMetadata{ID: "chatcmpl-test"}, outcome: "error"}
			if phase == "interrupted" {
				require.PanicsWithValue(t, http.ErrAbortHandler, func() { run.serve(c, e) })
			} else {
				run.serve(c, e)
			}
			assert.Equal(t, phase, run.outcome)
			wantChunks := 1
			if phase == "completed" {
				wantChunks = 3
			}
			assert.Equal(t, wantChunks, run.chunksWritten)
			assert.Equal(t, scenario, run.scenario)
		})
	}
}
