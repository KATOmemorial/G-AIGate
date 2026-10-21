package proxy_test

import (
	"io"
	"net/http"
	"testing"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestForwardRealWrapWriterOutputAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		write, short, flush, consumed, callbacks int
	}{
		{"success", 0, 0, 0, 4, 3},
		{"first Flush", 0, 0, 1, 1, 0},
		{"later Flush", 0, 0, 2, 2, 1},
		{"usage Flush", 0, 0, 3, 3, 2},
		{"Done Flush", 0, 0, 4, 4, 3},
		{"first Write", 1, 0, 0, 1, 0},
		{"later Write", 2, 0, 0, 2, 1},
		{"short Write", 0, 1, 0, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bottom := newOutput()
			bottom.failWrite, bottom.short, bottom.failFlush, bottom.cause = tc.write, tc.short, tc.flush, io.ErrClosedPipe
			c, _ := gin.CreateTestContext(bottom)
			ginWriter := c.Writer
			c.Writer = middleware.WrapWriter(c.Writer)
			require.Same(t, ginWriter, c.Writer.(interface{ Unwrap() http.ResponseWriter }).Unwrap())
			c.Writer.WriteHeader(http.StatusAccepted)
			require.False(t, ginWriter.Written())
			s := scripted(chunk(`{"n":1}`), chunk(`{"n":2}`), usage(`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`, 1, 2), done(), chunk(`{"unread":true}`))
			callbacks := 0
			result, err := proxy.NewForwarder().Forward(t.Context(), c.Writer, s.open, options, func(provider.StreamEvent) { callbacks++ })
			if tc.name == "success" {
				require.NoError(t, err)
				require.Equal(t, proxy.ResultSuccess, result.Kind)
				require.Equal(t, []string{`{"n":1}`, `{"n":2}`, `{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`, "[DONE]"}, payloads(t, bottom.body.String()))
			} else {
				require.Equal(t, proxy.ResultWriteError, result.Kind)
				if tc.short != 0 {
					require.ErrorIs(t, err, io.ErrShortWrite)
				} else {
					require.ErrorIs(t, err, io.ErrClosedPipe)
				}
			}
			require.True(t, result.WroteHeader)
			require.Equal(t, tc.callbacks, callbacks)
			require.Equal(t, min(tc.callbacks, 2), result.Chunks)
			require.Equal(t, tc.callbacks == 3, result.Usage != nil)
			require.Equal(t, 5-tc.consumed, len(s.events), "stop reading immediately after output failure")
			require.Equal(t, tc.consumed, bottom.writes)
			wantFlush := tc.consumed
			if tc.write != 0 || tc.short != 0 {
				wantFlush--
			}
			require.Equal(t, wantFlush, bottom.flushes)
			require.Equal(t, 1, s.closes)
			require.True(t, ginWriter.Written())
			require.Equal(t, bottom.body.Len(), ginWriter.Size())
			require.Equal(t, http.StatusOK, ginWriter.Status())
			require.Equal(t, http.StatusOK, bottom.status)
			if tc.consumed < 4 {
				require.NotContains(t, bottom.body.String(), "[DONE]")
			}
		})
	}
}
