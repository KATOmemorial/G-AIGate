package mock_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gaigate/internal/mock"

	"github.com/gin-gonic/gin"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type logCapture struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	records chan map[string]any
}

func (capture *logCapture) Write(data []byte) (int, error) {
	capture.mu.Lock()
	n, err := capture.buffer.Write(data)
	capture.mu.Unlock()
	var record map[string]any
	if json.Unmarshal(data, &record) == nil && record["msg"] == "request_done" {
		capture.records <- record
	}
	return n, err
}

func (capture *logCapture) text() string {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return capture.buffer.String()
}

func loggedChatServer(t *testing.T, e mock.Engine) (*httptest.Server, *logCapture) {
	t.Helper()
	capture := &logCapture{records: make(chan map[string]any, 64)}
	r := gin.New()
	r.Use(mock.RequestLog(mock.NewLogger(capture)))
	mock.RegisterChatRoutes(r, e)
	s := httptest.NewServer(r)
	s.Client().Timeout = 3 * time.Second
	t.Cleanup(s.Close)
	return s, capture
}

func nextLog(t *testing.T, capture *logCapture) map[string]any {
	t.Helper()
	select {
	case record := <-capture.records:
		_, err := time.Parse(time.RFC3339Nano, record["ts"].(string))
		require.NoError(t, err)
		assert.NotContains(t, record, "time")
		id, ok := strings.CutPrefix(record["request_id"].(string), "req_")
		require.True(t, ok)
		_, err = ulid.ParseStrict(id)
		require.NoError(t, err)
		for _, field := range []string{"model", "scenario", "chunks_written", "outcome", "latency_ms"} {
			assert.Contains(t, record, field)
		}
		assert.GreaterOrEqual(t, record["latency_ms"].(float64), float64(0))
		return record
	case <-time.After(time.Second):
		t.Fatal("handler did not emit its completion log")
		return nil
	}
}

// §5⑬: use the actual JSONHandler, including its ts transformation. An invalid
// incoming ID is replaced, not echoed or logged, even if it carries private text.
func TestRequestLogIDsFieldsAndPrivacy(t *testing.T) {
	e := mock.NewEngine()
	p := mock.DefaultScenarioParams()
	p.Chunks, p.ChunkInterval = 1, 0
	require.NoError(t, e.Set(chatKey, mock.Normal, p))
	s, capture := loggedChatServer(t, e)
	valid := "req_" + ulid.Make().String()
	for _, incoming := range []string{
		"", "private-body-marker", ulid.Make().String(), "req_bad",
		"req_8ZZZZZZZZZZZZZZZZZZZZZZZZZ", "req_01ARZ3NDEKTSV4RRFFQ69G5FAI", valid,
	} {
		r := chatHTTPRequest(t, t.Context(), s, chatBody(true, true), chatKey, "")
		r.Header.Set("X-Request-Id", incoming)
		resp, err := s.Client().Do(r)
		require.NoError(t, err)
		_, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		record := nextLog(t, capture)
		assert.Equal(t, resp.Header.Get("X-Request-Id"), record["request_id"])
		if incoming == valid {
			assert.Equal(t, valid, record["request_id"])
		} else {
			assert.NotEqual(t, incoming, record["request_id"])
		}
		assert.Equal(t, chatModel, record["model"])
		assert.Equal(t, "normal", record["scenario"])
		assert.Equal(t, float64(1), record["chunks_written"], "usage and DONE are excluded")
		assert.Equal(t, "completed", record["outcome"])
	}
	assert.NotContains(t, capture.text(), chatKey)
	assert.NotContains(t, capture.text(), "Authorization")
	assert.NotContains(t, capture.text(), "private-body-marker")
}

func TestRequestLogFaultOutcomesAndNoRecovery(t *testing.T) {
	for _, tc := range []struct {
		scenario mock.Scenario
		stream   bool
		outcome  string
		chunks   int
		status   int
	}{
		{mock.Interrupted, true, "interrupted", 3, 200},
		{mock.Interrupted, false, "interrupted", 0, 0},
		{mock.Downtime, false, "interrupted", 0, 0},
		{mock.RateLimited, true, "error", 0, 429},
	} {
		t.Run(fmt.Sprintf("%s/%t", tc.scenario, tc.stream), func(t *testing.T) {
			e := mock.NewEngine()
			p := mock.DefaultScenarioParams()
			p.ChunkInterval, p.DowntimeMode = 0, "refuse"
			require.NoError(t, e.Set(chatKey, tc.scenario, p))
			s, capture := loggedChatServer(t, e)
			r := chatHTTPRequest(t, t.Context(), s, chatBody(tc.stream, true), chatKey, "")
			resp, err := s.Client().Do(r)
			if tc.status == 0 {
				require.Error(t, err)
				require.Nil(t, resp)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.status, resp.StatusCode)
				_, err = io.ReadAll(resp.Body)
				resp.Body.Close()
				if tc.scenario == mock.Interrupted {
					require.Error(t, err, "logging must not recover ErrAbortHandler")
				} else {
					require.NoError(t, err)
				}
			}
			record := nextLog(t, capture)
			assert.Equal(t, tc.outcome, record["outcome"])
			assert.Equal(t, string(tc.scenario), record["scenario"])
			assert.Equal(t, float64(tc.chunks), record["chunks_written"])
			assert.NotContains(t, capture.text(), chatKey)
			assert.NotContains(t, capture.text(), "private-body-marker")
			assert.NotContains(t, capture.text(), "stack")
		})
	}
}

func TestRequestLogCanceledHandler(t *testing.T) {
	e := mock.NewEngine()
	p := mock.DefaultScenarioParams()
	p.TTFBDelay = 5 * time.Second
	require.NoError(t, e.Set(chatKey, mock.Slow, p))
	capture := &logCapture{records: make(chan map[string]any, 1)}
	r := gin.New()
	r.Use(mock.RequestLog(mock.NewLogger(capture)))
	mock.RegisterChatRoutes(r, e)
	entered := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		ctx := &waitObservedContext{Context: request.Context(), entered: entered}
		r.ServeHTTP(w, request.WithContext(ctx))
	}))
	defer s.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := chatHTTPRequest(t, ctx, s, chatBody(true, true), chatKey, "")
	result := make(chan error, 1)
	go func() {
		resp, err := s.Client().Do(request)
		if resp != nil {
			resp.Body.Close()
		}
		result <- err
	}()
	awaitSignal(t, entered, "slow handler wait")
	started := time.Now()
	cancel()
	record := nextLog(t, capture)
	assert.Less(t, time.Since(started), 800*time.Millisecond)
	assert.Equal(t, "canceled", record["outcome"])
	assert.Equal(t, "slow", record["scenario"])
	assert.Equal(t, float64(0), record["chunks_written"])
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestRequestLogConcurrentRequestsStayIsolated(t *testing.T) {
	s, capture := loggedChatServer(t, mock.NewEngine())
	var group errgroup.Group
	for i := range 16 {
		group.Go(func() error {
			body := fmt.Sprintf(`{"model":"model-%d","messages":[{}]}`, i)
			r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.URL+chatPath, strings.NewReader(body))
			if err != nil {
				return err
			}
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+chatKey)
			resp, err := s.Client().Do(r)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			_, err = io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != 200 {
				return fmt.Errorf("response status=%d, read error=%v", resp.StatusCode, err)
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
	ids, models := map[string]bool{}, map[string]bool{}
	for range 16 {
		record := nextLog(t, capture)
		ids[record["request_id"].(string)] = true
		models[record["model"].(string)] = true
		assert.Equal(t, "completed", record["outcome"])
		assert.Equal(t, float64(0), record["chunks_written"])
	}
	assert.Len(t, ids, 16)
	for i := range 16 {
		assert.True(t, models[fmt.Sprintf("model-%d", i)])
	}
}
