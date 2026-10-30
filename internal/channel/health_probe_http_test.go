package channel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/provider/openai"
	"gaigate/internal/secret"
	"gaigate/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestHealthBackgroundProbeRealHTTPUsesExactPreparedFingerprint(t *testing.T) {
	for _, mode := range []string{"normal", "configuration_changed", "health_changed", "own_timeout", "parent_cancel", "bad_ciphertext"} {
		t.Run(mode, func(t *testing.T) {
			mr := miniredis.RunT(t)
			client := store.NewRedis(store.RedisOptions{Addr: mr.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			state := store.NewVersionedHealthState(client)
			master := []byte(strings.Repeat("h", 32))
			enc, err := secret.Encrypt([]byte("owned-health-path-a"), master)
			require.NoError(t, err)
			if mode == "bad_ciphertext" {
				enc = []byte("corrupt-private-health-marker")
			}
			r, k := serviceRepo()
			entered, release, ended := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var actualCalls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(ended)
				actualCalls.Add(1)
				require.Equal(t, "Bearer owned-health-path-a", req.Header.Get("Authorization"))
				require.Empty(t, req.Header.Get("X-Mock-Scenario"))
				var fields map[string]json.RawMessage
				require.NoError(t, json.NewDecoder(req.Body).Decode(&fields))
				require.JSONEq(t, `"up-a"`, string(fields["model"]))
				require.JSONEq(t, `false`, string(fields["stream"]))
				require.JSONEq(t, `1`, string(fields["max_tokens"]))
				require.JSONEq(t, `[{"role":"user","content":"ping"}]`, string(fields["messages"]))
				close(entered)
				if mode == "own_timeout" || mode == "parent_cancel" {
					<-req.Context().Done()
					return
				}
				<-release
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"probe","object":"chat.completion","created":1,"model":"up-a","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}`)
			}))
			t.Cleanup(srv.Close)
			r.get = func(context.Context, int64) (*store.Channel, error) {
				return &store.Channel{ID: 1, Enabled: true, Provider: "openai", BaseURL: srv.URL}, nil
			}
			r.models = func(context.Context, int64) ([]store.ChannelModel, error) {
				return []store.ChannelModel{{ChannelID: 1, Model: "z", UpstreamModel: "up-z"}, {ChannelID: 1, Model: "a", UpstreamModel: "up-a"}}, nil
			}
			var keyReads atomic.Int32
			var changed atomic.Bool
			k.list = func(context.Context, int64) ([]store.ChannelKey, error) {
				keyReads.Add(1)
				selected := enc
				if changed.Load() {
					selected = []byte("new-private-ciphertext")
				}
				return []store.ChannelKey{{ID: 9, ChannelID: 1, Status: 1, ProviderKeyEnc: []byte("unselected-bad")},
					{ID: 2, ChannelID: 1, Status: 1, ProviderKeyEnc: selected}, {ID: 1, ChannelID: 1, Status: 0}}, nil
			}
			picker, err := NewProbePicker(r, k, master)
			require.NoError(t, err)
			upstream, err := openai.New(srv.Client(), 1)
			require.NoError(t, err)
			tester, err := NewConnectivityTester(picker, upstream, time.Second)
			require.NoError(t, err)
			// Change configuration AFTER capture but BEFORE Test calls Pick. The
			// provider must still use the original fingerprint's encrypted Key.
			wrapped := healthTesterFunc(func(ctx context.Context, ch *store.Channel, model string) (ProbeResult, error) {
				if mode == "configuration_changed" {
					changed.Store(true)
				}
				return tester.Test(ctx, ch, model)
			})
			opts := serviceOptions()
			if mode == "own_timeout" {
				opts.ProbeTimeout = 100 * time.Millisecond
			}
			h := newServiceTest(t, r, k, state, wrapped, opts, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan struct{})
			go func() { defer close(finished); h.probeChannel(ctx, 1) }()
			if mode != "bad_ciphertext" {
				select {
				case <-entered:
				case <-time.After(2 * time.Second):
					t.Fatal("provider not called")
				}
				if mode == "normal" || mode == "configuration_changed" || mode == "health_changed" {
					require.Equal(t, int32(1), keyReads.Load(), "picker must not re-read a different Key configuration")
				}
				if mode == "health_changed" {
					h.ReportRequestResult(context.Background(), 1, false)
				}
				if mode == "parent_cancel" {
					cancel()
				}
				close(release)
			}
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Fatal("probe did not finish")
			}
			if mode != "bad_ciphertext" {
				<-ended
			}
			record, err := state.LoadVersioned(context.Background(), 1)
			require.NoError(t, err)
			switch mode {
			case "normal":
				require.Equal(t, uint64(1), record.Version)
				require.True(t, record.Snapshot.LastProbeOk)
				require.Positive(t, record.Snapshot.LastProbeAt)
				require.Len(t, h.views, 1)
			case "own_timeout":
				require.Equal(t, uint64(1), record.Version)
				require.False(t, record.Snapshot.LastProbeOk)
				require.Equal(t, int32(1), record.Snapshot.ConsecutiveFailures)
			case "health_changed":
				require.Equal(t, uint64(1), record.Version)
				require.Equal(t, int32(1), record.Snapshot.ConsecutiveFailures)
				require.Zero(t, record.Snapshot.LastProbeAt)
			default:
				require.Equal(t, store.HealthRecord{}, record)
				require.Empty(t, h.views)
				require.Empty(t, mr.Keys())
			}
			if mode == "bad_ciphertext" {
				require.Zero(t, actualCalls.Load())
			} else {
				require.Equal(t, int32(1), actualCalls.Load())
			}
		})
	}
}
