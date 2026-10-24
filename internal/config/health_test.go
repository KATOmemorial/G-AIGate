package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelHealthConfigurationDefaultsYAMLAndEnvironment(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	for _, path := range []string{missingPath(t), "../../config.example.yaml"} {
		cfg, err := Load(path)
		require.NoError(t, err)
		require.Equal(t, 4, cfg.Channel.ProbeConcurrency)
		require.Equal(t, 500*time.Millisecond, cfg.Channel.StateTimeout)
		require.Equal(t, 30*time.Second, cfg.Channel.ProbeInterval)
		require.Equal(t, 60*time.Second, cfg.Channel.DisableDuration)
		require.Equal(t, 5, cfg.Channel.FailureThreshold)
		require.Equal(t, 5*time.Second, cfg.Channel.ProbeTimeout)
	}
	path := writeConfig(t, "channel:\n  probe-concurrency: 3\n  state-timeout: 27ms\n  probe-interval: 1ns\n  disable-duration: 1ns\n")
	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, 3, cfg.Channel.ProbeConcurrency)
	require.Equal(t, 27*time.Millisecond, cfg.Channel.StateTimeout)
	require.Equal(t, time.Nanosecond, cfg.Channel.ProbeInterval)
	require.Equal(t, time.Nanosecond, cfg.Channel.DisableDuration)
	t.Setenv("GAIGATE_CHANNEL_PROBE_CONCURRENCY", "64")
	t.Setenv("GAIGATE_CHANNEL_STATE_TIMEOUT", "1ms")
	t.Setenv("GAIGATE_CHANNEL_FAILURE_THRESHOLD", "2147483647")
	cfg, err = Load(path)
	require.NoError(t, err)
	require.Equal(t, 64, cfg.Channel.ProbeConcurrency)
	require.Equal(t, time.Millisecond, cfg.Channel.StateTimeout)
	require.Equal(t, 2147483647, cfg.Channel.FailureThreshold)
}

func TestChannelHealthConfigurationRejectsOutOfRangeValues(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	for _, tc := range []struct{ env, value, field string }{
		{"GAIGATE_CHANNEL_PROBE_CONCURRENCY", "0", "probe_concurrency"}, {"GAIGATE_CHANNEL_PROBE_CONCURRENCY", "65", "probe_concurrency"}, {"GAIGATE_CHANNEL_PROBE_CONCURRENCY", "-1", "probe_concurrency"},
		{"GAIGATE_CHANNEL_STATE_TIMEOUT", "999us", "state_timeout"}, {"GAIGATE_CHANNEL_STATE_TIMEOUT", "0s", "state_timeout"}, {"GAIGATE_CHANNEL_STATE_TIMEOUT", "-1s", "state_timeout"},
		{"GAIGATE_CHANNEL_FAILURE_THRESHOLD", "2147483648", "failure_threshold"},
	} {
		t.Run(tc.env+"/"+tc.value, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)
			_, err := Load(missingPath(t))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.field)
		})
	}
}
