package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelProbeTimeoutConfiguration(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	cfg, err := Load(missingPath(t))
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, cfg.Channel.ProbeTimeout)
	cfg, err = Load("../../config.example.yaml")
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, cfg.Channel.ProbeTimeout)
	path := writeConfig(t, "channel:\n  probe-timeout: 230ms\n")
	cfg, err = Load(path)
	require.NoError(t, err)
	assert.Equal(t, 230*time.Millisecond, cfg.Channel.ProbeTimeout)
	t.Setenv("GAIGATE_CHANNEL_PROBE_TIMEOUT", "1ms")
	cfg, err = Load(path)
	require.NoError(t, err)
	assert.Equal(t, time.Millisecond, cfg.Channel.ProbeTimeout)
	for _, value := range []string{"0s", "-1s", "999us", "bad"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("GAIGATE_CHANNEL_PROBE_TIMEOUT", value)
			_, err := Load(path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "probe")
		})
	}
}
