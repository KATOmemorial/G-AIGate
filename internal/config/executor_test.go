package config

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

func TestExecutorMaxAttemptsDefaultsYAMLAndEnvironment(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	cfg, err := Load(missingPath(t))
	require.NoError(t, err)
	require.Equal(t, 3, cfg.Router.MaxAttempts)
	cfg, err = Load("../../config.example.yaml")
	require.NoError(t, err)
	require.Equal(t, 3, cfg.Router.MaxAttempts)
	path := writeConfig(t, "router:\n  max-attempts: 16\n")
	cfg, err = Load(path)
	require.NoError(t, err)
	require.Equal(t, 16, cfg.Router.MaxAttempts)
	t.Setenv("GAIGATE_ROUTER_MAX_ATTEMPTS", "1")
	cfg, err = Load(path)
	require.NoError(t, err)
	require.Equal(t, 1, cfg.Router.MaxAttempts)
}
func TestExecutorMaxAttemptsRejectsInvalidScalars(t *testing.T) {
	for _, value := range []string{"0", "-1", "17", "1.5", "true", "bad", "9223372036854775807"} {
		for _, source := range []string{"yaml", "env"} {
			t.Run(source+"/"+value, func(t *testing.T) {
				t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
				path := missingPath(t)
				if source == "yaml" {
					path = writeConfig(t, "router:\n  max-attempts: "+value+"\n")
				} else {
					t.Setenv("GAIGATE_ROUTER_MAX_ATTEMPTS", value)
				}
				_, err := Load(path)
				require.Error(t, err)
			})
		}
	}
}
func TestExecutorLeaseCapacityUsesMinimumStateAndExactDuration(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	cfg, err := Load(missingPath(t))
	require.NoError(t, err)
	cfg.Router.StateTimeout = time.Second
	cfg.Channel.StateTimeout = 100 * time.Millisecond
	cfg.Proxy.OverallTimeout = 24*time.Hour - 600*time.Millisecond
	require.NoError(t, cfg.validate())
	cfg.Proxy.OverallTimeout += time.Nanosecond
	require.ErrorContains(t, cfg.validate(), "proxy.overall_timeout")
	cfg.Proxy.OverallTimeout = time.Duration(math.MaxInt64)
	require.ErrorContains(t, cfg.validate(), "proxy.overall_timeout")
	cfg.Proxy.OverallTimeout = 24*time.Hour - time.Second
	cfg.Channel.StateTimeout = time.Duration(math.MaxInt64)
	require.ErrorContains(t, cfg.validate(), "proxy.overall_timeout")
	cfg.Router.StateTimeout = 100 * time.Millisecond
	require.NoError(t, cfg.validate())
}
