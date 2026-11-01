package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRouterRouteTimeoutDefaultsOverridesAndLimits(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	for _, path := range []string{missingPath(t), "../../config.example.yaml"} {
		cfg, err := Load(path)
		require.NoError(t, err)
		require.Equal(t, 2*time.Second, cfg.Router.RouteTimeout)
	}
	path := writeConfig(t, "router:\n  route-timeout: 39ms\n")
	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, 39*time.Millisecond, cfg.Router.RouteTimeout)
	for _, value := range []string{"1ms", "1m", "2300ms"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("GAIGATE_ROUTER_ROUTE_TIMEOUT", value)
			cfg, err := Load(path)
			require.NoError(t, err)
			want, err := time.ParseDuration(value)
			require.NoError(t, err)
			require.Equal(t, want, cfg.Router.RouteTimeout)
		})
	}
}

func TestRouterRouteTimeoutRejectsInvalidYAMLAndEnvironment(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	for _, value := range []string{"0s", "-1ms", "999us", "1001us", "1m1ms", "1ns", "9223372036854775807ns", "invalid"} {
		for _, source := range []string{"yaml", "env"} {
			t.Run(source+"/"+value, func(t *testing.T) {
				path := missingPath(t)
				if source == "yaml" {
					path = writeConfig(t, "router:\n  route-timeout: "+value+"\n")
				} else {
					t.Setenv("GAIGATE_ROUTER_ROUTE_TIMEOUT", value)
				}
				cfg, err := Load(path)
				require.Nil(t, cfg)
				require.Error(t, err)
				if value != "invalid" {
					require.Contains(t, err.Error(), "router.route_timeout")
				}
			})
		}
	}
}
