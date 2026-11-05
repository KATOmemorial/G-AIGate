package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBreakerConfigurationDefaultsYAMLAndEnvironment(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	cfg, err := Load(missingPath(t))
	require.NoError(t, err)
	require.Equal(t, 500*time.Millisecond, cfg.Router.StateTimeout)
	require.Equal(t, time.Minute, cfg.Router.BreakerWindow)
	require.Equal(t, 20, cfg.Router.BreakerMinSamples)
	require.Equal(t, 0.5, cfg.Router.BreakerErrThreshold)
	require.Equal(t, 30*time.Second, cfg.Router.BreakerOpenDuration)
	cfg, err = Load("../../config.example.yaml")
	require.NoError(t, err)
	require.Equal(t, 500*time.Millisecond, cfg.Router.StateTimeout)
	path := writeConfig(t, "router:\n  state-timeout: 1ms\n  breaker-window: 1h\n  breaker-open-duration: 1ms\n  breaker-min-samples: 1000000\n  breaker-err-threshold: 0.123456\n")
	cfg, err = Load(path)
	require.NoError(t, err)
	require.Equal(t, time.Millisecond, cfg.Router.StateTimeout)
	require.Equal(t, time.Hour, cfg.Router.BreakerWindow)
	require.Equal(t, time.Millisecond, cfg.Router.BreakerOpenDuration)
	require.Equal(t, 1000000, cfg.Router.BreakerMinSamples)
	require.Equal(t, 0.123456, cfg.Router.BreakerErrThreshold)
	t.Setenv("GAIGATE_ROUTER_STATE_TIMEOUT", "1m")
	t.Setenv("GAIGATE_ROUTER_BREAKER_WINDOW", "1ms")
	t.Setenv("GAIGATE_ROUTER_BREAKER_OPEN_DURATION", "1h")
	t.Setenv("GAIGATE_ROUTER_BREAKER_MIN_SAMPLES", "1")
	t.Setenv("GAIGATE_ROUTER_BREAKER_ERR_THRESHOLD", "0.000001")
	cfg, err = Load(path)
	require.NoError(t, err)
	require.Equal(t, time.Minute, cfg.Router.StateTimeout)
	require.Equal(t, time.Millisecond, cfg.Router.BreakerWindow)
	require.Equal(t, time.Hour, cfg.Router.BreakerOpenDuration)
	require.Equal(t, 1, cfg.Router.BreakerMinSamples)
	require.Equal(t, 0.000001, cfg.Router.BreakerErrThreshold)
}

func TestBreakerConfigurationRejectsYAMLAndEnvBoundaries(t *testing.T) {
	for _, field := range []string{"state-timeout", "breaker-window", "breaker-open-duration", "breaker-min-samples", "breaker-err-threshold"} {
		values := []string{"0", "-1ms", "1ns", "1500us", "9223372036854775807ns"}
		switch field {
		case "state-timeout":
			values = append(values, "60001ms")
		case "breaker-window", "breaker-open-duration":
			values = append(values, "3600001ms")
		case "breaker-min-samples":
			values = []string{"0", "-1", "1000001", "9223372036854775807"}
		case "breaker-err-threshold":
			values = []string{"0", "-1", "0.0000009", "0.1234567", "0.5000001", "1.000001", ".nan", ".inf", "-.inf"}
		}
		for _, value := range values {
			for _, source := range []string{"yaml", "env"} {
				t.Run(field+"/"+value+"/"+source, func(t *testing.T) {
					t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
					path := missingPath(t)
					if source == "yaml" {
						path = writeConfig(t, fmt.Sprintf("router:\n  %s: %s\n", field, value))
					} else {
						envValue := value
						if value == ".nan" {
							envValue = "NaN"
						}
						if value == ".inf" {
							envValue = "Inf"
						}
						if value == "-.inf" {
							envValue = "-Inf"
						}
						t.Setenv("GAIGATE_ROUTER_"+strings.ToUpper(strings.ReplaceAll(field, "-", "_")), envValue)
					}
					_, err := Load(path)
					require.Error(t, err)
				})
			}
		}
	}
}

func TestBreakerConfigurationPPMNormalization(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	for _, v := range []float64{0.000001, 0.1, 0.123456, 0.5, 0.999999, 1, math.Nextafter(0.123456, 1), math.Nextafter(0.123456, 0)} {
		t.Run(strconv.FormatFloat(v, 'g', -1, 64), func(t *testing.T) {
			t.Setenv("GAIGATE_ROUTER_BREAKER_ERR_THRESHOLD", strconv.FormatFloat(v, 'g', -1, 64))
			_, err := Load(missingPath(t))
			require.NoError(t, err)
		})
	}
	for _, v := range []float64{math.Nextafter(0.000001, 0), math.Nextafter(1, 2), 0.1234567, math.NaN(), math.Inf(1), math.Inf(-1)} {
		require.False(t, breakerConfigThreshold(v))
	}
	v := 0.123456
	for range 3 {
		v = math.Nextafter(v, 1)
	}
	require.False(t, breakerConfigThreshold(v))
}

func TestBreakerConfigurationRejectsWeakScalarCoercions(t *testing.T) {
	for _, entry := range []struct{ field, value string }{
		{"breaker-min-samples", "1.5"}, {"breaker-min-samples", "true"}, {"breaker-err-threshold", "true"},
		{"state-timeout", "1000000.5"}, {"breaker-window", "1000000.5"}, {"breaker-open-duration", "1000000.5"},
	} {
		for _, source := range []string{"yaml", "env"} {
			t.Run(entry.field+"/"+entry.value+"/"+source, func(t *testing.T) {
				t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
				path := missingPath(t)
				if source == "yaml" {
					path = writeConfig(t, fmt.Sprintf("router:\n  %s: %s\n", entry.field, entry.value))
				} else {
					t.Setenv("GAIGATE_ROUTER_"+strings.ToUpper(strings.ReplaceAll(entry.field, "-", "_")), entry.value)
				}
				cfg, err := Load(path)
				require.Error(t, err)
				require.Nil(t, cfg)
			})
		}
	}
	// Integral numeric YAML values keep their exact existing nanosecond units.
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	cfg, err := Load(writeConfig(t, "router:\n  state-timeout: 1000000.0\n  breaker-min-samples: 1.0\n"))
	require.NoError(t, err)
	require.Equal(t, time.Millisecond, cfg.Router.StateTimeout)
	require.Equal(t, 1, cfg.Router.BreakerMinSamples)
}
