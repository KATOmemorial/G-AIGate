package config

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

const meterEnv = "GAIGATE_METER_ESTIMATE_CHARS_PER_TOKEN"

func TestMeterConfigurationSources(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	t.Setenv(meterEnv, "")
	for _, path := range []string{missingPath(t), "../../config.example.yaml"} {
		cfg, err := Load(path)
		require.NoError(t, err)
		require.Equal(t, 4.0, cfg.Meter.EstimateCharsPerToken)
	}
	for _, value := range []string{"0.001", "1024", "0.123456", "4", "4.0", "1e-3", "\"0.125\""} {
		t.Run(value, func(t *testing.T) {
			path := writeConfig(t, "meter:\n  estimate-chars-per-token: "+value+"\n")
			cfg, err := Load(path)
			require.NoError(t, err)
			want := map[string]float64{"0.001": 0.001, "1024": 1024, "0.123456": 0.123456, "4": 4, "4.0": 4, "1e-3": 0.001, "\"0.125\"": 0.125}[value]
			require.Equal(t, want, cfg.Meter.EstimateCharsPerToken)
			t.Setenv(meterEnv, "0.654321")
			cfg, err = Load(path)
			require.NoError(t, err)
			require.Equal(t, 0.654321, cfg.Meter.EstimateCharsPerToken)
		})
	}
	// An effective valid env value overrides even an invalid YAML scalar.
	for _, value := range []string{"true", "null", "[]", "1.0000001"} {
		t.Setenv(meterEnv, "0.125")
		cfg, err := Load(writeConfig(t, "meter:\n  estimate-chars-per-token: "+value+"\n"))
		require.NoError(t, err)
		require.Equal(t, 0.125, cfg.Meter.EstimateCharsPerToken)
	}
}

func TestMeterConfigurationRejectsWeakConversions(t *testing.T) {
	for _, tc := range []struct{ yaml, env string }{
		{"0", "0"}, {"-1", "-1"}, {"0.000999", "0.000999"}, {"1024.000001", "1024.000001"},
		{"0.1234567", "0.1234567"}, {"1.0000001", "1.0000001"}, {".nan", "NaN"}, {".inf", "Inf"}, {"-.inf", "-Inf"},
		{"true", "true"}, {"false", "false"}, {"[4]", "[4]"}, {"{value: 4}", "{value: 4}"}, {"null", "null"},
		{"\"\"", "garbage"}, {"\"1.00000000000000001\"", "1.00000000000000001"},
	} {
		for _, source := range []string{"yaml", "env"} {
			t.Run(tc.yaml+"/"+source, func(t *testing.T) {
				t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
				t.Setenv(meterEnv, "")
				path := writeConfig(t, "meter:\n  estimate-chars-per-token: "+tc.yaml+"\n")
				if source == "env" {
					path = writeConfig(t, "meter:\n  estimate-chars-per-token: 4\n")
					t.Setenv(meterEnv, tc.env)
				}
				cfg, err := Load(path)
				require.Error(t, err)
				require.Nil(t, cfg)
				require.Contains(t, err.Error(), "meter.estimate_chars_per_token")
			})
		}
	}
}

func TestMeterConfigurationExactFloatBoundary(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Nextafter(0.1, 1), math.Nextafter(0.123456, 0)} {
		require.False(t, meterConfigRatio(v))
	}
	for _, v := range []float64{0.001, 0.1, 0.123456, 1024} {
		require.True(t, meterConfigRatio(v))
	}
	for _, raw := range []any{true, []int{4}, nil, math.NaN(), "1.00000000000000001"} {
		_, err := meterScalar(raw)
		require.Error(t, err, fmt.Sprint(raw))
	}
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	cfg, err := Load(missingPath(t))
	require.NoError(t, err)
	cfg.Meter.EstimateCharsPerToken = math.Nextafter(0.1, 1)
	require.ErrorContains(t, cfg.validate(), "meter.estimate_chars_per_token")
}
