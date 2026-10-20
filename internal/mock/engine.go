// Package mock implements the independent mock HTTP upstream (12.1).
// It does not depend on the gateway or any other internal package.
package mock

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode"
)

// Scenario identifies the response behavior for one test key.
type Scenario string

const (
	Normal      Scenario = "normal"
	Slow        Scenario = "slow"
	Downtime    Scenario = "downtime"
	RateLimited Scenario = "rate_limited"
	Interrupted Scenario = "interrupted"
)

// ScenarioParams is a complete configuration. Zero values are never defaults.
type ScenarioParams struct {
	Chunks           int
	ChunkInterval    time.Duration
	TTFBDelay        time.Duration
	DowntimeMode     string
	FailAfterChunks  int
	PromptTokens     int
	CompletionTokens int
}

// ScenarioConfig is an immutable-by-value snapshot of a key's configuration.
type ScenarioConfig struct {
	Scenario Scenario
	Params   ScenarioParams
}

// Engine stores complete configurations, isolated by the full test key.
// All methods are safe for concurrent use. Returned values belong to the caller.
type Engine interface {
	Set(key string, s Scenario, p ScenarioParams) error
	Get(key string) (Scenario, ScenarioParams)
	Resolve(key, overrideHeader string) (Scenario, ScenarioParams, error)
	Snapshot() map[string]ScenarioConfig
}

type engine struct {
	mu      sync.RWMutex
	configs map[string]ScenarioConfig
}

// NewEngine starts with no explicitly configured keys.
func NewEngine() Engine {
	return &engine{configs: make(map[string]ScenarioConfig)}
}

// DefaultScenarioParams returns a fresh complete parameter value (12.1 §3.2).
func DefaultScenarioParams() ScenarioParams {
	return ScenarioParams{
		Chunks:           8,
		ChunkInterval:    50 * time.Millisecond,
		TTFBDelay:        2 * time.Second,
		DowntimeMode:     "503",
		FailAfterChunks:  3,
		PromptTokens:     12,
		CompletionTokens: 34,
	}
}

// Set validates before atomically replacing the whole configuration. A rejected
// update leaves both existing keys and the set of explicit keys unchanged.
func (e *engine) Set(key string, s Scenario, p ScenarioParams) error {
	if err := validateKey(key); err != nil {
		return fmt.Errorf("mock: set key: %w", err)
	}
	if err := validateScenario(s, p); err != nil {
		return fmt.Errorf("mock: set configuration: %w", err)
	}
	e.mu.Lock()
	e.configs[key] = ScenarioConfig{Scenario: s, Params: p}
	e.mu.Unlock()
	return nil
}

// Get reads one complete snapshot. Reading an unknown key never inserts it.
func (e *engine) Get(key string) (Scenario, ScenarioParams) {
	e.mu.RLock()
	c, ok := e.configs[key]
	e.mu.RUnlock()
	if !ok {
		return Normal, DefaultScenarioParams()
	}
	return c.Scenario, c.Params
}

// Resolve reads once, then applies only the scenario-name override. No state lock
// survives the read; callers keep this value throughout waits and network writes.
func (e *engine) Resolve(key, overrideHeader string) (Scenario, ScenarioParams, error) {
	if err := validateKey(key); err != nil {
		return "", ScenarioParams{}, fmt.Errorf("mock: resolve key: %w", err)
	}
	s, p := e.Get(key)
	if overrideHeader != "" {
		s = Scenario(overrideHeader)
	}
	if err := validateScenario(s, p); err != nil {
		return "", ScenarioParams{}, fmt.Errorf("mock: resolve configuration: %w", err)
	}
	return s, p, nil
}

// Snapshot copies only explicit keys under one read lock for a consistent view.
func (e *engine) Snapshot() map[string]ScenarioConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	copy := make(map[string]ScenarioConfig, len(e.configs))
	for key, c := range e.configs {
		copy[key] = c
	}
	return copy
}

// validateKey is shared with the HTTP Bearer parser. Reject rather than normalize;
// errors deliberately contain neither key bytes nor other request data.
func validateKey(key string) error {
	if key == "" {
		return errors.New("test key must be nonempty")
	}
	for _, r := range key {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return errors.New("test key must not contain whitespace or control characters")
		}
	}
	return nil
}

func validateScenario(s Scenario, p ScenarioParams) error {
	switch s {
	case Normal, Slow, Downtime, RateLimited, Interrupted:
	default:
		return errors.New("unknown scenario")
	}
	switch {
	case p.Chunks <= 0:
		return errors.New("chunks must be positive")
	case p.ChunkInterval < 0:
		return errors.New("chunk_interval must be nonnegative")
	case p.TTFBDelay < 0:
		return errors.New("ttfb_delay must be nonnegative")
	case p.DowntimeMode != "503" && p.DowntimeMode != "refuse":
		return errors.New("downtime_mode must be 503 or refuse")
	case p.FailAfterChunks < 0:
		return errors.New("fail_after_chunks must be nonnegative")
	case s == Interrupted && (p.FailAfterChunks < 1 || p.FailAfterChunks >= p.Chunks):
		return errors.New("interrupted requires 1 <= fail_after_chunks < chunks")
	case p.PromptTokens < 0:
		return errors.New("prompt_tokens must be nonnegative")
	case p.CompletionTokens < 0:
		return errors.New("completion_tokens must be nonnegative")
	}
	const maxInt = int(^uint(0) >> 1)
	if p.PromptTokens > maxInt-p.CompletionTokens {
		return errors.New("total_tokens exceeds Go int range")
	}
	return nil
}
