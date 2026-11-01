package router

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"gaigate/internal/access"
	"gaigate/internal/provider"
	"gaigate/internal/store"
)

const (
	maxCandidates    = 64
	maxFallbackBytes = 64 * 1024
)

// Minimal read projections make side-effecting dependencies inaccessible here.
// The public constructor accepts the frozen full repository/health interfaces.
type modelReader interface {
	Resolve(context.Context, string) (*store.ModelAlias, error)
}

type channelReader interface {
	Get(context.Context, int64) (*store.Channel, error)
	ListActiveByModel(context.Context, string) ([]store.Channel, error)
	GetModel(context.Context, int64, string) (*store.ChannelModel, error)
}

type healthReader interface {
	IsAvailable(context.Context, int64) bool
}

// resolver builds the candidate set. Ordering and the total budget belong to Router.
type resolver struct {
	models          modelReader
	channels        channelReader
	health          healthReader
	breaker         BreakerInspector
	defaultStrategy RouteStrategy
}

// stageError keeps infrastructure/configuration text out of the safe error message
// while retaining the original error for errors.Is/As.
type stageError struct {
	stage string
	cause error
}

func (e *stageError) Error() string           { return "router: " + e.stage }
func (e *stageError) Unwrap() error           { return e.cause }
func failure(stage string, cause error) error { return &stageError{stage: stage, cause: cause} }

func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return failure("planning canceled", errors.Join(err, context.Cause(ctx)))
	}
	return nil
}

func validStrategy(strategy RouteStrategy) bool {
	switch strategy {
	case Weighted, CostFirst, LatencyFirst, FallbackChain:
		return true
	default:
		return false
	}
}

func (r *resolver) resolve(ctx context.Context, model string, requested RouteStrategy) (*Plan, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if model == "" || !utf8.ValidString(model) {
		return nil, failure("invalid model", nil)
	}
	if utf8.RuneCountInString(model) > 128 {
		return nil, failure("unknown model", access.ErrModelNotFound)
	}
	if requested != Auto && !validStrategy(requested) {
		return nil, failure("invalid requested strategy", nil)
	}
	alias, err := r.models.Resolve(ctx, model)
	if canceled := contextError(ctx); canceled != nil {
		return nil, canceled
	}
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, failure("unknown model", errors.Join(access.ErrModelNotFound, err))
		}
		return nil, failure("alias lookup failed", err)
	}
	if alias == nil || alias.Name != model {
		return nil, failure("invalid alias", nil)
	}
	// Copy before any subsequent dependency call can replace its configuration.
	snapshot := *alias
	if alias.DefaultChannelID != nil {
		id := *alias.DefaultChannelID
		snapshot.DefaultChannelID = &id
	}
	aliasStrategy := RouteStrategy(snapshot.Strategy)
	if aliasStrategy != Auto && !validStrategy(aliasStrategy) {
		return nil, failure("invalid alias strategy", nil)
	}
	strategy := requested
	if strategy == Auto {
		strategy = aliasStrategy
	}
	if strategy == Auto {
		strategy = r.defaultStrategy
	}
	if !validStrategy(strategy) {
		return nil, failure("invalid default strategy", nil)
	}

	rows, err := r.initial(ctx, model, strategy, &snapshot)
	if err != nil {
		return nil, err
	}
	candidates := make([]Candidate, 0, len(rows))
	for _, row := range rows {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		if !row.Enabled {
			continue
		}
		if !validChannel(row) {
			return nil, failure("invalid channel configuration", nil)
		}
		mapping, err := r.channels.GetModel(ctx, int64(row.ID), model)
		if canceled := contextError(ctx); canceled != nil {
			return nil, canceled
		}
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, failure("mapping lookup failed", err)
		}
		if mapping == nil || mapping.ChannelID != row.ID || mapping.Model != model || !validName(mapping.UpstreamModel) {
			return nil, failure("invalid mapping", nil)
		}
		upstreamModel := mapping.UpstreamModel
		available := r.health.IsAvailable(ctx, int64(row.ID))
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		if !available {
			continue
		}
		view, err := r.breaker.Inspect(ctx, int64(row.ID))
		if canceled := contextError(ctx); canceled != nil {
			return nil, canceled
		}
		if err != nil {
			return nil, failure("breaker inspection failed", err)
		}
		switch view.State {
		case BreakerClosed, BreakerOpen, BreakerHalfOpen:
		default:
			return nil, failure("invalid breaker view", nil)
		}
		if view.Eligible {
			candidates = append(candidates, Candidate{Channel: row, UpstreamModel: upstreamModel})
		}
	}
	if len(candidates) == 0 {
		return nil, failure("no eligible candidate", provider.ErrNoCandidate)
	}
	return &Plan{Model: model, Strategy: strategy, Candidates: candidates}, nil
}

func (r *resolver) initial(ctx context.Context, model string, strategy RouteStrategy, alias *store.ModelAlias) ([]store.Channel, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if strategy == FallbackChain {
		ids, err := fallbackIDs(alias)
		if err != nil {
			return nil, err
		}
		rows := make([]store.Channel, 0, len(ids))
		for _, id := range ids {
			if err := contextError(ctx); err != nil {
				return nil, err
			}
			row, err := r.channels.Get(ctx, int64(id))
			if canceled := contextError(ctx); canceled != nil {
				return nil, canceled
			}
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, failure("channel lookup failed", err)
			}
			if row == nil || row.ID != id {
				return nil, failure("invalid referenced channel", nil)
			}
			rows = append(rows, cloneChannel(*row))
		}
		return rows, nil
	}
	rows, err := r.channels.ListActiveByModel(ctx, model)
	if canceled := contextError(ctx); canceled != nil {
		return nil, canceled
	}
	if err != nil {
		return nil, failure("candidate lookup failed", err)
	}
	unique := make([]store.Channel, 0, min(len(rows), maxCandidates))
	seen := make(map[uint64]store.Channel)
	for _, row := range rows {
		if !validID(row.ID) {
			return nil, failure("invalid channel ID", nil)
		}
		if previous, ok := seen[row.ID]; ok {
			if !reflect.DeepEqual(previous, row) {
				return nil, failure("conflicting duplicate channel", nil)
			}
			continue
		}
		row = cloneChannel(row)
		seen[row.ID] = row
		unique = append(unique, row)
		if len(unique) > maxCandidates {
			return nil, failure("too many initial candidates", nil)
		}
	}
	sort.Slice(unique, func(i, j int) bool {
		return unique[i].Priority > unique[j].Priority || unique[i].Priority == unique[j].Priority && unique[i].ID < unique[j].ID
	})
	return unique, nil
}

func fallbackIDs(alias *store.ModelAlias) ([]uint64, error) {
	if len(alias.FallbackIDs) > maxFallbackBytes {
		return nil, failure("fallback configuration too large", nil)
	}
	ids := make([]uint64, 0)
	if alias.DefaultChannelID != nil {
		ids = append(ids, *alias.DefaultChannelID)
	}
	// Use exactly the helper's []uint64 decoder semantics. Preflight only avoids
	// its legacy warning that logs the entire malformed configuration value.
	var decoded []uint64
	if json.Unmarshal([]byte(alias.FallbackIDs), &decoded) == nil {
		ids = append(ids, store.ParseFallbackIDs(alias.FallbackIDs)...)
	}
	unique := make([]uint64, 0, min(len(ids), maxCandidates))
	seen := make(map[uint64]bool)
	for _, id := range ids {
		if !validID(id) {
			return nil, failure("invalid fallback channel ID", nil)
		}
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
			if len(unique) > maxCandidates {
				return nil, failure("too many initial candidates", nil)
			}
		}
	}
	return unique, nil
}

func cloneChannel(row store.Channel) store.Channel {
	if row.LastCheckedAt != nil {
		checked := *row.LastCheckedAt
		row.LastCheckedAt = &checked
	}
	return row
}

func validID(id uint64) bool { return id > 0 && id <= math.MaxInt64 }
func validName(value string) bool {
	return value != "" && utf8.ValidString(value) && utf8.RuneCountInString(value) <= 128
}

func validChannel(row store.Channel) bool {
	return validID(row.ID) && validName(row.Name) && row.Provider == "openai" && validBaseURL(row.BaseURL) && row.Weight >= 1 && row.Weight <= 10000
}

// Keep the frozen 14.1 scalar rules without importing its private validation.
func validBaseURL(value string) bool {
	if len(value) == 0 || len(value) > 255 {
		return false
	}
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(value, "#") || u.Opaque != "" || strings.HasSuffix(u.Host, ":") {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	return true
}
