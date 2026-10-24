package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const healthFieldVersion = "version"

// HealthRecord extends the existing snapshot without changing the B2 API.
type HealthRecord struct {
	Snapshot HealthSnapshot
	Version  uint64
}

// VersionedHealthState provides a single-attempt, full-snapshot Redis CAS.
// Business conflict retries belong to the caller, not to this primitive.
type VersionedHealthState interface {
	LoadVersioned(ctx context.Context, channelID int64) (HealthRecord, error)
	CompareAndSwap(ctx context.Context, channelID int64, expected HealthRecord, next HealthSnapshot) (bool, error)
}

type versionedHealthState struct{ client *redis.Client }

func NewVersionedHealthState(client *redis.Client) VersionedHealthState {
	return &versionedHealthState{client: client}
}

func (h *versionedHealthState) validate(ctx context.Context, channelID int64) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("store: health operation canceled: %w", err)
	}
	if channelID <= 0 || h.client == nil {
		return errors.New("store: invalid health channel ID or client")
	}
	return nil
}

func (h *versionedHealthState) LoadVersioned(ctx context.Context, channelID int64) (HealthRecord, error) {
	if err := h.validate(ctx, channelID); err != nil {
		return HealthRecord{}, err
	}
	fields, err := h.client.HGetAll(ctx, healthKey(channelID)).Result()
	if err != nil {
		return HealthRecord{}, fmt.Errorf("store: load versioned health: %w", err)
	}
	return parseHealthRecord(fields)
}

// parseHealthRecord accepts missing version only for an otherwise complete old
// snapshot. A partial hash must never silently become a healthy zero snapshot.
func parseHealthRecord(fields map[string]string) (HealthRecord, error) {
	var record HealthRecord
	if len(fields) == 0 {
		return record, nil
	}
	for _, item := range []struct {
		name string
		bits int
		out  *int64
	}{
		{healthFieldDisabledUntil, 63, &record.Snapshot.DisabledUntil},
		{healthFieldLastProbeAt, 63, &record.Snapshot.LastProbeAt},
	} {
		n, err := parseHealthUint(fields[item.name], item.bits)
		if err != nil {
			return HealthRecord{}, fmt.Errorf("store: invalid health %s: %w", item.name, err)
		}
		*item.out = int64(n)
	}
	count, err := parseHealthUint(fields[healthFieldConsecutiveFailures], 31)
	if err != nil {
		return HealthRecord{}, fmt.Errorf("store: invalid health failure count: %w", err)
	}
	record.Snapshot.ConsecutiveFailures = int32(count)
	switch fields[healthFieldLastProbeOk] {
	case "0":
	case "1":
		record.Snapshot.LastProbeOk = true
	default:
		return HealthRecord{}, errors.New("store: invalid health probe bool")
	}
	if raw, ok := fields[healthFieldVersion]; ok {
		record.Version, err = parseHealthUint(raw, 64)
		if err != nil {
			return HealthRecord{}, fmt.Errorf("store: invalid health version: %w", err)
		}
	}
	return record, nil
}

func parseHealthUint(raw string, bits int) (uint64, error) {
	if raw == "" {
		return 0, errors.New("missing unsigned decimal")
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, errors.New("expected unsigned decimal")
		}
	}
	n, err := strconv.ParseUint(raw, 10, bits)
	if err != nil {
		// Do not echo arbitrary Redis contents through an error/log boundary.
		return 0, errors.New("unsigned decimal out of range")
	}
	return n, nil
}

func (h *versionedHealthState) CompareAndSwap(ctx context.Context, channelID int64, expected HealthRecord, next HealthSnapshot) (bool, error) {
	if err := h.validate(ctx, channelID); err != nil {
		return false, err
	}
	if next.ConsecutiveFailures < 0 || next.DisabledUntil < 0 || next.LastProbeAt < 0 || expected.Version == math.MaxUint64 {
		return false, errors.New("store: invalid next health snapshot or exhausted version")
	}
	key := healthKey(channelID)
	committed := false
	err := h.client.Watch(ctx, func(tx *redis.Tx) error {
		fields, err := tx.HGetAll(ctx, key).Result()
		if err != nil {
			return err
		}
		current, err := parseHealthRecord(fields)
		if err != nil {
			return err
		}
		if current != expected {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key,
				healthFieldConsecutiveFailures, strconv.FormatInt(int64(next.ConsecutiveFailures), 10),
				healthFieldDisabledUntil, strconv.FormatInt(next.DisabledUntil, 10),
				healthFieldLastProbeAt, strconv.FormatInt(next.LastProbeAt, 10),
				healthFieldLastProbeOk, boolFlag(next.LastProbeOk),
				healthFieldVersion, strconv.FormatUint(expected.Version+1, 10))
			// Health is persistent, including when repairing an accidental TTL.
			pipe.Persist(ctx, key)
			return nil
		})
		committed = err == nil
		return err
	}, key)
	if errors.Is(err, redis.TxFailedErr) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: compare and swap health: %w", err)
	}
	return committed, nil
}
