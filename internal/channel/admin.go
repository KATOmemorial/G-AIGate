// Package channel owns channel administration and endpoint/health contracts.
package channel

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sort"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/secret"
	"gaigate/internal/store"
)

type EndpointPicker interface {
	Pick(context.Context, *store.Channel, string) (*provider.Endpoint, error)
}

type HealthRegistry interface {
	IsAvailable(context.Context, int64) bool
	ReportRequestResult(context.Context, int64, bool)
}

type ConnectivityTester interface {
	Test(context.Context, *store.Channel, string) (ProbeResult, error)
}

type ProbeResult struct {
	OK         bool   `json:"ok"`
	LatencyMS  int64  `json:"latency_ms"`
	StatusCode int    `json:"status_code"`
	Reason     string `json:"reason"`
}

type ModelMappingInput struct {
	Model         string `json:"model"`
	UpstreamModel string `json:"upstream_model"`
}

// Inputs are complete business values. HTTP supplies creation defaults in M2.
type CreateChannelInput struct {
	Name     string
	Provider string
	BaseURL  string
	Priority int32
	Weight   int32
	Enabled  bool
	Models   []ModelMappingInput
	Keys     []string
}

type UpdateChannelInput struct {
	Name     string
	Provider string
	BaseURL  string
	Priority int32
	Weight   int32
	Enabled  bool
	Models   []ModelMappingInput
}

// Views deliberately cannot carry raw Keys, ciphertext, hashes or ORM fields.
type ChannelView struct {
	ID            int64      `json:"id"`
	Name          string     `json:"name"`
	Provider      string     `json:"provider"`
	BaseURL       string     `json:"base_url"`
	Priority      int32      `json:"priority"`
	Weight        int32      `json:"weight"`
	Enabled       bool       `json:"enabled"`
	HealthStatus  string     `json:"health_status"`
	LastCheckedAt *time.Time `json:"last_checked_at"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type ChannelDetail struct {
	ChannelView
	Models []ModelMappingInput `json:"models"`
	Keys   []KeyView           `json:"keys"`
}

type KeyView struct {
	ID        int64     `json:"id"`
	ChannelID int64     `json:"channel_id"`
	Weight    int32     `json:"weight"`
	Status    int8      `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type AdminService interface {
	List(context.Context) ([]ChannelView, error)
	Get(context.Context, int64) (*ChannelDetail, error)
	Create(context.Context, CreateChannelInput) (*ChannelDetail, error)
	Update(context.Context, int64, UpdateChannelInput) (*ChannelDetail, error)
	Delete(context.Context, int64) error
	AddKey(context.Context, int64, string) (*KeyView, error)
	UpdateKey(context.Context, int64, int64, int32, int8) (*KeyView, error)
	RemoveKey(context.Context, int64, int64) error
}

var errInvalidInput = errors.New("channel: invalid management input")

// Keep internal Is/As classification while making ordinary Error/log output safe.
type operationError struct {
	stage string
	cause error
}

func (e *operationError) Error() string { return "channel: " + e.stage }
func (e *operationError) Unwrap() error { return e.cause }
func failure(stage string, cause error) error {
	return &operationError{stage: stage, cause: cause}
}

type adminService struct {
	channels    store.ChannelRepo
	keys        store.KeyRepo
	models      store.ModelRepo
	invalidator store.Invalidator
	masterKey   []byte
	encrypt     func([]byte, []byte) ([]byte, error)
}

func NewAdminService(channels store.ChannelRepo, keys store.KeyRepo, models store.ModelRepo,
	invalidator store.Invalidator, masterKey []byte) (AdminService, error) {
	for _, dependency := range []any{channels, keys, models, invalidator} {
		if nilDependency(dependency) {
			return nil, failure("missing administration dependency", nil)
		}
	}
	if len(masterKey) != 32 {
		return nil, failure("invalid master key length", nil)
	}
	return &adminService{channels: channels, keys: keys, models: models, invalidator: invalidator,
		masterKey: append([]byte(nil), masterKey...), encrypt: secret.Encrypt}, nil
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func validID(id uint64) bool { return id > 0 && id <= math.MaxInt64 }

func channelView(ch *store.Channel) (ChannelView, error) {
	if ch == nil || !validID(ch.ID) {
		return ChannelView{}, failure("invalid channel identity", nil)
	}
	v := ChannelView{ID: int64(ch.ID), Name: ch.Name, Provider: ch.Provider, BaseURL: ch.BaseURL,
		Priority: ch.Priority, Weight: ch.Weight, Enabled: ch.Enabled, HealthStatus: ch.HealthStatus,
		CreatedAt: ch.CreatedAt.UTC(), UpdatedAt: ch.UpdatedAt.UTC()}
	if ch.LastCheckedAt != nil {
		checked := ch.LastCheckedAt.UTC()
		v.LastCheckedAt = &checked
	}
	return v, nil
}

func keyView(k *store.ChannelKey, owner int64) (*KeyView, error) {
	if k == nil || !validID(k.ID) || owner <= 0 || k.ChannelID != uint64(owner) {
		return nil, failure("invalid owned Key identity", nil)
	}
	return &KeyView{ID: int64(k.ID), ChannelID: owner, Weight: k.Weight, Status: k.Status,
		CreatedAt: k.CreatedAt.UTC()}, nil
}

func (s *adminService) List(ctx context.Context) ([]ChannelView, error) {
	if err := ctx.Err(); err != nil {
		return nil, failure("list canceled", err)
	}
	rows, err := s.channels.List(ctx)
	if err != nil {
		return nil, failure("list channels failed", err)
	}
	views := make([]ChannelView, 0, len(rows))
	for i := range rows {
		v, err := channelView(&rows[i])
		if err != nil {
			return nil, err
		}
		views = append(views, v)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
	return views, nil
}

func (s *adminService) Get(ctx context.Context, id int64) (*ChannelDetail, error) {
	if id <= 0 {
		return nil, errInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return nil, failure("read canceled", err)
	}
	ch, err := s.channels.Get(ctx, id)
	if err != nil {
		return nil, failure("read channel failed", err)
	}
	v, err := channelView(ch)
	if err != nil {
		return nil, err
	}
	if v.ID != id {
		return nil, failure("channel identity mismatch", nil)
	}
	models, err := s.channels.ListModels(ctx, id)
	if err != nil {
		return nil, failure("read channel models failed", err)
	}
	keys, err := s.keys.ListByChannel(ctx, id)
	if err != nil {
		return nil, failure("read channel Keys failed", err)
	}
	detail := &ChannelDetail{ChannelView: v, Models: make([]ModelMappingInput, 0, len(models)),
		Keys: make([]KeyView, 0, len(keys))}
	for _, model := range models {
		if model.ChannelID != uint64(id) {
			return nil, failure("mapping owner mismatch", nil)
		}
		detail.Models = append(detail.Models, ModelMappingInput{Model: model.Model, UpstreamModel: model.UpstreamModel})
	}
	for i := range keys {
		v, err := keyView(&keys[i], id)
		if err != nil {
			return nil, err
		}
		detail.Keys = append(detail.Keys, *v)
	}
	sort.Slice(detail.Models, func(i, j int) bool { return detail.Models[i].Model < detail.Models[j].Model })
	sort.Slice(detail.Keys, func(i, j int) bool { return detail.Keys[i].ID < detail.Keys[j].ID })
	return detail, nil
}

func (s *adminService) Create(ctx context.Context, in CreateChannelInput) (*ChannelDetail, error) {
	config := UpdateChannelInput{Name: in.Name, Provider: in.Provider, BaseURL: in.BaseURL,
		Priority: in.Priority, Weight: in.Weight, Enabled: in.Enabled, Models: in.Models}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if len(in.Keys) < 1 || len(in.Keys) > 64 {
		return nil, errInvalidInput
	}
	for _, raw := range in.Keys {
		if !validRawKey(raw) {
			return nil, errInvalidInput
		}
	}
	if err := s.checkModels(ctx, in.Models); err != nil {
		return nil, err
	}
	enc := make([][]byte, 0, len(in.Keys))
	for _, raw := range in.Keys {
		ciphertext, err := s.encryptKey(ctx, raw)
		if err != nil {
			return nil, err
		}
		enc = append(enc, ciphertext)
	}
	ch, mappings := configRows(0, config)
	if err := s.channels.Create(ctx, ch, mappings, enc); err != nil {
		return nil, failure("create channel transaction failed", err)
	}
	// The commit has succeeded. Invalidate before any fallible response read.
	s.invalidator.InvalidateAll()
	if !validID(ch.ID) {
		return nil, failure("invalid created channel identity", nil)
	}
	return s.Get(ctx, int64(ch.ID))
}

func (s *adminService) Update(ctx context.Context, id int64, in UpdateChannelInput) (*ChannelDetail, error) {
	if id <= 0 {
		return nil, errInvalidInput
	}
	if err := validateConfig(in); err != nil {
		return nil, err
	}
	if err := s.checkModels(ctx, in.Models); err != nil {
		return nil, err
	}
	ch, models := configRows(uint64(id), in)
	if err := s.channels.UpdateConfig(ctx, ch, models); err != nil {
		return nil, failure("update channel transaction failed", err)
	}
	s.invalidator.InvalidateAll()
	return s.Get(ctx, id)
}

func (s *adminService) Delete(ctx context.Context, id int64) error {
	if id <= 0 {
		return errInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return failure("delete canceled", err)
	}
	if err := s.channels.Delete(ctx, id); err != nil {
		return failure("delete channel transaction failed", err)
	}
	s.invalidator.InvalidateAll()
	return nil
}

func (s *adminService) encryptKey(ctx context.Context, raw string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, failure("encryption canceled", err)
	}
	enc, err := s.encrypt([]byte(raw), s.masterKey)
	if err != nil {
		return nil, failure("Key encryption failed", err)
	}
	if len(enc) == 0 || len(enc) > 512 {
		return nil, failure("invalid encrypted Key size", nil)
	}
	return enc, nil
}

func (s *adminService) AddKey(ctx context.Context, id int64, raw string) (*KeyView, error) {
	if id <= 0 || !validRawKey(raw) {
		return nil, errInvalidInput
	}
	enc, err := s.encryptKey(ctx, raw)
	if err != nil {
		return nil, err
	}
	k, err := s.keys.CreateOwned(ctx, id, enc)
	if err != nil {
		return nil, failure("create owned Key transaction failed", err)
	}
	s.invalidator.InvalidateAll()
	return keyView(k, id)
}

func (s *adminService) UpdateKey(ctx context.Context, channelID, keyID int64, weight int32, status int8) (*KeyView, error) {
	if channelID <= 0 || keyID <= 0 || weight < 1 || weight > 10000 || (status != 0 && status != 1) {
		return nil, errInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return nil, failure("Key update canceled", err)
	}
	k, err := s.keys.UpdateOwned(ctx, channelID, keyID, weight, status)
	if err != nil {
		return nil, failure("update owned Key transaction failed", err)
	}
	s.invalidator.InvalidateAll()
	return keyView(k, channelID)
}

func (s *adminService) RemoveKey(ctx context.Context, channelID, keyID int64) error {
	if channelID <= 0 || keyID <= 0 {
		return errInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return failure("Key removal canceled", err)
	}
	if err := s.keys.RemoveOwned(ctx, channelID, keyID); err != nil {
		return failure("remove owned Key transaction failed", err)
	}
	s.invalidator.InvalidateAll()
	return nil
}

func configRows(id uint64, in UpdateChannelInput) (*store.Channel, []store.ChannelModel) {
	ch := &store.Channel{ID: id, Name: in.Name, Provider: in.Provider, BaseURL: in.BaseURL,
		Priority: in.Priority, Weight: in.Weight, Enabled: in.Enabled}
	models := make([]store.ChannelModel, 0, len(in.Models))
	for _, m := range in.Models {
		models = append(models, store.ChannelModel{Model: m.Model, UpstreamModel: m.UpstreamModel})
	}
	return ch, models
}

func (s *adminService) checkModels(ctx context.Context, mappings []ModelMappingInput) error {
	if err := ctx.Err(); err != nil {
		return failure("model validation canceled", err)
	}
	aliases, err := s.models.List(ctx)
	if err != nil {
		return failure("list model aliases failed", err)
	}
	known := make(map[string]bool, len(aliases))
	for _, alias := range aliases {
		known[alias.Name] = true
	}
	for _, mapping := range mappings {
		if !known[mapping.Model] {
			return errInvalidInput
		}
	}
	return nil
}
