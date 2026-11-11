// Package meter provides accounting contracts and per-attempt measurement.
package meter

import (
	"context"
	"time"

	"gaigate/internal/access"
	"gaigate/internal/provider"
)

type Measurement struct {
	Usage     provider.Usage
	Estimated bool
}

// Collector is owned by one attempt and called serially. Observe consumes only
// successfully forwarded Chunk/UsageDone events; it never retains their storage.
type Collector interface {
	Observe(provider.StreamEvent)
	Finish(*provider.Usage, *provider.ChatResponse) (Measurement, error)
}

type PriceQuote struct {
	Model                           string
	PriceID                         uint64
	InputPricePerM, OutputPricePerM int64
	At                              time.Time
}

type Settlement struct {
	RequestID                            string
	UserID, APIKeyID, GroupID, ChannelID int64
	Model, Status                        string
	Usage                                provider.Usage
	Cost                                 int64
	Estimated                            bool
	CompletedAt                          time.Time
	Latency                              time.Duration
	Quote                                *PriceQuote
}

type Pricer interface {
	Quote(context.Context, string, time.Time) (PriceQuote, error)
	Calculate(PriceQuote, provider.Usage) (int64, error)
}

type Recorder interface {
	Pricer
	Commit(context.Context, Settlement) error
}

type QuotaGuard interface {
	Precheck(context.Context, *access.Principal) error
	Settle(context.Context, Settlement) error
}
