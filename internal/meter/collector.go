package meter

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"math/big"
	"strconv"

	"gaigate/internal/provider"
)

var (
	errInput    = errors.New("meter: invalid measurement input")
	errUsage    = errors.New("meter: invalid or conflicting usage")
	errOverflow = errors.New("meter: measurement overflow")
	errRatio    = errors.New("meter: invalid estimate chars per token")
	errFinished = errors.New("meter: collector already finished with different input")
)

// Every retained field has fixed size. In particular, neither request bytes nor
// output fragments survive the call that consumes them.
type collector struct {
	inputChars, outputChars int64
	numerator, denominator  int64
	usage                   provider.Usage
	hasUsage                bool
	err                     error
	finished                bool
	finishKey               [sha256.Size]byte
	result                  Measurement
	finishErr               error
}

func NewCollector(req *provider.ChatRequest, charsPerToken float64) (Collector, error) {
	num, den, err := ratio(charsPerToken)
	if err != nil {
		return nil, err
	}
	if req == nil || len(req.Messages) == 0 {
		return nil, errInput
	}
	messages := make([]any, len(req.Messages))
	for i, raw := range req.Messages {
		m, err := object(raw)
		if err != nil {
			return nil, err
		}
		role, ok := m["role"].(string)
		if !ok || role == "" {
			return nil, errInput
		}
		messages[i] = m
	}
	n, err := canonicalChars(messages)
	if err != nil {
		return nil, err
	}
	for _, key := range []string{"tools", "tool_choice", "response_format"} {
		if raw, ok := req.Extra[key]; ok {
			v, err := decode(raw)
			if err != nil {
				return nil, err
			}
			part, err := canonicalChars(v)
			if err != nil {
				return nil, err
			}
			n, err = add(n, part)
			if err != nil {
				return nil, err
			}
		}
	}
	return &collector{inputChars: n, numerator: num, denominator: den}, nil
}

// The shortest decimal representation is the contract, with no ULP relaxation.
func ratio(c float64) (int64, int64, error) {
	if math.IsNaN(c) || math.IsInf(c, 0) || c < 0.001 || c > 1024 {
		return 0, 0, errRatio
	}
	r, ok := new(big.Rat).SetString(strconv.FormatFloat(c, 'f', -1, 64))
	if !ok {
		return 0, 0, errRatio
	}
	scaled := new(big.Rat).Mul(r, big.NewRat(1000000, 1))
	if !scaled.IsInt() {
		return 0, 0, errRatio
	}
	return r.Num().Int64(), r.Denom().Int64(), nil
}

func validUsage(u provider.Usage) bool {
	return u.InputTokens >= 0 && u.OutputTokens >= 0 && u.InputTokens <= math.MaxInt64-u.OutputTokens
}

func mergeUsage(target **provider.Usage, u *provider.Usage) error {
	if u == nil {
		return nil
	}
	if !validUsage(*u) || (*target != nil && **target != *u) {
		return errUsage
	}
	copyUsage := *u
	*target = &copyUsage
	return nil
}

func (c *collector) Observe(e provider.StreamEvent) {
	if c.finished || c.err != nil {
		return
	}
	if e.Err != nil || (e.Kind != provider.Chunk && e.Kind != provider.UsageDone) {
		c.err = errInput
		return
	}
	root, err := object(e.Raw)
	if err != nil {
		c.err = err
		return
	}
	u, err := bodyUsage(root)
	if err != nil {
		c.err = err
		return
	}
	if e.Kind == provider.UsageDone {
		if e.Usage == nil {
			c.err = errUsage
			return
		}
		if err := mergeUsage(&u, e.Usage); err != nil {
			c.err = err
			return
		}
		c.usage, c.hasUsage = *u, true // cumulative frames replace, never add
		return
	}
	if e.Usage != nil || u != nil {
		c.err = errUsage
		return
	}
	n, err := outputChars(root, "delta")
	if err != nil {
		c.err = err
		return
	}
	c.outputChars, c.err = add(c.outputChars, n)
}

func (c *collector) Finish(usage *provider.Usage, response *provider.ChatResponse) (Measurement, error) {
	key := finishKey(usage, response)
	if c.finished {
		if key != c.finishKey {
			return Measurement{}, errFinished
		}
		return c.result, c.finishErr
	}
	c.finished, c.finishKey = true, key
	c.result, c.finishErr = c.finish(usage, response)
	return c.result, c.finishErr
}

func (c *collector) finish(usage *provider.Usage, response *provider.ChatResponse) (Measurement, error) {
	if c.err != nil {
		return Measurement{}, c.err
	}
	var selected *provider.Usage
	if err := mergeUsage(&selected, usage); err != nil {
		return Measurement{}, err
	}
	if c.hasUsage {
		if err := mergeUsage(&selected, &c.usage); err != nil {
			return Measurement{}, err
		}
	}
	output := c.outputChars
	if response != nil {
		root, err := object(response.Body)
		if err != nil {
			return Measurement{}, err
		}
		u, err := bodyUsage(root)
		if err != nil {
			return Measurement{}, err
		}
		for _, source := range []*provider.Usage{response.Usage, u} {
			if err := mergeUsage(&selected, source); err != nil {
				return Measurement{}, err
			}
		}
		// Nonstream response is an alternative representation, not extra output.
		output, err = outputChars(root, "message")
		if err != nil {
			return Measurement{}, err
		}
	}
	if selected != nil {
		return Measurement{Usage: *selected}, nil
	}
	in, err := ceilTokens(c.inputChars, c.numerator, c.denominator)
	if err != nil {
		return Measurement{}, err
	}
	out, err := ceilTokens(output, c.numerator, c.denominator)
	if err != nil {
		return Measurement{}, err
	}
	u := provider.Usage{InputTokens: in, OutputTokens: out}
	if !validUsage(u) {
		return Measurement{}, errOverflow
	}
	return Measurement{Usage: u, Estimated: true}, nil
}

func ceilTokens(chars, numerator, denominator int64) (int64, error) {
	if chars < 0 || numerator <= 0 || denominator <= 0 {
		return 0, errOverflow
	}
	n := new(big.Int).Mul(big.NewInt(chars), big.NewInt(denominator))
	n.Add(n, big.NewInt(numerator-1)).Quo(n, big.NewInt(numerator))
	if !n.IsInt64() {
		return 0, errOverflow
	}
	return n.Int64(), nil
}

// Fingerprint nil-presence, scalar snapshots and exact response bytes, retaining
// only a fixed-size digest for repeated Finish calls, including failed calls.
func finishKey(u *provider.Usage, response *provider.ChatResponse) [sha256.Size]byte {
	h := sha256.New()
	hashUsage(h, u)
	if response == nil {
		_, _ = h.Write([]byte{0})
	} else {
		_, _ = h.Write([]byte{1})
		hashUsage(h, response.Usage)
		_, _ = h.Write(response.Body)
	}
	var key [sha256.Size]byte
	copy(key[:], h.Sum(nil))
	return key
}

func hashUsage(h hash.Hash, u *provider.Usage) {
	if u == nil {
		_, _ = h.Write([]byte{0})
		return
	}
	var b [17]byte
	b[0] = 1
	binary.BigEndian.PutUint64(b[1:9], uint64(u.InputTokens))
	binary.BigEndian.PutUint64(b[9:17], uint64(u.OutputTokens))
	_, _ = h.Write(b[:])
}
