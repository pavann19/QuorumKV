// Package diskhealth provides local admission hints, never consensus decisions.
package diskhealth

import "errors"

var ErrPressure = errors.New("disk: configured storage headroom exhausted")
var ErrUnknown = errors.New("disk: storage headroom unavailable")

type Space struct {
	Available uint64
	Total     uint64
}
type Policy struct {
	MaxUsedPercent int    `json:"max_used_percent"`
	MinFreeBytes   uint64 `json:"min_free_bytes"`
}

func DefaultPolicy() Policy { return Policy{90, 64 << 20} }
func (p Policy) Validate() error {
	if p.MaxUsedPercent < 1 || p.MaxUsedPercent > 99 || p.MinFreeBytes == 0 {
		return errors.New("disk: watermark must be 1..99 and minimum free bytes positive")
	}
	return nil
}

type Status struct {
	Space
	Degraded bool
	Known    bool
}

func Evaluate(s Space, p Policy) (Status, error) {
	result := Status{Space: s, Degraded: true}
	if err := p.Validate(); err != nil {
		return result, ErrUnknown
	}
	if s.Total == 0 || s.Available > s.Total {
		return result, ErrUnknown
	}
	result.Known = true
	// floor(total * free-percent / 100), without uint64 multiplication overflow.
	freePercent := uint64(100 - p.MaxUsedPercent)
	threshold := (s.Total/100)*freePercent + ((s.Total%100)*freePercent)/100
	if s.Available <= threshold || s.Available < p.MinFreeBytes {
		return result, ErrPressure
	}
	result.Degraded = false
	return result, nil
}
