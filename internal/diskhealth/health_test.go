package diskhealth

import (
	"errors"
	"math"
	"testing"
)

func TestWatermarkBoundariesAndUnknownFailClosed(t *testing.T) {
	p := Policy{MaxUsedPercent: 90, MinFreeBytes: 5}
	cases := []struct {
		s    Space
		want error
	}{{Space{11, 100}, nil}, {Space{10, 100}, ErrPressure}, {Space{4, 20}, ErrPressure}, {Space{0, 100}, ErrPressure}, {Space{1, 0}, ErrUnknown}, {Space{101, 100}, ErrUnknown}, {Space{math.MaxUint64, math.MaxUint64}, nil}}
	for _, tc := range cases {
		status, err := Evaluate(tc.s, p)
		if !errors.Is(err, tc.want) || status.Degraded != (tc.want != nil) {
			t.Fatalf("%+v: %+v %v", tc.s, status, err)
		}
	}
}
func TestPolicyValidation(t *testing.T) {
	for _, p := range []Policy{{0, 1}, {100, 1}, {90, 0}} {
		if p.Validate() == nil {
			t.Fatal("invalid policy", p)
		}
	}
}
func TestProbeOwnedTemporaryDirectory(t *testing.T) {
	s, err := Probe(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if s.Total == 0 || s.Available > s.Total {
		t.Fatalf("invalid filesystem observation %+v", s)
	}
}
