// Package requestlimits owns request and transport safety ceilings.
package requestlimits

import (
	"errors"
	"time"
)

const MaxKeyBytes = 4096
const MaxValueBytes = 1 << 20
const MaxMessageBytes = 4 << 20
const MaxInflight = 64
const MaxStreams = 128
const RPCWait = 5 * time.Second
const MaxWriteReadGraceMS = 25

// Experimental read preference has not passed the mixed-load tail gate.
const DefaultWriteReadGraceMS = 0

type Limits struct {
	WriteReadGraceMS int    `json:"write_read_grace_ms"`
	KeyBytes         int    `json:"max_key_bytes"`
	ValueBytes       int    `json:"max_value_bytes"`
	Inflight         int    `json:"max_inflight"`
	Streams          uint32 `json:"max_streams"`
	TimeoutMS        int    `json:"timeout_ms"`
}

func Defaults() Limits {
	return Limits{KeyBytes: MaxKeyBytes, ValueBytes: MaxValueBytes, Inflight: MaxInflight, Streams: MaxStreams, TimeoutMS: int(RPCWait / time.Millisecond), WriteReadGraceMS: DefaultWriteReadGraceMS}
}
func (l Limits) Validate() error {
	if l.WriteReadGraceMS < 0 || l.WriteReadGraceMS > MaxWriteReadGraceMS || l.KeyBytes < 1 || l.KeyBytes > MaxKeyBytes || l.ValueBytes < 1 || l.ValueBytes > MaxValueBytes || l.Inflight < 1 || l.Inflight > MaxInflight || l.Streams < 1 || l.Streams > MaxStreams || l.TimeoutMS < 1 || l.TimeoutMS > int(RPCWait/time.Millisecond) {
		return errors.New("request limits must be positive and within validated safety ceilings")
	}
	return nil
}
func (l Limits) Wait() time.Duration { return time.Duration(l.TimeoutMS) * time.Millisecond }
