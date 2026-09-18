package fault

import (
	"math"

	"github.com/anishathalye/porcupine"
)

// kvInput/kvOutput/kvState define the sequential specification Porcupine
// checks the recorded history against: a simple last-write-wins register
// per key. Put always succeeds (an errored Put is excluded from the
// history entirely -- see toOperations); Get must return whatever the
// most recent linearized Put wrote, or not-found if there hasn't been one.
type kvInput struct {
	Key   string
	Kind  string // "put" or "get"
	Value string
}

type kvOutput struct {
	Found bool
	Value string
}

type kvState struct {
	Exists bool
	Value  string
}

// kvModel is partitioned by key: QuorumKV's keys are fully independent of
// each other (a Put to one key never affects another), so checking each
// key's sub-history separately is both correct and lets Porcupine avoid
// needless cross-key interleavings, which matters for performance on
// anything but a tiny history.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).Key
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} {
		return kvState{}
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(kvState)
		in := input.(kvInput)
		out := output.(kvOutput)

		if in.Kind == "put" {
			return true, kvState{Exists: true, Value: in.Value}
		}
		// "get"
		ok := out.Found == st.Exists && (!st.Exists || out.Value == st.Value)
		return ok, st
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		out := output.(kvOutput)
		if in.Kind == "put" {
			return "Put(" + in.Key + ", " + in.Value + ")"
		}
		if out.Found {
			return "Get(" + in.Key + ") -> " + out.Value
		}
		return "Get(" + in.Key + ") -> <not found>"
	},
}

// toOperations converts recorded Ops into Porcupine's Operation type.
//
// A Put that errored after all retries has an unknown outcome: it may have
// committed (e.g. the leader applied it and died before replying) or not.
// Dropping it, as an earlier version of this file did, can hide a real
// violation: a later Get that returns that Put's value would then look like
// a read of a value nobody wrote -- or, worse, an errored Put that DID take
// effect would silently vanish from the check. Instead it is kept as an
// operation that never returned (Return = MaxInt64): the checker may
// linearize it at any point after its call, which covers "it happened,
// somewhere in that window", or after every other operation, which is
// indistinguishable from "it never happened". That is the standard
// treatment of indeterminate operations (Jepsen's :info operations).
//
// An errored Get has no effect on state whichever way it went, so it is
// the one kind of operation that is still safe to drop.
func toOperations(history []Op) []porcupine.Operation {
	ops := make([]porcupine.Operation, 0, len(history))
	for _, o := range history {
		if o.Err != "" {
			if o.Kind != "put" {
				continue
			}
			ops = append(ops, porcupine.Operation{
				ClientId: o.ClientID,
				Input:    kvInput{Key: o.Key, Kind: o.Kind, Value: o.Value},
				Call:     o.CallNS,
				Output:   kvOutput{},
				Return:   math.MaxInt64,
			})
			continue
		}
		ops = append(ops, porcupine.Operation{
			ClientId: o.ClientID,
			Input:    kvInput{Key: o.Key, Kind: o.Kind, Value: o.Value},
			Call:     o.CallNS,
			Output:   kvOutput{Found: o.Found, Value: o.GotValue},
			Return:   o.ReturnNS,
		})
	}
	return ops
}
