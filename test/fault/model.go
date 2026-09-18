package fault

import (
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

// toOperations converts recorded Ops into Porcupine's Operation type,
// excluding any Op whose Err is non-empty: an operation that never
// received a definitive response has an unknown effect on server state
// (it may or may not have committed), and including it with a guessed
// outcome would make the check either meaningless or unfairly strict.
// This mirrors standard practice in linearizability testing (e.g. Jepsen)
// of treating indeterminate operations as excluded from the checked
// history rather than asserting an outcome for them.
func toOperations(history []Op) []porcupine.Operation {
	ops := make([]porcupine.Operation, 0, len(history))
	for _, o := range history {
		if o.Err != "" {
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
