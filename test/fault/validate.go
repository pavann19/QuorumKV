package fault

import (
	"fmt"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
)

// ValidateHistory rejects missing attempts and contradictory metadata before
// passing observations to the sequential-state checker.
func ValidateHistory(history []Op) error {
	if len(history) == 0 {
		return fmt.Errorf("empty history")
	}
	ordered := append([]Op(nil), history...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].CallNS < ordered[j].CallNS })
	seen := map[int64]bool{}
	events := map[int64]bool{}
	lastOperation := map[int64]Op{}
	lastClient := map[int]Op{}
	for _, op := range ordered {
		if op.OperationID <= 0 || op.AttemptID <= 0 || op.Attempt < 1 || op.Node == "" {
			return fmt.Errorf("missing attempt identity: %+v", op)
		}
		if seen[op.AttemptID] {
			return fmt.Errorf("duplicate attempt %d", op.AttemptID)
		}
		seen[op.AttemptID] = true
		if op.CallNS <= 0 || op.ReturnNS <= op.CallNS || events[op.CallNS] || events[op.ReturnNS] {
			return fmt.Errorf("invalid event order for attempt %d", op.AttemptID)
		}
		events[op.CallNS], events[op.ReturnNS] = true, true
		if op.Kind != "put" && op.Kind != "delete" && op.Kind != "get" {
			return fmt.Errorf("unknown kind %q", op.Kind)
		}
		if op.Err == "" {
			if !op.Acknowledged || op.Outcome != "acknowledged" {
				return fmt.Errorf("missing acknowledgement for attempt %d", op.AttemptID)
			}
		} else if op.Acknowledged || (op.Kind == "get" && op.Outcome != "failed_read") || (op.Kind != "get" && op.Outcome != "rejected" && op.Outcome != "uncertain") {
			return fmt.Errorf("contradictory outcome for attempt %d", op.AttemptID)
		}
		if previous, ok := lastOperation[op.OperationID]; ok {
			if previous.ClientID != op.ClientID || previous.Key != op.Key || previous.Kind != op.Kind || previous.Value != op.Value || previous.Attempt+1 != op.Attempt || previous.Acknowledged || previous.ReturnNS >= op.CallNS {
				return fmt.Errorf("invalid retry for operation %d", op.OperationID)
			}
		} else if op.Attempt != 1 {
			return fmt.Errorf("missing first attempt for operation %d", op.OperationID)
		}
		if previous, ok := lastClient[op.ClientID]; ok && previous.ReturnNS >= op.CallNS {
			return fmt.Errorf("overlapping calls on sequential client %d", op.ClientID)
		}
		lastOperation[op.OperationID], lastClient[op.ClientID] = op, op
	}
	for id := int64(1); id <= int64(len(history)); id++ {
		if !seen[id] {
			return fmt.Errorf("missing attempt ID %d", id)
		}
	}
	return nil
}

// CheckHistory tests the observed attempt history against the single-key
// Put/Get/Delete specification. It makes no claim about unobserved executions.
func CheckHistory(history []Op) error {
	if err := ValidateHistory(history); err != nil {
		return err
	}
	result := porcupine.CheckOperationsTimeout(kvModel, toOperations(history), 30*time.Second)
	if result != porcupine.Ok {
		return fmt.Errorf("history checker returned %s", result)
	}
	return nil
}

func validateWorkloadCoverage(history []Op) error {
	written, read := map[string]bool{}, map[string]bool{}
	found := 0
	for _, op := range history {
		if !op.Acknowledged {
			continue
		}
		if op.Kind == "put" {
			written[op.Key] = true
		}
		if op.Kind == "get" {
			read[op.Key] = true
			if op.Found {
				found++
			}
		}
	}
	if len(written) == 0 || found == 0 {
		return fmt.Errorf("no acknowledged write followed by a found read")
	}
	for key := range written {
		if !read[key] {
			return fmt.Errorf("key %q was written but never read", key)
		}
	}
	for key := range read {
		if !written[key] {
			return fmt.Errorf("key %q was read but never written", key)
		}
	}
	return nil
}
