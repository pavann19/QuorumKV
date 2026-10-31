package fault

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// CheckAndCommit converts history to Porcupine operations, runs the
// linearizability checker, always writes the full history (and the
// verdict) to test/fault/results/<name>.json regardless of outcome, and
// fails the test if the result isn't a clean pass -- per the build plan:
// "if any scenario produces a FAIL, that's not a failure of the project --
// that's the most valuable finding in it. Document it honestly." Writing
// the file happens before the t.Fatal, so a real failure is never lost.
func CheckAndCommit(t *testing.T, name string, history []Op, events ...FaultEvent) {
	t.Helper()
	if err := WriteHistory(name, history, false, events...); err != nil {
		t.Fatal(err)
	}
	if err := ValidateHistory(history); err != nil {
		t.Fatalf("%s: invalid attempt history: %v", name, err)
	}
	if err := validateWorkloadCoverage(history); err != nil {
		t.Fatalf("%s: ineffective workload: %v", name, err)
	}

	definite, uncertain, rejected, failedReads := 0, 0, 0, 0
	for _, o := range history {
		switch {
		case o.Err == "":
			definite++
		case (o.Kind == "put" || o.Kind == "delete") && o.Outcome == "uncertain":
			uncertain++
		case o.Outcome == "rejected":
			rejected++
		default:
			failedReads++
		}
	}
	t.Logf("%s: attempts=%d acknowledged=%d uncertain_mutations=%d rejected=%d failed_reads=%d", name, len(history), definite, uncertain, rejected, failedReads)

	ops := toOperations(history)
	result, info := porcupine.CheckOperationsVerbose(kvModel, ops, 30*time.Second)
	linearizable := result == porcupine.Ok

	if err := WriteHistory(name, history, linearizable, events...); err != nil {
		t.Fatalf("%s: writing history file: %v", name, err)
	}

	if !linearizable {
		path, visErr := writeVisualization(name, info)
		if visErr == nil {
			t.Logf("%s: linearizability FAILED (result=%s) -- visualization written to %s", name, result, path)
		} else {
			t.Logf("%s: linearizability FAILED (result=%s), and writing visualization also failed: %v", name, result, visErr)
		}
		t.Fatalf("%s: observed history rejected (Porcupine result: %s) -- see %s", name, result, filepath.Join(ResultsDir(), name+".json"))
	}

	t.Logf("%s: PASS -- observed history accepted (%d attempts modeled)", name, len(ops))
}

func writeVisualization(name string, info porcupine.LinearizationInfo) (string, error) {
	path := filepath.Join(ResultsDir(), name+".html")
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := porcupine.Visualize(kvModel, info, f); err != nil {
		return "", err
	}
	return path, nil
}
