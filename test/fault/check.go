package fault

import (
	"os"
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
func CheckAndCommit(t *testing.T, name string, history []Op) {
	t.Helper()

	included := 0
	for _, o := range history {
		if o.Err == "" {
			included++
		}
	}
	t.Logf("%s: %d operations recorded, %d included in the linearizability check (%d excluded: indeterminate outcome after retries were exhausted)",
		name, len(history), included, len(history)-included)

	ops := toOperations(history)
	result, info := porcupine.CheckOperationsVerbose(kvModel, ops, 30*time.Second)
	linearizable := result == porcupine.Ok

	if err := WriteHistory(name, history, linearizable); err != nil {
		t.Fatalf("%s: writing history file: %v", name, err)
	}

	if !linearizable {
		path, visErr := writeVisualization(name, info)
		if visErr == nil {
			t.Logf("%s: linearizability FAILED (result=%s) -- visualization written to %s", name, result, path)
		} else {
			t.Logf("%s: linearizability FAILED (result=%s), and writing visualization also failed: %v", name, result, visErr)
		}
		t.Fatalf("%s: history is not linearizable (Porcupine result: %s) -- see test/fault/results/%s.json", name, result, name)
	}

	t.Logf("%s: PASS -- history is linearizable (%d ops checked)", name, included)
}

func writeVisualization(name string, info porcupine.LinearizationInfo) (string, error) {
	path := "results/" + name + ".html"
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
