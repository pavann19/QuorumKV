//go:build measure

package labcluster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Observe the unchanged mixed-load and exact-catchup fixture with the default
// one-entry replication setting. Before/after binaries use this identical test.
// This does not replace the broader batching acceptance matrix or its failures.
func TestMTLSHotPathMeasurement(t *testing.T) {
	output := os.Getenv("QUORUMKV_RESULTS_DIR")
	if output == "" {
		t.Fatal("explicit isolated measurement output required")
	}
	runs := []writeBatchMeasurement{}
	defer func() {
		report := map[string]any{"scope": "local mTLS, 3 nodes, one follower offline during load then exact catchup; unchanged default append=1; observations only", "go": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "runs": runs}
		raw, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(output, "mtls-hot-path.json"), append(raw, '\n'), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	for rep := 0; rep < 3; rep++ {
		result := measureWriteBatchProfile(t, true, 1024, 1, rep)
		runs = append(runs, result)
		if result.Error != "" || !result.HistoryChecked {
			t.Fatalf("profile %d: %+v", rep, result)
		}
		t.Logf("rep=%d writes=%d reads=%d writes/s=%.2f read-p99-ms=%.3f write-p99-ms=%.3f", rep, len(result.WriteNS), len(result.ReadNS), result.WritesPerSecond, float64(result.ReadP99NS)/1e6, float64(result.WriteP99NS)/1e6)
	}
}
