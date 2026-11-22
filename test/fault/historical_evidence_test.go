package fault

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Replay the published observations, independently of their recorded verdicts.
func TestHistoricalOctoberHistories(t *testing.T) {
	counts := map[string]int{"leader_partition": 40, "minority_partition": 40, "rolling_restarts": 488, "network_delay": 18, "double_leader_attempt": 21}
	for name, count := range counts {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "..", "docs", "evidence", "historical-20261026", name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var record struct {
				Ops []Op `json:"ops"`
			}
			if err := json.Unmarshal(data, &record); err != nil {
				t.Fatal(err)
			}
			if len(record.Ops) != count {
				t.Fatalf("got %d attempts, want %d", len(record.Ops), count)
			}
			if err := CheckHistory(record.Ops); err != nil {
				t.Fatal(err)
			}
		})
	}
}
