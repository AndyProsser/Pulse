package metrics

import (
	"path/filepath"
	"testing"
	"time"
)

// A database whose time-major idx_metrics_query_all is already unique (as
// written by main-line builds) must not get idx_metrics_lookup back on start,
// and a leftover lookup index must be dropped.
func TestEnsureMetricsIdentityIndexSkipsLookupWhenTimeMajorUnique(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.DBPath = filepath.Join(dir, "metrics-time-major.db")
	cfg.FlushInterval = time.Hour

	store, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, err := store.db.Exec(`
		DROP INDEX IF EXISTS idx_metrics_query_all;
		CREATE UNIQUE INDEX idx_metrics_query_all
		ON metrics(resource_type, resource_id, tier, timestamp, metric_type);
	`); err != nil {
		t.Fatalf("make time-major index unique: %v", err)
	}
	// Lookup is still present here (created by the first start); the next
	// start must remove it rather than keep two identity trees.
	store.Close()

	store, err = NewStore(cfg)
	if err != nil {
		t.Fatalf("reopen NewStore: %v", err)
	}
	defer store.Close()

	exists, err := store.metricsIndexExists("idx_metrics_lookup")
	if err != nil {
		t.Fatalf("metricsIndexExists: %v", err)
	}
	if exists {
		t.Fatal("idx_metrics_lookup was kept or recreated despite a unique time-major identity index")
	}
	ok, err := store.metricsIndexMatches("idx_metrics_query_all", true, metricsTimeMajorIdentityColumns)
	if err != nil || !ok {
		t.Fatalf("idx_metrics_query_all should remain the unique identity index (ok=%v err=%v)", ok, err)
	}

	// Writes must still upsert on the five identity columns.
	now := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 2; i++ {
		store.writeBatch([]bufferedMetric{{resourceType: "vm", resourceID: "vm-1", metricType: "cpu", value: float64(i), timestamp: now, tier: TierRaw}})
	}
	var n int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM metrics WHERE resource_id = 'vm-1'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rows for repeated identity = %d, want 1 (upsert on unique identity)", n)
	}
}
