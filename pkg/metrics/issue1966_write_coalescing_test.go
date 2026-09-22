package metrics

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TestIssue1966WriteCoalescingReducesCommitCount reproduces the scenario from
// https://github.com/rcourtman/Pulse/issues/1966: on a live install,
// monitor.go's four independent per-provider collector passes (agent, VM,
// storage, app-container) each call WriteBatchBounded separately per poll
// cycle, landing in s.writeCh a few milliseconds apart rather than all at
// once. Reporters measured continuous multi-MB/s writes against metrics.db
// with only tens of KB/s of net logical growth — i.e. each of these near-
// simultaneous writes was committing (BEGIN/INSERT×N/COMMIT against the
// metrics table + its 3 indexes) on its own instead of merging.
//
// This test drives two real Store instances (real SQLite files, real WAL
// mode, the same NewStore path production uses) through several simulated
// poll cycles with that same few-millisecond stagger, and counts actual
// commits via the store's own "Wrote metrics batch" log line (one per
// writeBatch call, i.e. one per SQLite transaction). One store has
// writeCoalesceWindow forced to 0 to reproduce the pre-fix behavior
// (coalesceQueuedRequests only drains what happens to already be queued);
// the other uses the fixed NewStore default. It asserts the fix collapses
// each cycle's several independent commits down to one.
func TestIssue1966WriteCoalescingReducesCommitCount(t *testing.T) {
	const cycles = 8
	const writersPerCycle = 4
	const cycleGap = 300 * time.Millisecond // well past the coalesce window, so cycles don't bleed together

	runScenario := func(t *testing.T, disableCoalescing bool) (commits int, dbBytes, walBytes int64) {
		t.Helper()

		dir := t.TempDir()
		cfg := DefaultConfig(dir)
		store, err := NewStore(cfg)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		defer store.Close()

		if err := store.WaitForMaintenance(10 * time.Second); err != nil {
			t.Fatalf("WaitForMaintenance: %v", err)
		}

		if disableCoalescing {
			store.writeCoalesceWindow = 0
		}

		var buf bytes.Buffer
		var bufMu sync.Mutex
		origLogger := log.Logger
		log.Logger = zerolog.New(zerolog.SyncWriter(lockedWriter{&buf, &bufMu})).Level(zerolog.DebugLevel)
		t.Cleanup(func() { log.Logger = origLogger })

		ts := time.Now().UTC()
		for cycle := 0; cycle < cycles; cycle++ {
			var wg sync.WaitGroup
			// Same four independent collector passes as
			// internal/monitoring/monitor.go: syncUnifiedAgentMetrics,
			// syncUnifiedVMMetrics, syncUnifiedStorageMetrics,
			// syncUnifiedAppContainerMetrics — each fires its own write a
			// few ms after the others rather than in lockstep.
			for w := 0; w < writersPerCycle; w++ {
				wg.Add(1)
				go func(cycle, writer int) {
					defer wg.Done()
					time.Sleep(time.Duration(writer) * 5 * time.Millisecond)
					store.WriteBatchBounded([]WriteMetric{{
						ResourceType: "vm",
						ResourceID:   fmt.Sprintf("vm-%d", writer),
						MetricType:   "cpu",
						Value:        float64(cycle),
						Timestamp:    ts.Add(time.Duration(cycle) * time.Second),
						Tier:         TierRaw,
					}})
				}(cycle, w)
			}
			wg.Wait()
			time.Sleep(cycleGap)
		}

		bufMu.Lock()
		logged := buf.String()
		bufMu.Unlock()
		commits = strings.Count(logged, "Wrote metrics batch")

		if info, err := os.Stat(filepath.Join(dir, "metrics.db")); err == nil {
			dbBytes = info.Size()
		}
		if info, err := os.Stat(filepath.Join(dir, "metrics.db-wal")); err == nil {
			walBytes = info.Size()
		}
		return commits, dbBytes, walBytes
	}

	beforeCommits, beforeDB, beforeWAL := runScenario(t, true)
	afterCommits, afterDB, afterWAL := runScenario(t, false)

	t.Logf("without coalescing (old behavior): %d commits over %d cycles (%d writers/cycle) — metrics.db=%dB wal=%dB",
		beforeCommits, cycles, writersPerCycle, beforeDB, beforeWAL)
	t.Logf("with coalescing (fix):             %d commits over %d cycles (%d writers/cycle) — metrics.db=%dB wal=%dB",
		afterCommits, cycles, writersPerCycle, afterDB, afterWAL)

	if beforeCommits <= cycles {
		t.Fatalf("expected old behavior to commit more than once per cycle on average (writers miss each other), got %d commits over %d cycles", beforeCommits, cycles)
	}
	if afterCommits > cycles {
		t.Fatalf("expected the fix to collapse each cycle's %d writers into exactly one commit (%d cycles), got %d commits", writersPerCycle, cycles, afterCommits)
	}
	if afterCommits >= beforeCommits {
		t.Fatalf("expected coalescing to reduce commit count, got %d (with) vs %d (without)", afterCommits, beforeCommits)
	}
}

type lockedWriter struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
