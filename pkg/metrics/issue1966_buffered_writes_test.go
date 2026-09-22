package metrics

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TestIssue1966BufferedWritesReduceCommitCountUnderIndependentAgentReports
// reproduces the real-world #1966 driver found by live-testing on a ~20-guest
// fleet: internal/monitoring/monitor.go's updateResourceStore runs at every
// *ingest boundary* — once per accepted agent report — not once per shared
// poll cycle. With N independently-timed agents each reporting on its own
// schedule, that's up to N separate WriteBatchBounded-style commits per
// second, landing tens of seconds apart from each other rather than
// clustered — which is why the #1966 coalesceQueuedRequests linger-window
// fix (TestIssue1966WriteCoalescingReducesCommitCount) measured only an
// ~11% live commit-count reduction instead of the far larger synthetic win
// it showed for genuinely simultaneous writes.
//
// WriteBatchBuffered addresses the actual pattern: route these calls through
// the store's existing in-memory sample buffer (the one Write/WriteWithTier
// already use) so independent, spread-out report-driven writes accumulate
// and flush together on the FlushInterval cadence, instead of each
// committing on its own.
func TestIssue1966BufferedWritesReduceCommitCountUnderIndependentAgentReports(t *testing.T) {
	const agents = 20
	// Each agent reports once, at its own evenly-spaced offset across the
	// full window — 200ms apart, comfortably past both the old code's
	// instant-drain window and the #1966 150ms coalesce linger, so
	// WriteBatchBounded here commits close to once per agent regardless of
	// that earlier fix.
	const perAgentGap = 200 * time.Millisecond
	const flushInterval = 500 * time.Millisecond

	runScenario := func(t *testing.T, buffered bool) (commits int) {
		t.Helper()

		dir := t.TempDir()
		cfg := DefaultConfig(dir)
		cfg.FlushInterval = flushInterval
		store, err := NewStore(cfg)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		defer store.Close()

		if err := store.WaitForMaintenance(10 * time.Second); err != nil {
			t.Fatalf("WaitForMaintenance: %v", err)
		}

		var buf bytes.Buffer
		var bufMu sync.Mutex
		origLogger := log.Logger
		log.Logger = zerolog.New(zerolog.SyncWriter(lockedWriter{&buf, &bufMu})).Level(zerolog.DebugLevel)
		t.Cleanup(func() { log.Logger = origLogger })

		ts := time.Now().UTC()
		var wg sync.WaitGroup
		for a := 0; a < agents; a++ {
			wg.Add(1)
			go func(agent int) {
				defer wg.Done()
				time.Sleep(time.Duration(agent) * perAgentGap)
				metrics := []WriteMetric{{
					ResourceType: "agent",
					ResourceID:   fmt.Sprintf("agent-%d", agent),
					MetricType:   "cpu",
					Value:        float64(agent),
					Timestamp:    ts.Add(time.Duration(agent) * time.Second),
					Tier:         TierRaw,
				}}
				if buffered {
					store.WriteBatchBuffered(metrics)
				} else {
					store.WriteBatchBounded(metrics)
				}
			}(a)
		}
		wg.Wait()

		// Let any pending buffered flush (FlushInterval) land.
		time.Sleep(cfg.FlushInterval + 200*time.Millisecond)
		store.Flush()

		bufMu.Lock()
		logged := buf.String()
		bufMu.Unlock()
		commits = strings.Count(logged, "Wrote metrics batch")
		return commits
	}

	totalSpread := time.Duration(agents) * perAgentGap
	boundedCommits := runScenario(t, false)
	bufferedCommits := runScenario(t, true)

	t.Logf("WriteBatchBounded (old): %d commits for %d agents, one write each %v apart", boundedCommits, agents, perAgentGap)
	t.Logf("WriteBatchBuffered (fix): %d commits for %d agents over ~%v with %v flush interval", bufferedCommits, agents, totalSpread, flushInterval)

	if boundedCommits < agents/2 {
		t.Fatalf("expected WriteBatchBounded to commit close to once per independent write (spread %v apart, no clustering), got only %d commits for %d writes", perAgentGap, boundedCommits, agents)
	}
	if bufferedCommits >= boundedCommits {
		t.Fatalf("expected buffering to commit far less often than WriteBatchBounded, got %d (buffered) vs %d (bounded)", bufferedCommits, boundedCommits)
	}
	// The whole scenario spans ~totalSpread with a flushInterval-cadence
	// flush, so buffered commits should land near totalSpread/flushInterval,
	// not near the write count.
	maxExpectedBufferedCommits := int(totalSpread/flushInterval) + 3
	if bufferedCommits > maxExpectedBufferedCommits {
		t.Fatalf("expected buffered commits bounded by flush cadence (~%d), got %d", maxExpectedBufferedCommits, bufferedCommits)
	}
}
