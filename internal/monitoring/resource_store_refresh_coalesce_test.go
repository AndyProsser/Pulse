package monitoring

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
	"github.com/rcourtman/pulse-go-rewrite/internal/unifiedresources"
	"github.com/rcourtman/pulse-go-rewrite/pkg/metrics"
)

// slowCountingStore wraps a real *unifiedresources.MonitorAdapter (see
// getAllCountingStore in issue1966_getall_once_test.go for why embedding the
// concrete type matters), counting GetAll() calls and letting each one be
// artificially slowed so concurrent refresh calls actually overlap in a
// test.
type slowCountingStore struct {
	*unifiedresources.MonitorAdapter
	getAllCalls atomic.Int64
	delay       time.Duration
}

func (s *slowCountingStore) GetAll() []unifiedresources.Resource {
	s.getAllCalls.Add(1)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.MonitorAdapter.GetAll()
}

func newCoalesceTestMonitor(t *testing.T, store *slowCountingStore) *Monitor {
	t.Helper()
	metricsStore, err := metrics.NewStore(metrics.DefaultConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("metrics.NewStore: %v", err)
	}
	t.Cleanup(func() { metricsStore.Close() })

	return &Monitor{
		state:          models.NewState(),
		resourceStore:  store,
		metricsHistory: NewMetricsHistory(1024, 24*time.Hour),
		metricsStore:   metricsStore,
	}
}

// TestRefreshUnifiedResourceStoreCoalescesConcurrentCallers is the
// regression test for the fix itself: N goroutines calling
// refreshUnifiedResourceStoreAfterAgentStateChange while a rebuild is
// already running must not each trigger their own full rebuild. At most two
// actual rebuild passes should happen - the one already running (which
// every late arrival missed the start of) plus at most one catch-up pass
// for whatever arrived while it ran.
func TestRefreshUnifiedResourceStoreCoalescesConcurrentCallers(t *testing.T) {
	store := &slowCountingStore{
		MonitorAdapter: unifiedresources.NewMonitorAdapter(nil),
		delay:          50 * time.Millisecond,
	}
	monitor := newCoalesceTestMonitor(t, store)

	const callers = 20
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			monitor.refreshUnifiedResourceStoreAfterAgentStateChange()
		}()
	}
	wg.Wait()

	if got := store.getAllCalls.Load(); got > 2 {
		t.Fatalf("GetAll() called %d times for %d concurrent refresh triggers, want at most 2 (one in-flight rebuild + one coalesced catch-up)", got, callers)
	}
	if got := store.getAllCalls.Load(); got < 1 {
		t.Fatalf("GetAll() never called - refresh did not run at all")
	}
}

// TestRefreshUnifiedResourceStoreUncontendedCallRunsImmediately confirms the
// common (non-concurrent) case is unchanged: a single call still runs its
// rebuild synchronously, inline, before returning - not deferred to a
// background goroutine or a later window. This is the "immediately visible"
// guarantee the function's doc comment makes.
func TestRefreshUnifiedResourceStoreUncontendedCallRunsImmediately(t *testing.T) {
	store := &slowCountingStore{MonitorAdapter: unifiedresources.NewMonitorAdapter(nil)}
	monitor := newCoalesceTestMonitor(t, store)

	monitor.refreshUnifiedResourceStoreAfterAgentStateChange()

	if got := store.getAllCalls.Load(); got != 1 {
		t.Fatalf("GetAll() called %d times for a single uncontended refresh, want exactly 1", got)
	}

	// A second, later, isolated call must also run immediately - the
	// running/pending flags must not get stuck set after the first call.
	monitor.refreshUnifiedResourceStoreAfterAgentStateChange()
	if got := store.getAllCalls.Load(); got != 2 {
		t.Fatalf("GetAll() called %d times after two sequential uncontended refreshes, want exactly 2", got)
	}
}

// TestRefreshUnifiedResourceStoreLateArrivalGetsCaughtUp confirms a call
// that arrives *after* an in-flight rebuild has already read its snapshot
// still gets its change published, via the coalesced catch-up pass, rather
// than being silently dropped.
func TestRefreshUnifiedResourceStoreLateArrivalGetsCaughtUp(t *testing.T) {
	store := &slowCountingStore{
		MonitorAdapter: unifiedresources.NewMonitorAdapter(nil),
		delay:          80 * time.Millisecond,
	}
	monitor := newCoalesceTestMonitor(t, store)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitor.refreshUnifiedResourceStoreAfterAgentStateChange()
	}()

	// Give the first call time to start (and be mid-GetAll) before the
	// second arrives, so it's guaranteed to hit the "already running" path.
	time.Sleep(20 * time.Millisecond)
	monitor.refreshUnifiedResourceStoreAfterAgentStateChange()

	wg.Wait()

	if got := store.getAllCalls.Load(); got != 2 {
		t.Fatalf("GetAll() called %d times, want exactly 2 (in-flight pass + one coalesced catch-up for the late arrival)", got)
	}

	monitor.resourceStoreRefreshMu.Lock()
	running, pending := monitor.resourceStoreRefreshRunning, monitor.resourceStoreRefreshPending
	monitor.resourceStoreRefreshMu.Unlock()
	if running || pending {
		t.Fatalf("coalescer left running=%v pending=%v set after completing", running, pending)
	}
}
