package monitoring

import (
	"testing"
	"time"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
	"github.com/rcourtman/pulse-go-rewrite/internal/unifiedresources"
	"github.com/rcourtman/pulse-go-rewrite/pkg/metrics"
)

// getAllCountingStore wraps a real *unifiedresources.MonitorAdapter and
// counts GetAll() calls, otherwise behaving identically — embedding the
// concrete adapter type (not the ResourceStoreInterface interface) so every
// optional interface it implements (AtomicSnapshotResourceStore,
// MetricsTargetResourceStore, StaleThresholdResourceStore, ...) still type-
// asserts correctly through the wrapper. See #1966: an earlier attempt at
// this fix wrapped the interface instead of the concrete type and broke
// exactly these type assertions silently.
type getAllCountingStore struct {
	*unifiedresources.MonitorAdapter
	getAllCalls int
}

func (s *getAllCountingStore) GetAll() []unifiedresources.Resource {
	s.getAllCalls++
	return s.MonitorAdapter.GetAll()
}

// TestUpdateResourceStoreCallsGetAllOnce reproduces #1966's CPU finding:
// updateResourceStore was calling store.GetAll() six separate times per
// invocation (once from each of the five metrics sync passes, once more for
// alert evaluation) — GetAll() deep-clones and re-sorts every tracked
// resource, so a single accepted agent report paid for six full-fleet
// clone+sort passes instead of one. Confirms the fix: exactly one call now.
func TestUpdateResourceStoreCallsGetAllOnce(t *testing.T) {
	store := &getAllCountingStore{MonitorAdapter: unifiedresources.NewMonitorAdapter(nil)}

	metricsStore, err := metrics.NewStore(metrics.DefaultConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("metrics.NewStore: %v", err)
	}
	defer metricsStore.Close()

	monitor := &Monitor{
		resourceStore:  store,
		metricsHistory: NewMetricsHistory(1024, 24*time.Hour),
		metricsStore:   metricsStore,
	}

	state := models.StateSnapshot{
		Hosts: []models.Host{{
			ID:       "host-1",
			Hostname: "test-host",
			CPUUsage: 12.5,
		}},
	}

	monitor.updateResourceStore(state)

	if store.getAllCalls != 1 {
		t.Fatalf("GetAll() called %d times for one updateResourceStore pass, want 1 (was 6 before #1966's fix: five metrics sync passes + alert evaluation each independently rebuilding the full resource list)", store.getAllCalls)
	}
}

// TestUpdateResourceStoreForReadCallsGetAllOnce is the read-path counterpart
// — same six-call pattern existed in updateResourceStoreForRead.
func TestUpdateResourceStoreForReadCallsGetAllOnce(t *testing.T) {
	store := &getAllCountingStore{MonitorAdapter: unifiedresources.NewMonitorAdapter(nil)}

	metricsStore, err := metrics.NewStore(metrics.DefaultConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("metrics.NewStore: %v", err)
	}
	defer metricsStore.Close()

	monitor := &Monitor{
		resourceStore:  store,
		metricsHistory: NewMetricsHistory(1024, 24*time.Hour),
		metricsStore:   metricsStore,
	}

	state := models.StateSnapshot{
		Hosts: []models.Host{{
			ID:       "host-1",
			Hostname: "test-host",
			CPUUsage: 12.5,
		}},
	}

	// Prime the registry once so TryReplaceRegistryForRead below actually
	// finds a snapshot to work with rather than a no-op empty rebuild.
	monitor.updateResourceStore(state)
	store.getAllCalls = 0

	monitor.updateResourceStoreForRead(state)

	if store.getAllCalls > 1 {
		t.Fatalf("GetAll() called %d times for one updateResourceStoreForRead pass, want at most 1", store.getAllCalls)
	}
}
