package alerts

import (
	"testing"
	"time"

	"github.com/rcourtman/pulse-go-rewrite/internal/storagehealth"
	"github.com/rcourtman/pulse-go-rewrite/internal/unifiedresources"
)

// Reproduces #2221: a provider-incident on a resource under operator
// suppression (retired/muted/maintenance) must not re-dispatch a
// notification on every sync cycle. setActiveAlertNoLock deliberately drops
// suppressed alerts rather than tracking them as active (see
// ReconcileResourceOperatorState's doc comment), so the "already active,
// don't re-fire" check in SyncUnifiedResourceIncidents can never succeed for
// them unless the dispatch loop itself also respects that suppression.
//
// Live impact: a single unresolved TrueNAS incident on a resource marked
// "retired" produced 1,213 duplicate notification emails over 6.5 hours —
// once per accepted agent report — before this fix.
func TestSyncUnifiedResourceIncidentsDoesNotRepeatDispatchWhileSuppressed(t *testing.T) {
	m := newTestManager(t)
	configureUnifiedEvalManager(t, m, unifiedEvalBaseConfig())

	m.SetOperatorIntentContextResolver(func(resourceID string, now time.Time) (OperatorIntentContext, bool) {
		return OperatorIntentContext{LifecycleState: "retired"}, true
	})

	var dispatched []Alert
	m.SetAlertCallback(func(alert *Alert) { dispatched = append(dispatched, *alert) })

	resource := unifiedresources.Resource{
		ID:         "storage:tank",
		Type:       unifiedresources.ResourceTypeStorage,
		Name:       "tank",
		ParentName: "truenas-main",
		Sources:    []unifiedresources.DataSource{unifiedresources.SourceTrueNAS},
		Storage: &unifiedresources.StorageMeta{
			Platform:   "truenas",
			Topology:   "pool",
			Protection: "zfs",
			IsZFS:      true,
		},
		Incidents: []unifiedresources.ResourceIncident{{
			Provider: "truenas",
			NativeID: "alert-1",
			Code:     "truenas_volume_status",
			Severity: storagehealth.RiskWarning,
			Summary:  "Pool tank is DEGRADED",
		}},
	}

	// Simulate many accepted-report sync cycles against the same
	// still-unresolved, still-suppressed condition.
	for i := 0; i < 20; i++ {
		m.SyncUnifiedResourceIncidents([]unifiedresources.Resource{resource})
	}

	if len(dispatched) != 0 {
		t.Fatalf("dispatched = %d notifications across 20 cycles while suppressed, want 0", len(dispatched))
	}
	if len(m.GetActiveAlerts()) != 0 {
		t.Fatalf("active alerts = %d while suppressed, want 0", len(m.GetActiveAlerts()))
	}
}

// Confirms the fix doesn't over-suppress: once the resource is no longer
// under operator suppression, the same still-firing incident dispatches
// exactly once and then stops re-firing on repeated cycles, matching
// ordinary (non-suppressed) incident behavior.
func TestSyncUnifiedResourceIncidentsDispatchesOnceAfterSuppressionLifted(t *testing.T) {
	m := newTestManager(t)
	configureUnifiedEvalManager(t, m, unifiedEvalBaseConfig())

	suppressed := true
	m.SetOperatorIntentContextResolver(func(resourceID string, now time.Time) (OperatorIntentContext, bool) {
		if suppressed {
			return OperatorIntentContext{LifecycleState: "retired"}, true
		}
		return OperatorIntentContext{LifecycleState: "active"}, true
	})

	var dispatched []Alert
	m.SetAlertCallback(func(alert *Alert) { dispatched = append(dispatched, *alert) })

	resource := unifiedresources.Resource{
		ID:         "storage:tank",
		Type:       unifiedresources.ResourceTypeStorage,
		Name:       "tank",
		ParentName: "truenas-main",
		Sources:    []unifiedresources.DataSource{unifiedresources.SourceTrueNAS},
		Storage: &unifiedresources.StorageMeta{
			Platform:   "truenas",
			Topology:   "pool",
			Protection: "zfs",
			IsZFS:      true,
		},
		Incidents: []unifiedresources.ResourceIncident{{
			Provider: "truenas",
			NativeID: "alert-1",
			Code:     "truenas_volume_status",
			Severity: storagehealth.RiskWarning,
			Summary:  "Pool tank is DEGRADED",
		}},
	}

	m.SyncUnifiedResourceIncidents([]unifiedresources.Resource{resource})
	m.SyncUnifiedResourceIncidents([]unifiedresources.Resource{resource})
	if len(dispatched) != 0 {
		t.Fatalf("dispatched = %d while suppressed, want 0", len(dispatched))
	}

	suppressed = false
	for i := 0; i < 5; i++ {
		m.SyncUnifiedResourceIncidents([]unifiedresources.Resource{resource})
	}

	if len(dispatched) != 1 {
		t.Fatalf("dispatched = %d notifications after suppression lifted across 5 cycles, want 1", len(dispatched))
	}
	if len(m.GetActiveAlerts()) != 1 {
		t.Fatalf("active alerts = %d after suppression lifted, want 1", len(m.GetActiveAlerts()))
	}
}
