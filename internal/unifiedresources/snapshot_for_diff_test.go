package unifiedresources

import (
	"testing"
	"time"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
)

// TestReplaceRegistryEmitsChangesViaSnapshotForDiff is the regression test
// for Fix C: replaceRegistryLocked's before/after diff inputs switched from
// List() (deep clone) to snapshotForDiff() (shallow copy). If that shallow
// copy ever ended up aliasing the same underlying data for "before" and
// "after" - the exact failure mode a broken version of this optimization
// would produce - every diff would silently look like "nothing changed" and
// the audit trail would go dark. This proves add, update, and remove are
// all still detected correctly end-to-end through the real MonitorAdapter
// path (PopulateFromSnapshot -> replaceRegistry -> replaceRegistryLocked),
// not just at the buildResourceChange unit level (which never exercises
// snapshotForDiff at all).
func TestReplaceRegistryEmitsChangesViaSnapshotForDiff(t *testing.T) {
	store := NewMemoryStore()
	adapter := NewMonitorAdapter(NewRegistry(store))

	// Initial population: host-1 online, host-2 online.
	adapter.PopulateFromSnapshot(models.StateSnapshot{
		Hosts: []models.Host{
			{ID: "host-1", Hostname: "alpha", Status: "online"},
			{ID: "host-2", Hostname: "beta", Status: "online"},
		},
	})

	initial, err := store.GetRecentChanges("", time.Time{}, 100)
	if err != nil {
		t.Fatalf("GetRecentChanges after initial populate: %v", err)
	}
	if got := len(initial); got != 2 {
		t.Fatalf("initial populate recorded %d changes, want 2 (one per discovered host)", got)
	}
	// Resource IDs are content-hashed, not the caller-supplied Host.ID, so
	// capture them from what was actually recorded rather than assuming a
	// literal "host-1"/"host-2" round-trip.
	initialIDs := make(map[string]bool, 2)
	initialChangeIDs := make(map[string]bool, 2)
	for _, c := range initial {
		if c.Kind != ChangeStateTransition || c.From != "absent" || c.To != "online" {
			t.Fatalf("expected an 'online' discovery change, got %+v", c)
		}
		initialIDs[c.ResourceID] = true
		initialChangeIDs[c.ID] = true
	}
	if len(initialIDs) != 2 {
		t.Fatalf("initial changes referenced %d distinct resource ids, want 2: %+v", len(initialIDs), initial)
	}

	// Second population: one initial host goes offline (update), the other
	// is gone (removed), and a brand new host appears (created). Exercises
	// all three diff outcomes in one pass.
	adapter.PopulateFromSnapshot(models.StateSnapshot{
		Hosts: []models.Host{
			{ID: "host-1", Hostname: "alpha", Status: "offline"},
			{ID: "host-3", Hostname: "gamma", Status: "online"},
		},
	})

	all, err := store.GetRecentChanges("", time.Time{}, 100)
	if err != nil {
		t.Fatalf("GetRecentChanges after second populate: %v", err)
	}
	if got := len(all); got != 5 {
		t.Fatalf("total changes recorded = %d, want 5 (2 discovered + 1 updated + 1 removed + 1 discovered)", got)
	}

	// GetRecentChanges sorts most-recent-first, so index slicing isn't a
	// reliable way to isolate the second populate's changes - filter by
	// change ID instead of assuming an order.
	var sawUpdate, sawRemoval, sawNewCreation bool
	for _, c := range all {
		if initialChangeIDs[c.ID] {
			continue
		}
		switch {
		case initialIDs[c.ResourceID] && c.Kind == ChangeStateTransition && c.From == "online" && c.To == "offline":
			sawUpdate = true
		case initialIDs[c.ResourceID] && c.Kind == ChangeStateTransition && c.From == "online" && c.To == "absent":
			sawRemoval = true
		case !initialIDs[c.ResourceID] && c.Kind == ChangeStateTransition && c.From == "absent" && c.To == "online":
			sawNewCreation = true
		default:
			t.Errorf("unexpected change on second populate: %+v", c)
		}
	}

	if !sawUpdate {
		t.Error("did not observe the surviving host's online->offline state transition")
	}
	if !sawRemoval {
		t.Error("did not observe the dropped host's removal")
	}
	if !sawNewCreation {
		t.Error("did not observe the new host's creation")
	}
}

// TestSnapshotForDiffReturnsIndependentCopies confirms snapshotForDiff's
// core safety property directly: two snapshots taken before and after an
// ingest must be independent - mutating the registry after the first
// snapshot must never retroactively change what the first snapshot reports.
func TestSnapshotForDiffReturnsIndependentCopies(t *testing.T) {
	rr := NewRegistry(nil)
	rr.IngestResources([]Resource{{ID: "vm-1", Type: ResourceTypeVM, Name: "alpha", Status: StatusOnline}})

	before := rr.snapshotForDiff()
	if len(before) != 1 || before[0].Status != StatusOnline {
		t.Fatalf("before snapshot = %+v, want one online vm-1", before)
	}

	rr.IngestResources([]Resource{{ID: "vm-1", Type: ResourceTypeVM, Name: "alpha", Status: StatusOffline}})

	// The already-taken "before" snapshot must still show the old status -
	// if snapshotForDiff aliased live registry state instead of copying it,
	// this would flip to offline too.
	if before[0].Status != StatusOnline {
		t.Fatalf("before snapshot mutated after a later ingest: status = %v, want unchanged online", before[0].Status)
	}

	after := rr.snapshotForDiff()
	if len(after) != 1 || after[0].Status != StatusOffline {
		t.Fatalf("after snapshot = %+v, want one offline vm-1", after)
	}
}
