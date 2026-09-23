package unifiedresources

import (
	"sync"
	"testing"
)

// TestWithViewCacheConcurrentReadersDoNotRace exercises the fast path added
// to withViewCache: many goroutines calling view accessors (VMs, Nodes,
// Hosts) concurrently, while views are already fresh, must not race or
// deadlock. Previously every call unconditionally took the exclusive write
// lock just to check freshness, serializing all readers even when nothing
// needed rebuilding; this only verifies correctness/safety (run with
// -race), not the serialization itself, which isn't practical to assert
// reliably from a lock-timing test.
func TestWithViewCacheConcurrentReadersDoNotRace(t *testing.T) {
	rr := NewRegistry(nil)
	rr.IngestResources([]Resource{
		{ID: "vm-1", Type: ResourceTypeVM, Name: "alpha"},
		{ID: "vm-2", Type: ResourceTypeVM, Name: "beta"},
		{ID: "agent-1", Type: ResourceTypeAgent, Name: "host-a", Agent: &AgentData{AgentID: "m-1", Hostname: "host-a"}},
	})

	// Prime the view cache once so most readers below hit the fast (clean)
	// path rather than every one racing to be the rebuilder.
	if got := len(rr.VMs()); got != 2 {
		t.Fatalf("VMs() = %d, want 2", got)
	}

	const readers = 50
	var wg sync.WaitGroup
	wg.Add(readers * 3)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			if got := len(rr.VMs()); got != 2 {
				t.Errorf("concurrent VMs() = %d, want 2", got)
			}
		}()
		go func() {
			defer wg.Done()
			_ = rr.Nodes()
		}()
		go func() {
			defer wg.Done()
			if got := len(rr.Hosts()); got != 1 {
				t.Errorf("concurrent Hosts() = %d, want 1", got)
			}
		}()
	}
	wg.Wait()
}

// TestWithViewCacheRebuildsAfterIngest confirms the fast-path addition
// didn't break the actual invalidation behavior: a write that dirties the
// views must still be visible to the next read, including when readers and
// a writer race.
func TestWithViewCacheRebuildsAfterIngest(t *testing.T) {
	rr := NewRegistry(nil)
	rr.IngestResources([]Resource{{ID: "vm-1", Type: ResourceTypeVM, Name: "alpha"}})
	if got := len(rr.VMs()); got != 1 {
		t.Fatalf("VMs() = %d, want 1 before second ingest", got)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		rr.IngestResources([]Resource{{ID: "vm-2", Type: ResourceTypeVM, Name: "beta"}})
	}()
	go func() {
		defer wg.Done()
		_ = rr.VMs()
	}()
	wg.Wait()

	if got := len(rr.VMs()); got != 2 {
		t.Fatalf("VMs() = %d after second ingest, want 2", got)
	}
}
