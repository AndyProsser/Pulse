package unifiedresources_test

import (
	"testing"
	"time"

	"github.com/rcourtman/pulse-go-rewrite/internal/mock"
	"github.com/rcourtman/pulse-go-rewrite/internal/unifiedresources"
)

// Registry reads copy stored resources without recomputing canonical
// metadata, so the stored resources must already hold exactly what a full
// refresh would produce. Check that against the demo fixture, which covers
// every provider, before and after a second ingest mutates the registry.
func TestRegistryReadCopiesMatchFullyRefreshedClones(t *testing.T) {
	if err := mock.SetEnabled(true); err != nil {
		t.Fatalf("enable mock: %v", err)
	}
	t.Cleanup(func() { _ = mock.SetEnabled(false) })

	graph := mock.CurrentFixtureGraph()
	registry := unifiedresources.NewRegistry(nil)
	ingest := func() {
		registry.IngestSnapshot(unifiedresources.SnapshotWithoutSources(graph.State, mock.SupplementalOwnedSources()))
		for _, source := range mock.SupplementalOwnedSources() {
			if records := graph.SupplementalRecords(source); len(records) > 0 {
				registry.IngestRecords(source, records)
			}
		}
	}

	ingest()
	if got := len(registry.List()); got < 100 {
		t.Fatalf("fixture produced %d resources, want a realistic estate (>=100)", got)
	}
	if drift := unifiedresources.RegistryCloneDriftForTest(registry); len(drift) > 0 {
		t.Fatalf("%d resources drift after first ingest, e.g. %v", len(drift), drift[:min(5, len(drift))])
	}

	// A later ingest with changed data must leave the stored copies refreshed too.
	graph.State.LastUpdate = graph.State.LastUpdate.Add(time.Minute)
	for i := range graph.State.VMs {
		graph.State.VMs[i].CPU += 0.01
		graph.State.VMs[i].Tags = append(graph.State.VMs[i].Tags, "drift-check")
	}
	ingest()
	if drift := unifiedresources.RegistryCloneDriftForTest(registry); len(drift) > 0 {
		t.Fatalf("%d resources drift after re-ingest, e.g. %v", len(drift), drift[:min(5, len(drift))])
	}
}
