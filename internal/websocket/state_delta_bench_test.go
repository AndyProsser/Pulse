package websocket

import (
	"fmt"
	"testing"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
)

// BenchmarkBuildClientStateSnapshot measures the cost of turning one current-
// state broadcast into a client delta baseline: marshal the full state,
// split it back into per-field RawMessages, and key each resource/
// infrastructure/alert entry by id (extractKeyedEntries).
//
// This is a fixed comparison point across main and this branch: the call
// signature (buildClientStateSnapshot(state interface{})) is unchanged by
// the CPU-2199 fixes, only its internal cost is. On main, every entry's id
// is re-derived by unmarshaling that entry a second time even though
// encoding/json already produced it in the same order as the source slice.
// On this branch, extractKeyedEntries is handed those ids directly
// (knownEntryIDs) and only verifies the first entry before trusting the
// rest, skipping the redundant per-entry decode.
//
// Reproduce on a clean checkout:
//
//	go test ./internal/websocket/... -run '^$' -bench BenchmarkBuildClientStateSnapshot -benchmem -count 5
//
// Run once on upstream/main and once on this branch to compare ns/op and
// allocs/op directly.
func BenchmarkBuildClientStateSnapshot(b *testing.B) {
	for _, count := range []int{100, 1000, 2000} {
		b.Run(fmt.Sprintf("resources=%d", count), func(b *testing.B) {
			state := models.EmptyStateFrontend()
			state.LastUpdate = 100
			state.Resources = make([]models.ResourceFrontend, count)
			for i := 0; i < count; i++ {
				state.Resources[i] = models.ResourceFrontend{
					ID:          fmt.Sprintf("agent:host-%d", i),
					Type:        "agent",
					Name:        fmt.Sprintf("host-%d", i),
					DisplayName: fmt.Sprintf("host-%d", i),
					Status:      "online",
					CPU:         &models.ResourceMetricFrontend{Current: float64(i % 100)},
					Memory:      &models.ResourceMetricFrontend{Current: float64((i * 7) % 100)},
				}
			}
			state.ConnectedInfrastructure = make([]models.ConnectedInfrastructureItemFrontend, count/10+1)
			for i := range state.ConnectedInfrastructure {
				state.ConnectedInfrastructure[i] = models.ConnectedInfrastructureItemFrontend{
					ID:   fmt.Sprintf("infra:%d", i),
					Name: fmt.Sprintf("infra-%d", i),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Vary one field so every pass is a real encode, not a
				// cached/degenerate no-op - matches a report changing one
				// resource's metrics on an otherwise steady fleet.
				state.Resources[0].CPU = &models.ResourceMetricFrontend{Current: float64(i % 100)}
				if _, err := buildClientStateSnapshot(state); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
