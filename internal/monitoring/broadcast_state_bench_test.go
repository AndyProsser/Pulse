package monitoring

// Isolates the three once-per-broadcast stages of BuildFrontendState on a
// synthetic ~1,200-resource estate (#2199): registry listing, connected
// infrastructure (top-level system grouping) and conversion to the frontend
// shape. Not an installed-fleet or end-to-end CPU benchmark.

import (
	"fmt"
	"testing"
	"time"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
	"github.com/rcourtman/pulse-go-rewrite/internal/unifiedresources"
)

func broadcastBenchState(n int) models.StateSnapshot {
	now := time.Now().UTC()
	s := models.StateSnapshot{VMs: make([]models.VM, n), LastUpdate: now}
	for i := range s.VMs {
		s.VMs[i] = models.VM{ID: fmt.Sprintf("lab:node-%d:%d", i%4, i+100), VMID: i + 100, Name: fmt.Sprintf("vm-%d", i),
			Node: fmt.Sprintf("node-%d", i%4), Instance: "lab", Status: "running", CPUs: 4, CPU: 0.25, LastSeen: now}
	}
	return s
}

func broadcastBenchResources(b *testing.B, n int) ([]unifiedresources.Resource, models.StateSnapshot) {
	snap := broadcastBenchState(n)
	reg := unifiedresources.NewRegistry(nil)
	reg.IngestSnapshot(snap)
	return reg.List(), snap
}

func BenchmarkBroadcastRegistryList1200(b *testing.B) {
	snap := broadcastBenchState(1200)
	reg := unifiedresources.NewRegistry(nil)
	reg.IngestSnapshot(snap)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reg.List()
	}
}

func BenchmarkBroadcastConnectedInfrastructure1200(b *testing.B) {
	res, snap := broadcastBenchResources(b, 1200)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildConnectedInfrastructure(res, snap)
	}
}

func BenchmarkBroadcastConvertResources1200(b *testing.B) {
	res, _ := broadcastBenchResources(b, 1200)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		convertResourcesForBroadcast(res, nil)
	}
}
