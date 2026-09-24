package unifiedresources

import (
	"fmt"
	"testing"
	"time"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
)

// BenchmarkPopulateFromSnapshot_AcceptedReport measures the cost of one
// accepted-report registry rebuild: MonitorAdapter.PopulateFromSnapshot ->
// replaceRegistry -> replaceRegistryLocked. Every accepted agent/Docker/
// Kubernetes report calls this, discarding and rebuilding the canonical
// registry from the full state snapshot rather than updating just the one
// resource that changed (see issue #2199).
//
// This is a fixed comparison point across main and this branch: the call
// signature (PopulateFromSnapshot(models.StateSnapshot)) is unchanged by the
// CPU-2199 fixes, only its internal cost is. On main, replaceRegistryLocked
// takes a full List() (deep clone, ~20 nested sub-clones per resource) of
// both the "before" and "after" registry state purely to feed the
// audit-trail diff. On this branch it uses snapshotForDiff() (a single
// shallow struct copy) for the same comparison. withViewCache also switches
// from an unconditional exclusive lock to a read-lock fast path on this
// branch (b4cb200e1), and concurrent same-cycle rebuild triggers coalesce
// instead of running redundantly (e99d64536) - this single-goroutine
// benchmark doesn't exercise that second effect, so treat its numbers as
// the deep-clone-diff mechanism specifically, not the full multi-mechanism
// live-fleet reduction reported in the issue.
//
// Reproduce on a clean checkout:
//
//	go test ./internal/unifiedresources/... -run '^$' -bench BenchmarkPopulateFromSnapshot_AcceptedReport -benchmem -count 5
//
// Run once on upstream/main and once on this branch to compare ns/op and
// allocs/op directly.
func BenchmarkPopulateFromSnapshot_AcceptedReport(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprintf("resources=%d", count), func(b *testing.B) {
			store := NewMemoryStore()
			adapter := NewMonitorAdapter(NewRegistry(store))

			snapshot := syntheticFleetSnapshot(count)
			adapter.PopulateFromSnapshot(snapshot)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Vary one host's CPU reading so every pass is a real
				// ingest + diff, not a no-op - matches one agent's report
				// changing its own metrics on an otherwise steady fleet.
				snapshot.Hosts[0].CPUUsage = float64(i % 100)
				adapter.PopulateFromSnapshot(snapshot)
			}
		})
	}
}

// syntheticFleetSnapshot builds a StateSnapshot mixing agent-reported hosts
// (unified agents, TrueNAS-style) with Proxmox-reported VMs and containers,
// mirroring the mixed fleet composition described in issue #2199.
func syntheticFleetSnapshot(total int) models.StateSnapshot {
	hostCount := total / 5
	if hostCount == 0 {
		hostCount = 1
	}
	remaining := total - hostCount
	vmCount := remaining / 2
	containerCount := remaining - vmCount

	now := time.Now().UTC()
	snapshot := models.EmptyStateSnapshot()

	snapshot.Hosts = make([]models.Host, hostCount)
	for i := range snapshot.Hosts {
		snapshot.Hosts[i] = models.Host{
			ID:        fmt.Sprintf("agent-%d", i),
			Hostname:  fmt.Sprintf("agent-host-%d", i),
			Platform:  "linux",
			CPUCount:  8,
			CPUUsage:  float64(i % 100),
			Memory:    models.Memory{Total: 16 << 30, Used: 8 << 30},
			LastSeen:  now,
		}
	}

	snapshot.VMs = make([]models.VM, vmCount)
	for i := range snapshot.VMs {
		snapshot.VMs[i] = models.VM{
			ID:       fmt.Sprintf("cluster-a:pve-%d:%d", i%10, 100+i),
			Name:     fmt.Sprintf("vm-%d", i),
			Node:     fmt.Sprintf("pve-%d", i%10),
			Instance: "cluster-a",
			VMID:     100 + i,
			Status:   "running",
			Type:     "qemu",
			CPU:      float64(i%100) / 100,
			Memory:   models.Memory{Total: 4 << 30, Used: 2 << 30},
		}
	}

	snapshot.Containers = make([]models.Container, containerCount)
	for i := range snapshot.Containers {
		snapshot.Containers[i] = models.Container{
			ID:       fmt.Sprintf("cluster-a:pve-%d:%d", i%10, 200+i),
			Name:     fmt.Sprintf("ct-%d", i),
			Node:     fmt.Sprintf("pve-%d", i%10),
			Instance: "cluster-a",
			VMID:     200 + i,
			Status:   "running",
			Type:     "lxc",
			CPU:      float64(i%100) / 100,
			Memory:   models.Memory{Total: 2 << 30, Used: 1 << 30},
		}
	}

	return snapshot
}
