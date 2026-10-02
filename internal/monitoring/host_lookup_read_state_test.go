package monitoring

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/rcourtman/pulse-go-rewrite/internal/config"
	"github.com/rcourtman/pulse-go-rewrite/internal/models"
	"github.com/rcourtman/pulse-go-rewrite/internal/unifiedresources"
)

// hostLookupFixtureState is an estate where Kubernetes metadata objects
// outnumber the machines, as on a real k3s homelab: host and Docker agents,
// one VM, and a cluster carrying nodes plus secrets, RBAC, events and
// replica sets.
func hostLookupFixtureState(now time.Time) *models.State {
	state := models.NewState()
	used := now.Add(-time.Minute)
	for i, name := range []string{"pve-a", "k3s-1"} {
		state.UpsertHost(models.Host{
			ID:              fmt.Sprintf("host-%d", i),
			Hostname:        name,
			MachineID:       fmt.Sprintf("machine-%d", i),
			Status:          "online",
			TokenID:         fmt.Sprintf("token-%d", i),
			TokenName:       "install",
			TokenHint:       "pt_…abcd",
			TokenLastUsedAt: &used,
			LastSeen:        now,
		})
	}
	state.UpsertDockerHost(models.DockerHost{
		ID:       "docker-1",
		AgentID:  "docker-agent-1",
		Hostname: "k3s-1",
		Status:   "online",
		LastSeen: now,
	})
	state.UpdateVMs([]models.VM{{
		ID: "lab:pve-a:101", Name: "db", Node: "pve-a", Instance: "lab",
		VMID: 101, Status: "running", Type: "qemu", LastSeen: now,
	}})

	cluster := models.KubernetesCluster{
		ID: "k3s", AgentID: "k3s-agent", Name: "k3s", Status: "online", LastSeen: now,
		Nodes: []models.KubernetesNode{{UID: "node-uid-1", Name: "k3s-1"}},
	}
	for i := 0; i < 20; i++ {
		cluster.Secrets = append(cluster.Secrets, models.KubernetesSecret{UID: fmt.Sprintf("secret-%d", i), Name: fmt.Sprintf("s%d", i), Namespace: "apps"})
		cluster.ClusterRoles = append(cluster.ClusterRoles, models.KubernetesClusterRole{UID: fmt.Sprintf("role-%d", i), Name: fmt.Sprintf("r%d", i)})
		cluster.Events = append(cluster.Events, models.KubernetesEvent{UID: fmt.Sprintf("event-%d", i), Name: fmt.Sprintf("e%d", i), Namespace: "apps"})
		cluster.ReplicaSets = append(cluster.ReplicaSets, models.KubernetesReplicaSet{UID: fmt.Sprintf("rs-%d", i), Name: fmt.Sprintf("rs%d", i), Namespace: "apps"})
	}
	state.UpsertKubernetesCluster(cluster)
	return state
}

// The per-report read state exists to resolve host and Docker-host identity.
// It must give the same answers as a registry built from the full snapshot,
// without ingesting Kubernetes metadata objects that can never be hosts.
func TestHostLookupReadStateMatchesFullSnapshotWithoutKubernetesMetadata(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	state := hostLookupFixtureState(now)
	monitor := &Monitor{config: &config.Config{}, state: state}

	full := unifiedresources.NewRegistry(nil)
	full.IngestSnapshotWithStaleThresholds(state.GetSnapshot(), monitor.resourceStaleThresholds())
	want := unifiedresources.NewMonitorAdapterWithStaleThresholds(full, monitor.resourceStaleThresholds())

	got := monitor.snapshotBackedUnifiedReadState()
	if got == nil {
		t.Fatal("expected a host lookup read state")
	}

	if g, w := hostLookupFields(got.Hosts()), hostLookupFields(want.Hosts()); !reflect.DeepEqual(g, w) {
		t.Fatalf("Hosts() differ from full snapshot:\n got  %v\n want %v", g, w)
	}
	if len(got.Hosts()) != 2 {
		t.Fatalf("Hosts() = %d, want 2", len(got.Hosts()))
	}
	if g, w := dockerHostLookupFields(got.DockerHosts()), dockerHostLookupFields(want.DockerHosts()); !reflect.DeepEqual(g, w) {
		t.Fatalf("DockerHosts() differ from full snapshot:\n got  %v\n want %v", g, w)
	}

	lister, ok := got.(interface {
		GetAll() []unifiedresources.Resource
	})
	if !ok {
		t.Fatalf("read state %T cannot list resources", got)
	}
	for _, r := range lister.GetAll() {
		switch r.Type {
		case unifiedresources.ResourceTypeK8sSecret, unifiedresources.ResourceTypeK8sClusterRole,
			unifiedresources.ResourceTypeK8sEvent, unifiedresources.ResourceTypeK8sReplicaSet:
			t.Fatalf("host lookup read state ingested Kubernetes metadata %s %q", r.Type, r.ID)
		}
	}
}

func hostLookupFields(hosts []*unifiedresources.HostView) []string {
	var out []string
	for _, h := range hosts {
		used := ""
		if t := h.TokenLastUsedAt(); t != nil {
			used = t.UTC().Format(time.RFC3339Nano)
		}
		out = append(out, fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s", h.ID(), h.AgentID(), h.Hostname(), h.MachineID(), h.TokenID(), h.TokenName(), used))
	}
	return out
}

func dockerHostLookupFields(hosts []*unifiedresources.DockerHostView) []string {
	var out []string
	for _, h := range hosts {
		out = append(out, fmt.Sprintf("%s|%s|%s|%s", h.ID(), h.AgentID(), h.Hostname(), h.TokenID()))
	}
	return out
}
