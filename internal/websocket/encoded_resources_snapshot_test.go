package websocket

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
)

func encodedResourcesFixture(t *testing.T) models.StateFrontend {
	t.Helper()
	cpu := 42.5
	state := models.StateFrontend{
		LastUpdate: 1790926159000,
		Resources: []models.ResourceFrontend{
			{ID: "vm-1", Type: "vm", Name: "db <primary> & co", Tags: []string{"a", "b"}, CPU: &models.ResourceMetricFrontend{Current: cpu}},
			{ID: "agent-2", Type: "agent", Name: "pve-a", Labels: map[string]string{"zone": "a", "rack": "1"}},
			{ID: "k8s-secret-3", Type: "k8s-secret", Name: "ünïcode   name"},
		},
		ConnectedInfrastructure: []models.ConnectedInfrastructureItemFrontend{{ID: "ci-1", Name: "pve-a"}},
		CapabilityCatalog:       map[string]json.RawMessage{"cap1": json.RawMessage(`{"x":1}`)},
	}
	for _, resource := range state.Resources {
		encoded, err := json.Marshal(resource)
		if err != nil {
			t.Fatalf("marshal %s: %v", resource.ID, err)
		}
		key, ok := ResourceEntryKey(encoded)
		if !ok {
			t.Fatalf("no entry key for %s", resource.ID)
		}
		state.EncodedResources = append(state.EncodedResources, models.EncodedResource{ID: key, JSON: encoded})
	}
	return state
}

// A snapshot built from cached per-resource encodings must equal the one
// built by encoding the whole state.
func TestBuildClientStateSnapshotFromEncodedResourcesMatchesFullEncode(t *testing.T) {
	state := encodedResourcesFixture(t)
	plain := state
	plain.EncodedResources = nil

	want, err := buildClientStateSnapshot(plain)
	if err != nil {
		t.Fatalf("full encode: %v", err)
	}
	got, err := buildClientStateSnapshot(state)
	if err != nil {
		t.Fatalf("encoded resources: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot from encoded resources differs:\n got  %+v\n want %+v", got, want)
	}
	if !snapshotUsedEncodedResources(state) {
		t.Fatal("consistent encoded resources were not used")
	}
}

// Encodings that do not line up with Resources are ignored.
func TestBuildClientStateSnapshotIgnoresInconsistentEncodedResources(t *testing.T) {
	state := encodedResourcesFixture(t)
	plain := state
	plain.EncodedResources = nil
	want, err := buildClientStateSnapshot(plain)
	if err != nil {
		t.Fatalf("full encode: %v", err)
	}

	short := state
	short.EncodedResources = state.EncodedResources[:2]
	duplicate := state
	duplicate.EncodedResources = append([]models.EncodedResource(nil), state.EncodedResources...)
	duplicate.EncodedResources[2].ID = duplicate.EncodedResources[0].ID

	for name, broken := range map[string]models.StateFrontend{"short": short, "duplicate id": duplicate} {
		if snapshotUsedEncodedResources(broken) {
			t.Fatalf("%s: inconsistent encodings were used", name)
		}
		got, err := buildClientStateSnapshot(broken)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: fallback snapshot differs from full encode", name)
		}
	}
}

func snapshotUsedEncodedResources(state models.StateFrontend) bool {
	_, ok := consistentEncodedResources(state)
	return ok
}
