package unifiedresources

import (
	"slices"
	"testing"
)

// Tags built from labels feed the state broadcast and its per-client diffs, so
// the same labels must always produce the same slice. Go randomises map
// iteration, so an unsorted conversion shows up as a spurious tag change on
// every rebuild.
func TestLabelsToTagsIsSortedAndStable(t *testing.T) {
	labels := map[string]string{
		"pod-template-hash":                     "69999f76",
		"app":                                   "5etools",
		"kubernetes.io/metadata.name":           "5etools",
		"kustomize.toolkit.fluxcd.io/name":      "apps",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
		"app.kubernetes.io/part-of":             "homelab",
		"tier":                                  "",
		"zone":                                  "a",
	}
	want := []string{
		"app.kubernetes.io/part-of:homelab",
		"app:5etools",
		"kubernetes.io/metadata.name:5etools",
		"kustomize.toolkit.fluxcd.io/name:apps",
		"kustomize.toolkit.fluxcd.io/namespace:flux-system",
		"pod-template-hash:69999f76",
		"tier",
		"zone:a",
	}
	for i := 0; i < 50; i++ {
		if got := labelsToTags(labels); !slices.Equal(got, want) {
			t.Fatalf("run %d: labelsToTags() = %v, want %v", i, got, want)
		}
	}
}
