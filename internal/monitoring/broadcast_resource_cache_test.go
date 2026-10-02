package monitoring

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/rcourtman/pulse-go-rewrite/internal/mock"
	"github.com/rcourtman/pulse-go-rewrite/internal/unifiedresources"
)

// The broadcast resource cache must be invisible: for any sequence of
// estates, the cached conversion returns exactly what the uncached one does,
// and each encoding is exactly json.Marshal of its resource. Only resources
// whose content changed may miss.
func TestCachedBroadcastConversionMatchesUncached(t *testing.T) {
	if err := mock.SetEnabled(true); err != nil {
		t.Fatalf("enable mock: %v", err)
	}
	t.Cleanup(func() { _ = mock.SetEnabled(false) })
	// Fixture snapshots are not reproducible call to call, so both paths
	// convert the same slice; conversion must leave its input untouched.
	resources, _ := mock.CurrentFixtureGraph().UnifiedResourceSnapshot()
	if len(resources) < 100 {
		t.Fatalf("fixture has %d resources, want a realistic estate", len(resources))
	}
	fingerprint := func() []resourceContentKey {
		var hasher resourceContentHasher
		keys := make([]resourceContentKey, len(resources))
		for i := range resources {
			keys[i], _ = hasher.key(&resources[i])
		}
		return keys
	}

	cache := newBroadcastResourceCache()
	check := func(round string) (hits, misses int) {
		t.Helper()
		before := fingerprint()
		want, wantCatalogs := convertResourcesForBroadcast(resources)
		stats := cache.stats()
		got, encoded, gotCatalogs := convertResourcesForBroadcastCached(cache, resources)
		after := cache.stats()
		if !reflect.DeepEqual(fingerprint(), before) {
			t.Fatalf("%s: conversion modified its input resources", round)
		}

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: cached resources differ from uncached conversion", round)
		}
		if !reflect.DeepEqual(gotCatalogs, wantCatalogs) {
			t.Fatalf("%s: cached catalogs differ from uncached conversion", round)
		}
		if len(encoded) != len(want) {
			t.Fatalf("%s: %d encodings for %d resources", round, len(encoded), len(want))
		}
		for i := range want {
			wantJSON, err := json.Marshal(want[i])
			if err != nil {
				t.Fatalf("%s: marshal %s: %v", round, want[i].ID, err)
			}
			if encoded[i].ID != want[i].ID || !bytes.Equal(encoded[i].JSON, wantJSON) {
				t.Fatalf("%s: encoding %d (%s) differs from json.Marshal", round, i, want[i].ID)
			}
		}
		return int(after.hits - stats.hits), int(after.misses - stats.misses)
	}

	if hits, misses := check("cold"); hits != 0 || misses == 0 {
		t.Fatalf("cold: hits=%d misses=%d, want only misses", hits, misses)
	}
	if hits, misses := check("warm"); misses != 0 || hits == 0 {
		t.Fatalf("warm: hits=%d misses=%d, want only hits", hits, misses)
	}

	// Change three VMs in different ways; only those three may miss.
	changed := 0
	for i := range resources {
		if resources[i].Type != unifiedresources.ResourceTypeVM || changed == 3 {
			continue
		}
		switch changed {
		case 0:
			resources[i].LastSeen = resources[i].LastSeen.Add(time.Second)
		case 1:
			resources[i].Tags = append(append([]string(nil), resources[i].Tags...), "cache-check")
		case 2:
			resources[i].Name += "-renamed"
		}
		changed++
	}
	if changed != 3 {
		t.Fatalf("fixture has %d VMs, need 3", changed)
	}
	if hits, misses := check("three VMs changed"); misses != 3 {
		t.Fatalf("three VMs changed: hits=%d misses=%d, want exactly 3 misses", hits, misses)
	}
}
