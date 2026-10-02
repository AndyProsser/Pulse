package websocket

import (
	"encoding/json"
	"fmt"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
)

// ResourceEntryKey returns the id the state delta keys one encoded resource
// by, exactly as it reads it from the full encoded state. Producers of
// models.EncodedResource use it to fill EncodedResource.ID. It reports false
// for entries the fast scanner leaves to the reference decoder.
func ResourceEntryKey(encoded []byte) (string, bool) {
	id, err := entryID(encoded)
	if err != nil || id == "" {
		return "", false
	}
	return id, true
}

// encodedResourcesFor returns state as a frontend state whose cached resource
// encodings can stand in for encoding Resources: one per resource, each with
// a distinct non-empty key and a body.
func encodedResourcesFor(state interface{}) (models.StateFrontend, bool) {
	frontend, ok := state.(models.StateFrontend)
	if !ok || frontend.EncodedResources == nil || len(frontend.EncodedResources) != len(frontend.Resources) {
		return models.StateFrontend{}, false
	}
	seen := make(map[string]struct{}, len(frontend.EncodedResources))
	for _, entry := range frontend.EncodedResources {
		if entry.ID == "" || len(entry.JSON) == 0 {
			return models.StateFrontend{}, false
		}
		if _, duplicate := seen[entry.ID]; duplicate {
			return models.StateFrontend{}, false
		}
		seen[entry.ID] = struct{}{}
	}
	return frontend, true
}

// snapshotFromEncodedResources encodes everything but the resources and
// splices in their cached encodings, so unchanged resources are neither
// re-encoded nor re-scanned.
func snapshotFromEncodedResources(frontend models.StateFrontend) (*clientStateSnapshot, error) {
	entries := frontend.EncodedResources
	frontend.Resources = nil
	frontend.EncodedResources = nil
	owned, err := json.Marshal(frontend)
	if err != nil {
		return nil, fmt.Errorf("marshal state snapshot: %w", err)
	}
	return snapshotFromOwnedEncodedWith(owned, entries)
}
