package websocket

import (
	"encoding/json"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
)

// ResourceEntryKey returns the id the state snapshot keys one encoded
// resource by, decoded from its bytes exactly as buildFrontendStateSnapshot
// does. Producers of models.EncodedResource use it to fill
// EncodedResource.ID; it reports false when the entry has no usable id.
func ResourceEntryKey(encoded []byte) (string, bool) {
	var identity struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(encoded, &identity); err != nil || identity.ID == "" {
		return "", false
	}
	return identity.ID, true
}

// consistentEncodedResources returns state's cached resource encodings when
// they can stand in for encoding Resources: one per resource, each with a
// body and a distinct non-empty id.
func consistentEncodedResources(state models.StateFrontend) ([]models.EncodedResource, bool) {
	entries := state.EncodedResources
	if entries == nil || len(entries) != len(state.Resources) {
		return nil, false
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.ID == "" || len(entry.JSON) == 0 {
			return nil, false
		}
		if _, duplicate := seen[entry.ID]; duplicate {
			return nil, false
		}
		seen[entry.ID] = struct{}{}
	}
	return entries, true
}

// buildFrontendStateSnapshotFromEncoded encodes everything but the resources
// through the generic path and keys the cached resource encodings by their
// recorded ids, so unchanged resources are neither encoded nor decoded again.
// The encodings are immutable; the snapshot retains them as they are.
func buildFrontendStateSnapshotFromEncoded(state models.StateFrontend, entries []models.EncodedResource) (*clientStateSnapshot, error) {
	state.Resources = nil
	state.EncodedResources = nil
	snapshot, err := buildGenericClientStateSnapshot(state)
	if err != nil {
		return nil, err
	}
	snapshot.resources = make(map[string]json.RawMessage, len(entries))
	snapshot.resourceOrder = make([]string, 0, len(entries))
	for _, entry := range entries {
		snapshot.resources[entry.ID] = json.RawMessage(entry.JSON)
		snapshot.resourceOrder = append(snapshot.resourceOrder, entry.ID)
	}
	return snapshot, nil
}
