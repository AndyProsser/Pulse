package websocket

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"unicode/utf8"
)

const resourceDeltaField = "resourceDelta"
const infrastructureField = "connectedInfrastructure"
const infrastructureDeltaField = "connectedInfrastructureDelta"
const activeAlertsField = "activeAlerts"
const activeAlertsDeltaField = "activeAlertsDelta"

// Keyed delta fields are id-keyed arrays that travel as per-item merge
// patches once a client baseline exists, instead of re-shipping the whole
// array on every broadcast. When a payload cannot be keyed (an entry without
// an id), it stays in fields and diffs whole, so the keyed path can never
// corrupt the projection.
var keyedDeltaFields = []struct {
	field      string
	deltaField string
}{
	{infrastructureField, infrastructureDeltaField},
	{activeAlertsField, activeAlertsDeltaField},
}

type keyedFieldSnapshot struct {
	entries map[string]json.RawMessage
	order   []string
	raw     json.RawMessage
}

type clientStateSnapshot struct {
	fields        map[string]json.RawMessage
	resources     map[string]json.RawMessage
	resourceOrder []string
	// Keyed snapshots by field name; a field is present only when every
	// entry keyed by id.
	keyed map[string]*keyedFieldSnapshot
}

// encodedSize approximates the encoded state size from what the snapshot
// retains (entries plus plain fields), for the snapshot-size gauge.
func (s *clientStateSnapshot) encodedSize() int {
	size := 0
	for _, entry := range s.resources {
		size += len(entry)
	}
	for _, value := range s.fields {
		size += len(value)
	}
	for _, keyed := range s.keyed {
		size += len(keyed.raw)
	}
	return size
}

type resourceDeltaPayload struct {
	Upserts []json.RawMessage `json:"upserts,omitempty"`
	Removed []string          `json:"removed,omitempty"`
	Order   []string          `json:"order,omitempty"`
}

func extractKeyedEntries(
	encoded json.RawMessage,
	field string,
) (map[string]json.RawMessage, []string, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(encoded, &entries); err != nil {
		return nil, nil, fmt.Errorf("decode state %s: %w", field, err)
	}
	// Decode all identities in one pass rather than allocating a decoder and
	// throwaway struct for every entry. Keys must come from the encoded entries,
	// not hints from the source value: checking one hinted ID cannot establish
	// the identity of the rest, especially if a marshaler changes their order.
	identities := make([]struct {
		ID string `json:"id"`
	}, len(entries))
	if err := json.Unmarshal(encoded, &identities); err != nil {
		return nil, nil, fmt.Errorf("decode state %s identity: %w", field, err)
	}

	byID := make(map[string]json.RawMessage, len(entries))
	order := make([]string, 0, len(entries))
	for i, entry := range entries {
		id := identities[i].ID
		if id == "" {
			return nil, nil, fmt.Errorf("state %s entry is missing id", field)
		}
		if _, exists := byID[id]; exists {
			return nil, nil, fmt.Errorf("state %s id %q is duplicated", field, id)
		}
		// RawMessage.UnmarshalJSON already copied each entry into its own buffer.
		// Retaining that buffer is safe; copying it a second time is redundant.
		byID[id] = entry
		order = append(order, id)
	}
	return byID, order, nil
}

// buildClientStateSnapshot encodes state once and parses it in a single scan.
// json.Marshal already returns one exactly sized copy of its (internally
// pooled) encode buffer, and that copy is the only one the snapshot keeps: every
// field and entry in it is a capped sub-slice. Anything the scanner does not
// handle (escaped keys, malformed or duplicate ids, a non-object state) goes
// through the reference decoder so its results and errors are unchanged.
func buildClientStateSnapshot(state interface{}) (*clientStateSnapshot, error) {
	owned, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("marshal state snapshot: %w", err)
	}
	if snapshot, scanErr := snapshotFromOwnedEncoded(owned); scanErr == nil {
		return snapshot, nil
	}
	return snapshotFromEncodedLegacy(owned)
}

func snapshotFromOwnedEncoded(owned []byte) (*clientStateSnapshot, error) {
	fields := make(map[string]json.RawMessage)
	err := forEachObjectMember(owned, func(key []byte, escaped bool, value []byte) error {
		// encoding/json rewrites invalid UTF-8 in keys; leave that to it.
		if escaped || !utf8.Valid(key) {
			return errScanUnsupported
		}
		fields[string(key)] = value[:len(value):len(value)]
		return nil
	})
	if err != nil {
		return nil, err
	}

	resources := make(map[string]json.RawMessage)
	resourceOrder := make([]string, 0)
	if encodedResources, ok := fields["resources"]; ok {
		resources, resourceOrder, err = extractKeyedEntriesFast(encodedResources)
		if err != nil {
			return nil, err
		}
		delete(fields, "resources")
	}

	snapshot := &clientStateSnapshot{
		fields:        fields,
		resources:     resources,
		resourceOrder: resourceOrder,
		keyed:         make(map[string]*keyedFieldSnapshot),
	}
	for _, keyedField := range keyedDeltaFields {
		encodedField, ok := fields[keyedField.field]
		if !ok {
			continue
		}
		entries, order, err := extractKeyedEntriesFast(encodedField)
		if err != nil {
			// The reference path keeps an unkeyable array as a plain field.
			return nil, err
		}
		snapshot.keyed[keyedField.field] = &keyedFieldSnapshot{
			entries: entries,
			order:   order,
			raw:     encodedField,
		}
		delete(fields, keyedField.field)
	}
	return snapshot, nil
}

// snapshotFromEncodedLegacy parses an encoded state with three full decodes
// (fields, entries, ids). It is the reference implementation: the one-pass
// scanner falls back to it for anything it does not handle, and tests use it
// as the oracle.
func snapshotFromEncodedLegacy(encoded []byte) (*clientStateSnapshot, error) {
	var err error

	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, fmt.Errorf("decode state snapshot: %w", err)
	}

	resources := make(map[string]json.RawMessage)
	resourceOrder := make([]string, 0)
	if encodedResources, ok := fields["resources"]; ok {
		resources, resourceOrder, err = extractKeyedEntries(encodedResources, "resource")
		if err != nil {
			return nil, err
		}
		delete(fields, "resources")
	}

	snapshot := &clientStateSnapshot{
		fields:        fields,
		resources:     resources,
		resourceOrder: resourceOrder,
		keyed:         make(map[string]*keyedFieldSnapshot),
	}

	for _, keyedField := range keyedDeltaFields {
		encodedField, ok := fields[keyedField.field]
		if !ok {
			continue
		}
		entries, order, keyErr := extractKeyedEntries(encodedField, keyedField.field)
		if keyErr != nil {
			continue
		}
		snapshot.keyed[keyedField.field] = &keyedFieldSnapshot{
			entries: entries,
			order:   order,
			raw:     append(json.RawMessage(nil), encodedField...),
		}
		delete(fields, keyedField.field)
	}

	return snapshot, nil
}

func buildKeyedArrayDelta(
	previousEntries, currentEntries map[string]json.RawMessage,
	previousOrder, currentOrder []string,
	label string,
) (resourceDeltaPayload, error) {
	payload := resourceDeltaPayload{}
	for _, id := range currentOrder {
		currentEntry := currentEntries[id]
		previousEntry, exists := previousEntries[id]
		if !exists {
			payload.Upserts = append(payload.Upserts, currentEntry)
			continue
		}
		if bytes.Equal(previousEntry, currentEntry) {
			continue
		}
		patch, err := createJSONMergePatch(previousEntry, currentEntry)
		if err != nil {
			return resourceDeltaPayload{}, fmt.Errorf("build %s %q patch: %w", label, id, err)
		}
		payload.Upserts = append(payload.Upserts, patch)
	}
	for id := range previousEntries {
		if _, exists := currentEntries[id]; !exists {
			payload.Removed = append(payload.Removed, id)
		}
	}
	sort.Strings(payload.Removed)
	if !reflect.DeepEqual(previousOrder, currentOrder) {
		payload.Order = append([]string(nil), currentOrder...)
	}
	return payload, nil
}

func (p resourceDeltaPayload) isEmpty() bool {
	return len(p.Upserts) == 0 && len(p.Removed) == 0 && len(p.Order) == 0
}

func buildClientStateDelta(previous, current *clientStateSnapshot) (map[string]interface{}, error) {
	if previous == nil || current == nil {
		return nil, fmt.Errorf("state delta requires previous and current snapshots")
	}

	delta := make(map[string]interface{})
	for key, currentValue := range current.fields {
		if previousValue, ok := previous.fields[key]; !ok || !bytes.Equal(previousValue, currentValue) {
			delta[key] = currentValue
		}
	}
	for key := range previous.fields {
		if _, ok := current.fields[key]; !ok {
			delta[key] = nil
		}
	}

	resourceDelta, err := buildKeyedArrayDelta(
		previous.resources,
		current.resources,
		previous.resourceOrder,
		current.resourceOrder,
		"resource",
	)
	if err != nil {
		return nil, err
	}
	if !resourceDelta.isEmpty() {
		delta[resourceDeltaField] = resourceDelta
	}

	for _, keyedField := range keyedDeltaFields {
		previousSnapshot := previous.keyed[keyedField.field]
		currentSnapshot := current.keyed[keyedField.field]
		switch {
		case previousSnapshot != nil && currentSnapshot != nil:
			keyedDelta, err := buildKeyedArrayDelta(
				previousSnapshot.entries,
				currentSnapshot.entries,
				previousSnapshot.order,
				currentSnapshot.order,
				keyedField.field,
			)
			if err != nil {
				return nil, err
			}
			if !keyedDelta.isEmpty() {
				delta[keyedField.deltaField] = keyedDelta
			}
		case currentSnapshot != nil:
			// The previous snapshot carried the array as a plain field (or
			// not at all): fall back to shipping the full array once so the
			// client re-adopts a clean baseline.
			if previousRaw, ok := previous.fields[keyedField.field]; !ok ||
				!bytes.Equal(previousRaw, currentSnapshot.raw) {
				delta[keyedField.field] = currentSnapshot.raw
			}
		case previousSnapshot != nil:
			// The array left the keyed path. If the current snapshot still
			// carries it as a plain field, the fields loop above already
			// shipped it whole; if it vanished entirely, clear it like any
			// removed field.
			if _, ok := current.fields[keyedField.field]; !ok {
				delta[keyedField.field] = nil
			}
		}
	}

	return delta, nil
}

func createJSONMergePatch(previous, current json.RawMessage) (json.RawMessage, error) {
	previousValue, currentValue := trimJSONSpace(previous), trimJSONSpace(current)
	if !isObjectValue(previousValue) || !isObjectValue(currentValue) {
		// Only entries (objects) are diffed in production; anything else is a
		// whole-value replacement. Object diffs validate as they scan.
		if validateJSONValue(previousValue) != nil || validateJSONValue(currentValue) != nil {
			return createJSONMergePatchLegacy(previous, current)
		}
		if bytes.Equal(previousValue, currentValue) {
			return json.RawMessage(`{}`), nil
		}
		// Replacement of a non-object value: re-encode compactly like the
		// reference implementation does.
		return json.Marshal(json.RawMessage(currentValue))
	}
	patch, changed, err := appendMergePatch(make([]byte, 0, 256), previousValue, currentValue, true)
	if err != nil {
		return createJSONMergePatchLegacy(previous, current)
	}
	if !changed {
		return json.RawMessage(`{}`), nil
	}
	return patch, nil
}

// createJSONMergePatchLegacy diffs by decoding both sides into interface{}
// trees. It is the reference implementation: the faster raw diff falls back to
// it for anything it does not handle, and tests use it as the oracle.
func createJSONMergePatchLegacy(previous, current json.RawMessage) (json.RawMessage, error) {
	var previousValue interface{}
	if err := json.Unmarshal(previous, &previousValue); err != nil {
		return nil, err
	}
	var currentValue interface{}
	if err := json.Unmarshal(current, &currentValue); err != nil {
		return nil, err
	}

	patchValue, changed := diffJSONMergeValue(previousValue, currentValue)
	if !changed {
		return json.RawMessage(`{}`), nil
	}
	if patchObject, ok := patchValue.(map[string]interface{}); ok {
		if currentObject, ok := currentValue.(map[string]interface{}); ok {
			if id, ok := currentObject["id"]; ok {
				patchObject["id"] = id
			}
		}
	}
	patch, err := json.Marshal(patchValue)
	if err != nil {
		return nil, err
	}
	return patch, nil
}

func diffJSONMergeValue(previous, current interface{}) (interface{}, bool) {
	if reflect.DeepEqual(previous, current) {
		return nil, false
	}

	previousObject, previousIsObject := previous.(map[string]interface{})
	currentObject, currentIsObject := current.(map[string]interface{})
	if !previousIsObject || !currentIsObject {
		return current, true
	}

	patch := make(map[string]interface{})
	for key := range previousObject {
		if _, exists := currentObject[key]; !exists {
			patch[key] = nil
		}
	}
	for key, currentValue := range currentObject {
		previousValue, exists := previousObject[key]
		if !exists {
			patch[key] = currentValue
			continue
		}
		if nestedPatch, changed := diffJSONMergeValue(previousValue, currentValue); changed {
			patch[key] = nestedPatch
		}
	}
	return patch, len(patch) > 0
}
