package websocket

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
)

// ---- fixtures ---------------------------------------------------------------

// realisticFrontendState mirrors the production payload measured on the
// reference instance: ~1,200 resources of ~2.4 KB each, nearly all of the state
// bytes. tick advances the live-metric fields on every resource, which is what
// makes almost every entry change on every broadcast.
func realisticFrontendState(count, tick int) models.StateFrontend {
	state := benchmarkFrontendState(count)
	state.LastUpdate = int64(1000 + tick)
	for i := range state.Resources {
		r := &state.Resources[i]
		r.LastSeen = int64(5000 + tick)
		r.CPU = &models.ResourceMetricFrontend{Current: float64((i + tick) % 100)}
		r.Memory = &models.ResourceMetricFrontend{Current: float64((i*3 + tick) % 100)}
		r.PlatformData = json.RawMessage(fmt.Sprintf(
			`{"vmid":%d,"node":"pve-%d","uptime":%d,"tags":["a","b","c"],"net":{"in":%d,"out":%d},"disks":[%s],"notes":%q}`,
			i, i%4, 86400+tick, 1000*i+tick, 2000*i+tick, strings.TrimSuffix(strings.Repeat(`{"dev":"sda","size":107374182400,"used":53687091200,"health":"PASSED"},`, 6), ","),
			strings.Repeat("note ", 200),
		))
	}
	return state
}

// ---- merge patch ------------------------------------------------------------

func TestMergePatchKeepsLargeIntegersExact(t *testing.T) {
	previous := json.RawMessage(`{"id":"a","bytes":1}`)
	current := json.RawMessage(`{"id":"a","bytes":18446744073709551615}`)
	got, err := createJSONMergePatch(previous, current)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"bytes":18446744073709551615,"id":"a"}` {
		t.Fatalf("patch = %s; integers must not round-trip through float64", got)
	}
}

func TestMergePatchAllocationsStaySmall(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful under the race detector")
	}
	state := realisticFrontendState(2, 0)
	next := realisticFrontendState(2, 1)
	previous, _ := json.Marshal(state.Resources[0])
	current, _ := json.Marshal(next.Resources[0])
	allocs := testing.AllocsPerRun(50, func() {
		if _, err := createJSONMergePatch(previous, current); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 60 {
		t.Fatalf("createJSONMergePatch allocations = %.0f for a %d byte entry; want <= 60", allocs, len(current))
	}
}

// randomJSON builds a deterministic nested value covering every JSON kind.
func randomJSON(r *rand.Rand, depth int) interface{} {
	switch k := r.Intn(8); {
	case k == 0:
		return nil
	case k == 1:
		return r.Intn(2) == 0
	case k == 2:
		return r.Intn(1000)
	case k == 3:
		return float64(r.Intn(1000)) / 8
	case k == 4:
		return fmt.Sprintf("s-%d \"q\" \\ é\n", r.Intn(50))
	case k == 5 && depth > 0:
		n := r.Intn(4)
		out := make([]interface{}, n)
		for i := range out {
			out[i] = randomJSON(r, depth-1)
		}
		return out
	case depth > 0:
		return randomObject(r, depth-1)
	default:
		return r.Intn(10)
	}
}

func randomObject(r *rand.Rand, depth int) map[string]interface{} {
	obj := map[string]interface{}{}
	for i, n := 0, r.Intn(6); i < n; i++ {
		obj[fmt.Sprintf("k%d", r.Intn(8))] = randomJSON(r, depth)
	}
	return obj
}

func mutateJSON(r *rand.Rand, v interface{}) interface{} {
	obj, ok := v.(map[string]interface{})
	if !ok || r.Intn(6) == 0 {
		return randomJSON(r, 2)
	}
	out := make(map[string]interface{}, len(obj))
	for k, val := range obj {
		switch r.Intn(6) {
		case 0: // drop key
		case 1:
			out[k] = randomJSON(r, 2)
		case 2:
			out[k] = mutateJSON(r, val)
		default:
			out[k] = val
		}
	}
	if r.Intn(3) == 0 {
		out[fmt.Sprintf("k%d", r.Intn(8))] = randomJSON(r, 2)
	}
	return out
}

// applyMergePatch implements RFC 7386 on decoded values.
func applyMergePatch(target, patch interface{}) interface{} {
	patchObj, ok := patch.(map[string]interface{})
	if !ok {
		return patch
	}
	targetObj, ok := target.(map[string]interface{})
	if !ok {
		targetObj = map[string]interface{}{}
	}
	out := make(map[string]interface{}, len(targetObj))
	for k, v := range targetObj {
		out[k] = v
	}
	for k, v := range patchObj {
		if v == nil {
			delete(out, k)
		} else {
			out[k] = applyMergePatch(out[k], v)
		}
	}
	return out
}

func decodeJSON(t testing.TB, raw []byte) interface{} {
	t.Helper()
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("invalid JSON %q: %v", raw, err)
	}
	return v
}

func TestMergePatchMatchesLegacy(t *testing.T) {
	for seed := int64(1); seed <= 3000; seed++ {
		r := rand.New(rand.NewSource(seed))
		prevObj := randomObject(r, 3)
		curObj := mutateJSON(r, prevObj)
		if seed%7 == 0 { // include non-object roots
			curObj = randomJSON(r, 2)
		}
		previous, _ := json.Marshal(prevObj)
		current, _ := json.Marshal(curObj)

		got, gotErr := createJSONMergePatch(previous, current)
		want, wantErr := createJSONMergePatchLegacy(previous, current)
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("seed %d: error mismatch got=%v want=%v", seed, gotErr, wantErr)
		}
		if !reflect.DeepEqual(decodeJSON(t, got), decodeJSON(t, want)) {
			t.Fatalf("seed %d: patch differs\n prev=%s\n cur=%s\n got=%s\nwant=%s", seed, previous, current, got, want)
		}
	}
}

// stripNulls removes null members: RFC 7386 cannot represent them, so the
// reconstruction property only holds for null-free documents.
func stripNulls(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, val := range x {
			if val != nil {
				out[k] = stripNulls(val)
			}
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, val := range x {
			out[i] = stripNulls(val)
		}
		return out
	}
	return v
}

func TestMergePatchAppliedToPreviousReconstructsCurrent(t *testing.T) {
	for seed := int64(1); seed <= 3000; seed++ {
		r := rand.New(rand.NewSource(seed))
		prevObj := stripNulls(randomObject(r, 3)).(map[string]interface{})
		curObj := stripNulls(mutateJSON(r, prevObj))
		if _, isObj := curObj.(map[string]interface{}); !isObj {
			continue
		}
		previous, _ := json.Marshal(prevObj)
		current, _ := json.Marshal(curObj)
		patch, err := createJSONMergePatch(previous, current)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		applied := applyMergePatch(decodeJSON(t, previous), decodeJSON(t, patch))
		if !reflect.DeepEqual(applied, decodeJSON(t, current)) {
			t.Fatalf("seed %d: previous+patch != current\n prev=%s\n cur=%s\n patch=%s", seed, previous, current, patch)
		}
	}
}

func TestMergePatchMalformedInputMatchesLegacyErrors(t *testing.T) {
	for _, pair := range [][2]string{
		{`{"a":1}`, `{"a":`}, {`{"a":1`, `{"a":2}`}, {``, `{}`}, {`{}`, ``},
		{`{"a":[1}]}`, `{"a":[2]}`}, {`{"a":tru}`, `{"a":true}`}, {`{"a" 1}`, `{"a":1}`},
		{`{"a":1,}`, `{"a":2}`}, {`[1,2`, `[1,3]`}, {`{"a":"\x"}`, `{"a":"b"}`},
	} {
		got, gotErr := createJSONMergePatch(json.RawMessage(pair[0]), json.RawMessage(pair[1]))
		_, wantErr := createJSONMergePatchLegacy(json.RawMessage(pair[0]), json.RawMessage(pair[1]))
		if (gotErr == nil) != (wantErr == nil) {
			t.Errorf("%q -> %q: got err=%v (patch %s), legacy err=%v", pair[0], pair[1], gotErr, got, wantErr)
		}
	}
}

// ---- snapshot ----------------------------------------------------------------

func snapshotsEqual(a, b *clientStateSnapshot) bool {
	return reflect.DeepEqual(a, b)
}

func TestSnapshotMatchesLegacyAcrossShapes(t *testing.T) {
	docs := []string{
		`{}`, `null`, `[]`, `{"resources":null}`, `{"resources":[]}`,
		`{"resources":[{"id":"a"},{"id":"b"}],"x":1,"y":{"z":[1,2,{"id":"inner"}]}}`,
		`{"resources":[{"name":"n","id":"尾\"\\\n&","nested":{"id":"no"}}]}`,
		`{"resources":[{"id":"a","id":"b"}]}`,
		`{"resources":[{"ID":"upper"},{"Id":"mixed"}]}`,
		`{"resources":[{"id":"escaped-key"}]}`,
		`{"resources":[{"id":"a"},{"id":"a"}]}`, `{"resources":[{}]}`, `{"resources":[null]}`,
		`{"resources":[{"id":null}]}`, `{"resources":[{"id":42}]}`, `{"resources":[{"id":null,"id":"late"}]}`,
		`{"resources":{"id":"not-an-array"}}`, `{"resources":[{"id":"a"}],"resources":[{"id":"b"}]}`,
		`{"resources":[{"id":"a"}]}`,
		`{"connectedInfrastructure":[{"id":"i1"},{"id":"i2"}],"activeAlerts":[{"id":"a1"}]}`,
		`{"connectedInfrastructure":[{"id":"i1"},{}],"activeAlerts":[{"id":"a1"},{"id":"a1"}]}`,
		`{"connectedInfrastructure":null,"activeAlerts":[]}`,
		"{\n \"resources\" : [ {\"id\" : \"sp\" , \"v\" : [ 1 , 2 ] } ] \n}\n",
		`{"a":"}{][","resources":[{"id":"q","s":"]}\"[{"}]}`,
		`{"resources":[{"id":"a","big":18446744073709551615,"f":1.5e300,"neg":-0}]}`,
	}
	for _, doc := range docs {
		want, wantErr := buildClientStateSnapshot(encodedStateFixture{json.RawMessage(doc)})
		// Oracle: the legacy parser applied to what the encoder emits.
		var enc bytes.Buffer
		if err := json.NewEncoder(&enc).Encode(encodedStateFixture{json.RawMessage(doc)}); err != nil {
			t.Fatalf("%q: encoder rejected fixture: %v", doc, err)
		}
		legacy, legacyErr := snapshotFromEncodedLegacy(enc.Bytes())
		if (wantErr == nil) != (legacyErr == nil) {
			t.Fatalf("%q: error mismatch fast=%v legacy=%v", doc, wantErr, legacyErr)
		}
		if wantErr == nil && !snapshotsEqual(want, legacy) {
			t.Fatalf("%q: snapshot differs\nfast  =%#v\nlegacy=%#v", doc, want, legacy)
		}
	}
}

func TestSnapshotRandomStatesMatchLegacy(t *testing.T) {
	for seed := int64(1); seed <= 400; seed++ {
		r := rand.New(rand.NewSource(seed))
		doc := map[string]interface{}{}
		for _, key := range []string{"resources", "connectedInfrastructure", "activeAlerts"} {
			n := r.Intn(6)
			entries := make([]interface{}, n)
			for i := range entries {
				e := randomObject(r, 3)
				if r.Intn(40) != 0 {
					e["id"] = fmt.Sprintf("%s-%d-é\"%d", key, i, r.Intn(3))
				}
				entries[i] = e
			}
			if r.Intn(8) != 0 {
				doc[key] = entries
			}
		}
		for k, v := range randomObject(r, 2) {
			doc["f"+k] = v
		}
		raw, _ := json.Marshal(doc)
		got, gotErr := buildClientStateSnapshot(encodedStateFixture{raw})
		emitted, err := json.Marshal(encodedStateFixture{raw})
		if err != nil {
			t.Fatal(err)
		}
		want, wantErr := snapshotFromEncodedLegacy(emitted)
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("seed %d: error mismatch fast=%v legacy=%v doc=%s", seed, gotErr, wantErr, raw)
		}
		if gotErr == nil && !snapshotsEqual(got, want) {
			t.Fatalf("seed %d: snapshot differs for %s", seed, raw)
		}
	}
}

func TestSnapshotEntriesDoNotAliasEachOther(t *testing.T) {
	snap, err := buildClientStateSnapshot(encodedStateFixture{json.RawMessage(
		`{"resources":[{"id":"r-1","n":"alpha"},{"id":"r-2","n":"beta"}]}`)})
	if err != nil {
		t.Fatal(err)
	}
	first := snap.resources["r-1"]
	_ = append(first, "OVERWRITE"...)
	if string(snap.resources["r-2"]) != `{"id":"r-2","n":"beta"}` {
		t.Fatalf("appending to one retained entry corrupted its neighbour: %s", snap.resources["r-2"])
	}
}

// allocatedPerRun reports average heap bytes allocated per call with GC held
// off, so encoding/json's pooled encode buffer is measured in steady state.
func allocatedPerRun(t *testing.T, runs int, fn func()) float64 {
	t.Helper()
	// One P keeps encoding/json's per-P encode-buffer pool from going cold
	// when the goroutine migrates; GC off keeps it from being evicted.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	fn() // warm pools
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		fn()
	}
	runtime.ReadMemStats(&after)
	return float64(after.TotalAlloc-before.TotalAlloc) / float64(runs)
}

// Parsing the encoded state must add little on top of encoding it. The legacy
// parse copied the whole state twice more (fields, then entries), adding ~2.8x
// the payload; the scan keeps the one marshalled copy and only builds the id
// index.
func TestSnapshotAddsLittleOverEncodingTheState(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation measurements are not meaningful under the race detector")
	}
	state := realisticFrontendState(1000, 0)
	payload, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	encodeOnly := allocatedPerRun(t, 8, func() {
		if _, err := json.Marshal(state); err != nil {
			t.Fatal(err)
		}
	})
	withSnapshot := allocatedPerRun(t, 8, func() {
		if _, err := buildClientStateSnapshot(state); err != nil {
			t.Fatal(err)
		}
	})
	if extra, limit := withSnapshot-encodeOnly, 0.25*float64(len(payload)); extra > limit {
		t.Fatalf("snapshot parse allocates %.0f bytes beyond encoding (%.2fx of the %d byte state); want <= 0.25x",
			extra, extra/float64(len(payload)), len(payload))
	}
}

func BenchmarkBuildClientStateSnapshotRealistic1220(b *testing.B) {
	state := realisticFrontendState(1220, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := buildClientStateSnapshot(state); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildClientStateDeltaRealistic1220AllEntriesChange(b *testing.B) {
	previous, err := buildClientStateSnapshot(realisticFrontendState(1220, 0))
	if err != nil {
		b.Fatal(err)
	}
	current, err := buildClientStateSnapshot(realisticFrontendState(1220, 1))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := buildClientStateDelta(previous, current); err != nil {
			b.Fatal(err)
		}
	}
}

// Arbitrary bytes must behave identically to the encoding/json reference:
// the same error-or-not and the same patch meaning.
func FuzzMergePatchMatchesLegacy(f *testing.F) {
	for _, seed := range [][2]string{
		{`{"id":"a","v":1}`, `{"id":"a","v":2}`},
		{`{"a":{"b":1,"c":[1,2]}}`, `{"a":{"b":2,"c":[1,2]},"d":null}`},
		{`{"a":1,"a":2}`, `{"a":3}`}, {`{"a":1}`, `{"a":2}`},
		{`[1]`, `[2]`}, {`1`, `{}`}, {`{}`, `{}`}, {` {"a":1} `, "\n{\"a\":2}\n"},
		{`{"a":01}`, `{"a":1}`}, {`{"a":"\ud800"}`, `{"a":"x"}`},
	} {
		f.Add([]byte(seed[0]), []byte(seed[1]))
	}
	f.Fuzz(func(t *testing.T, previous, current []byte) {
		got, gotErr := createJSONMergePatch(previous, current)
		want, wantErr := createJSONMergePatchLegacy(previous, current)
		// The reference decodes numbers into float64, so valid JSON such as
		// 1e700 makes it fail; the raw diff handling that is an improvement.
		legacyOverflow := wantErr != nil && gotErr == nil && json.Valid(previous) && json.Valid(current) &&
			strings.Contains(wantErr.Error(), "cannot unmarshal number")
		if (gotErr == nil) != (wantErr == nil) && !legacyOverflow {
			t.Fatalf("error mismatch fast=%v legacy=%v for %q -> %q", gotErr, wantErr, previous, current)
		}
		if gotErr != nil || legacyOverflow {
			return
		}
		var gv, wv interface{}
		if err := json.Unmarshal(got, &gv); err != nil {
			t.Fatalf("fast patch is not valid JSON: %q (%v)", got, err)
		}
		if err := json.Unmarshal(want, &wv); err != nil {
			t.Fatal(err)
		}
		// The contract is behavioural: applying either patch to the previous
		// document must give the same result. Textually different but
		// numerically equal scalars (0 vs 0E0) may add a redundant, harmless
		// replacement to the raw diff, and large integers keep their exact
		// digits instead of passing through float64.
		var pv, cv interface{}
		if json.Unmarshal(previous, &pv) != nil || json.Unmarshal(current, &cv) != nil {
			return
		}
		_, prevIsObject := pv.(map[string]interface{})
		_, curIsObject := cv.(map[string]interface{})
		if !prevIsObject || !curIsObject {
			return // production only diffs object entries
		}
		if !reflect.DeepEqual(applyMergePatch(pv, gv), applyMergePatch(pv, wv)) {
			t.Fatalf("patches apply differently for %q -> %q\nfast=%s\nlegacy=%s", previous, current, got, want)
		}
	})
}

func FuzzSnapshotMatchesLegacy(f *testing.F) {
	for _, doc := range []string{
		`{"resources":[{"id":"a"},{"id":"b"}],"x":1}`, `{"resources":[{"id":"a"},{"id":"a"}]}`,
		`{"connectedInfrastructure":[{"id":"i"}],"activeAlerts":[{}]}`, `null`, `[]`,
		`{"resources":[{"id":"a"}]}`, `{"resources":[{"ID":"a"}]}`,
	} {
		f.Add([]byte(doc))
	}
	f.Fuzz(func(t *testing.T, doc []byte) {
		if !json.Valid(doc) {
			t.Skip() // the encoder only ever hands the parser valid JSON
		}
		var normalized bytes.Buffer
		if err := json.Compact(&normalized, doc); err != nil {
			t.Skip()
		}
		got, gotErr := buildClientStateSnapshot(encodedStateFixture{normalized.Bytes()})
		// The oracle parses what the encoder emits (it HTML-escapes raw values).
		emitted, err := json.Marshal(encodedStateFixture{normalized.Bytes()})
		if err != nil {
			t.Skip()
		}
		want, wantErr := snapshotFromEncodedLegacy(emitted)
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("error mismatch fast=%v legacy=%v for %q", gotErr, wantErr, normalized.Bytes())
		}
		if gotErr == nil && !snapshotsEqual(got, want) {
			t.Fatalf("snapshot differs for %q", normalized.Bytes())
		}
	})
}
