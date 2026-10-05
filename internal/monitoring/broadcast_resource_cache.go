package monitoring

import (
	"bytes"
	"encoding/json"
	"hash/maphash"
	"math"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
	"github.com/rcourtman/pulse-go-rewrite/internal/unifiedresources"
	"github.com/rcourtman/pulse-go-rewrite/internal/websocket"
)

// broadcastResourceCache keeps each resource's broadcast conversion and its
// JSON encoding from one build to the next. Every broadcast re-converts the
// whole estate although only a handful of resources change between two
// broadcasts; an entry is reused only when the resource going into
// conversion has exactly the same content, so a hit returns what converting
// it again would. Conversion depends on nothing but that resource.
//
// Heartbeat timestamps are the exception: a Kubernetes cluster report moves
// the sighting and refresh times of every object in the cluster - about a
// thousand on a homelab k3s - and nothing else. The key leaves those times
// (including each source's own sighting) out; of them only lastSeen reaches
// the broadcast, so an entry that differs
// only there has that one value patched into its encoding.
type broadcastResourceCache struct {
	mu      sync.Mutex
	entries map[string]broadcastResourceCacheEntry
	hits    uint64
	patched uint64
	misses  uint64
}

type broadcastResourceCacheEntry struct {
	key      resourceContentKey
	prepared preparedBroadcastResource
	frontend models.ResourceFrontend
	encoded  []byte
	// entryKey is the id the websocket delta keys encoded by.
	entryKey string
	// lastSeen is frontend.LastSeen; around it, encoded splits into
	// lastSeenPrefix and lastSeenSuffix (both nil when it cannot be located).
	lastSeen       int64
	lastSeenPrefix []byte
	lastSeenSuffix []byte
}

type broadcastResourceCacheStats struct {
	hits    uint64
	patched uint64
	misses  uint64
}

var broadcastResourceCacheResults = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "pulse_websocket_state_resource_cache_total",
	Help: "Resources in built broadcast states by cache result: hit (reused), patched (reused with a new lastSeen), miss (converted and encoded).",
}, []string{"result"})

func newBroadcastResourceCache() *broadcastResourceCache {
	return &broadcastResourceCache{entries: make(map[string]broadcastResourceCacheEntry)}
}

func (c *broadcastResourceCache) stats() broadcastResourceCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return broadcastResourceCacheStats{hits: c.hits, patched: c.patched, misses: c.misses}
}

// convertPresentationResourcesForBroadcastCached is
// convertPresentationResourcesForBroadcast reusing per-resource work cached
// from earlier builds. It also returns the encoding of each converted
// resource, in order, keyed as the websocket snapshot reads it, or nil when
// any could not be (the snapshot then encodes as before). Cached values are
// shared: callers must not modify the returned resources.
func convertPresentationResourcesForBroadcastCached(
	cache *broadcastResourceCache,
	allResources []unifiedresources.Resource,
) ([]models.ResourceFrontend, []models.EncodedResource, broadcastResourceCatalogs) {
	if len(allResources) == 0 {
		return []models.ResourceFrontend{}, nil, broadcastResourceCatalogs{}
	}

	cache.mu.Lock()
	previous := cache.entries
	cache.mu.Unlock()

	next := make(map[string]broadcastResourceCacheEntry, len(allResources))
	converted := make([]broadcastResourceCacheEntry, 0, len(allResources))
	catalogs := newBroadcastResourceCatalogs()
	encodable := true
	var hits, patched, misses uint64
	var hasher resourceContentHasher
	for _, r := range allResources {
		key, hashable := hasher.key(&r)
		lastSeen := monitorLastSeenUnix(r.LastSeen)
		entry, found := previous[r.ID]
		switch {
		case hashable && found && entry.key == key && entry.lastSeen == lastSeen:
			hits++
		case hashable && found && entry.key == key && entry.lastSeenPrefix != nil:
			patched++
			entry = entry.withLastSeen(lastSeen)
		default:
			misses++
			entry = newBroadcastResourceCacheEntry(key, r)
			if entry.encoded == nil {
				encodable = false
			}
		}
		catalogs.add(entry.prepared)
		if hashable && entry.encoded != nil {
			next[r.ID] = entry
		}
		converted = append(converted, entry)
	}

	sort.Slice(converted, func(i, j int) bool {
		left, right := converted[i].prepared, converted[j].prepared
		if left.sortKey == right.sortKey {
			return left.input.ID < right.input.ID
		}
		return left.sortKey < right.sortKey
	})

	result := make([]models.ResourceFrontend, len(converted))
	encodings := make([]models.EncodedResource, len(converted))
	for i, entry := range converted {
		result[i] = entry.frontend
		encodings[i] = models.EncodedResource{ID: entry.entryKey, JSON: entry.encoded}
	}
	if !encodable {
		encodings = nil
	}

	cache.mu.Lock()
	cache.entries = next
	cache.hits += hits
	cache.patched += patched
	cache.misses += misses
	cache.mu.Unlock()
	broadcastResourceCacheResults.WithLabelValues("hit").Add(float64(hits))
	broadcastResourceCacheResults.WithLabelValues("patched").Add(float64(patched))
	broadcastResourceCacheResults.WithLabelValues("miss").Add(float64(misses))
	return result, encodings, catalogs.finish()
}

// lastSeenSentinel stands in for lastSeen when an entry is first encoded so
// the value's position can be found unambiguously.
const lastSeenSentinel int64 = math.MinInt64 + 0x5157

var lastSeenSentinelMember = []byte(`"lastSeen":` + strconv.FormatInt(lastSeenSentinel, 10))

// newBroadcastResourceCacheEntry converts and encodes one resource. encoded
// stays nil when the resource cannot be encoded or keyed for the websocket
// delta.
func newBroadcastResourceCacheEntry(key resourceContentKey, r unifiedresources.Resource) broadcastResourceCacheEntry {
	entry := broadcastResourceCacheEntry{key: key, prepared: prepareBroadcastResource(r)}
	entry.frontend = models.ConvertResourceToFrontend(entry.prepared.input)
	entry.lastSeen = entry.frontend.LastSeen

	withSentinel := entry.frontend
	withSentinel.LastSeen = lastSeenSentinel
	if encoded, err := json.Marshal(withSentinel); err == nil && bytes.Count(encoded, lastSeenSentinelMember) == 1 {
		at := bytes.Index(encoded, lastSeenSentinelMember) + len(`"lastSeen":`)
		entry.lastSeenPrefix = encoded[:at:at]
		entry.lastSeenSuffix = encoded[at+len(lastSeenSentinelMember)-len(`"lastSeen":`):]
		entry.encoded = entry.spliceLastSeen(entry.lastSeen)
	} else if encoded, err := json.Marshal(entry.frontend); err == nil {
		entry.encoded = encoded
	}
	if entry.encoded != nil {
		key, ok := websocket.ResourceEntryKey(entry.encoded)
		if !ok {
			entry.encoded = nil
		}
		entry.entryKey = key
	}
	return entry
}

// withLastSeen returns the entry for the same content seen at another time.
func (e broadcastResourceCacheEntry) withLastSeen(lastSeen int64) broadcastResourceCacheEntry {
	e.frontend.LastSeen = lastSeen
	e.lastSeen = lastSeen
	e.encoded = e.spliceLastSeen(lastSeen)
	return e
}

func (e broadcastResourceCacheEntry) spliceLastSeen(lastSeen int64) []byte {
	out := make([]byte, 0, len(e.lastSeenPrefix)+20+len(e.lastSeenSuffix))
	out = append(out, e.lastSeenPrefix...)
	out = strconv.AppendInt(out, lastSeen, 10)
	return append(out, e.lastSeenSuffix...)
}

// resourceContentKey identifies a resource's full content: two resources with
// the same key convert and encode identically. It is 128 bits of keyed hash
// over a canonical walk of every field, exported or not.
type resourceContentKey [2]uint64

var resourceContentSeeds = [2]maphash.Seed{maphash.MakeSeed(), maphash.MakeSeed()}

// resourceContentHasher serialises a value into a reusable buffer and hashes
// the buffer. Maps are folded order-independently; times use their JSON text
// so instants that encode differently never share a key.
type resourceContentHasher struct {
	buf     []byte
	scratch []byte
}

const resourceContentMaxDepth = 64

func (h *resourceContentHasher) key(resource *unifiedresources.Resource) (resourceContentKey, bool) {
	h.buf = h.buf[:0]
	if !h.write(reflect.ValueOf(resource).Elem(), 0) {
		return resourceContentKey{}, false
	}
	return resourceContentKey{
		maphash.Bytes(resourceContentSeeds[0], h.buf),
		maphash.Bytes(resourceContentSeeds[1], h.buf),
	}, true
}

var timeType = reflect.TypeOf(time.Time{})

// resourceContentVolatileFields lists, per struct type, the heartbeat fields
// the content key leaves out. Only Resource.LastSeen reaches the broadcast,
// and the cache patches it; the others never do (tests pin both).
var resourceContentVolatileFields = map[reflect.Type][]bool{
	reflect.TypeOf(unifiedresources.Resource{}):                   volatileFieldMask(unifiedresources.Resource{}, "LastSeen", "UpdatedAt"),
	reflect.TypeOf(unifiedresources.ResourceDiscoveryReadiness{}): volatileFieldMask(unifiedresources.ResourceDiscoveryReadiness{}, "GeneratedAt"),
	// Per-source sightings feed metric sample times, not the broadcast; the
	// source's status string still counts.
	reflect.TypeOf(unifiedresources.SourceStatus{}): volatileFieldMask(unifiedresources.SourceStatus{}, "LastSeen"),
}

func volatileFieldMask(value any, names ...string) []bool {
	t := reflect.TypeOf(value)
	mask := make([]bool, t.NumField())
	for _, name := range names {
		field, ok := t.FieldByName(name)
		if !ok || len(field.Index) != 1 {
			panic("broadcast resource cache: no field " + t.Name() + "." + name)
		}
		mask[field.Index[0]] = true
	}
	return mask
}

// write appends a canonical, self-delimiting serialisation of v. Times in
// unexported fields fall back to their struct fields, which can only cause
// extra misses. It reports
// false for values it cannot serialise faithfully (functions, channels, very
// deep or cyclic graphs); such resources are simply never cached.
func (h *resourceContentHasher) write(v reflect.Value, depth int) bool {
	if depth > resourceContentMaxDepth {
		return false
	}
	if v.Type() == timeType && v.CanInterface() {
		h.buf = append(h.buf, 't')
		h.buf = v.Interface().(time.Time).AppendFormat(h.buf, time.RFC3339Nano)
		h.buf = append(h.buf, 0)
		return true
	}
	switch v.Kind() {
	case reflect.String:
		h.buf = appendUvarint(append(h.buf, 's'), uint64(v.Len()))
		h.buf = append(h.buf, v.String()...)
	case reflect.Bool:
		if v.Bool() {
			h.buf = append(h.buf, 'T')
		} else {
			h.buf = append(h.buf, 'F')
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		h.buf = appendUvarint(append(h.buf, 'i'), uint64(v.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		h.buf = appendUvarint(append(h.buf, 'u'), v.Uint())
	case reflect.Float32, reflect.Float64:
		h.buf = appendUvarint(append(h.buf, 'f'), math.Float64bits(v.Float()))
	case reflect.Pointer:
		if v.IsNil() {
			h.buf = append(h.buf, 'n')
			return true
		}
		h.buf = append(h.buf, 'p')
		return h.write(v.Elem(), depth+1)
	case reflect.Interface:
		if v.IsNil() {
			h.buf = append(h.buf, 'n')
			return true
		}
		elem := v.Elem()
		name := elem.Type().String()
		h.buf = appendUvarint(append(h.buf, 'I'), uint64(len(name)))
		h.buf = append(h.buf, name...)
		return h.write(elem, depth+1)
	case reflect.Slice:
		if v.IsNil() {
			h.buf = append(h.buf, 'n')
			return true
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			h.buf = appendUvarint(append(h.buf, 'b'), uint64(v.Len()))
			h.buf = append(h.buf, v.Bytes()...)
			return true
		}
		fallthrough
	case reflect.Array:
		h.buf = appendUvarint(append(h.buf, 'a'), uint64(v.Len()))
		for i := 0; i < v.Len(); i++ {
			if !h.write(v.Index(i), depth+1) {
				return false
			}
		}
	case reflect.Map:
		if v.IsNil() {
			h.buf = append(h.buf, 'n')
			return true
		}
		return h.writeMap(v, depth)
	case reflect.Struct:
		h.buf = append(h.buf, '{')
		volatile := resourceContentVolatileFields[v.Type()]
		for i := 0; i < v.NumField(); i++ {
			if volatile != nil && volatile[i] {
				continue
			}
			if !h.write(v.Field(i), depth+1) {
				return false
			}
		}
		h.buf = append(h.buf, '}')
	default:
		return false
	}
	return true
}

// writeMap folds each entry's hash with addition, so iteration order does not
// matter. Entries are serialised into the hasher's scratch space first.
func (h *resourceContentHasher) writeMap(v reflect.Value, depth int) bool {
	var sums [2]uint64
	outer := h.buf
	iter := v.MapRange()
	for iter.Next() {
		h.buf = h.scratch[:0]
		if !h.write(iter.Key(), depth+1) || !h.write(iter.Value(), depth+1) {
			h.buf = outer
			return false
		}
		sums[0] += maphash.Bytes(resourceContentSeeds[0], h.buf)
		sums[1] += maphash.Bytes(resourceContentSeeds[1], h.buf)
		h.scratch = h.buf
	}
	h.buf = appendUvarint(append(outer, 'm'), uint64(v.Len()))
	h.buf = appendUvarint(h.buf, sums[0])
	h.buf = appendUvarint(h.buf, sums[1])
	return true
}

func appendUvarint(dst []byte, x uint64) []byte {
	for x >= 0x80 {
		dst = append(dst, byte(x)|0x80)
		x >>= 7
	}
	return append(dst, byte(x))
}
