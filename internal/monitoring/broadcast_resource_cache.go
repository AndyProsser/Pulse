package monitoring

import (
	"encoding/json"
	"hash/maphash"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"

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
type broadcastResourceCache struct {
	mu      sync.Mutex
	entries map[string]broadcastResourceCacheEntry
	hits    uint64
	misses  uint64
}

type broadcastResourceCacheEntry struct {
	key      resourceContentKey
	prepared preparedBroadcastResource
	frontend models.ResourceFrontend
	encoded  []byte
	// entryKey is the id the websocket delta keys encoded by.
	entryKey string
}

type broadcastResourceCacheStats struct {
	hits   uint64
	misses uint64
}

func newBroadcastResourceCache() *broadcastResourceCache {
	return &broadcastResourceCache{entries: make(map[string]broadcastResourceCacheEntry)}
}

func (c *broadcastResourceCache) stats() broadcastResourceCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return broadcastResourceCacheStats{hits: c.hits, misses: c.misses}
}

// convertResourcesForBroadcastCached is convertResourcesForBroadcast reusing
// per-resource work cached from earlier builds. It also returns the encoding
// of each converted resource, in order, keyed as the websocket delta reads
// it, or nil when any could not be (the snapshot then encodes as before). Cached values are shared: callers
// must not modify the returned resources.
func convertResourcesForBroadcastCached(
	cache *broadcastResourceCache,
	allResources []unifiedresources.Resource,
	metricsTargetResolvers ...MetricsTargetResourceStore,
) ([]models.ResourceFrontend, []models.EncodedResource, broadcastResourceCatalogs) {
	if len(allResources) == 0 {
		return []models.ResourceFrontend{}, nil, broadcastResourceCatalogs{}
	}
	allResources = attachBroadcastMetricsTargets(
		allResources,
		firstBroadcastMetricsTargetResolver(metricsTargetResolvers),
	)
	allResources = unifiedresources.CoalescePresentationHostResources(allResources)

	cache.mu.Lock()
	previous := cache.entries
	cache.mu.Unlock()

	next := make(map[string]broadcastResourceCacheEntry, len(allResources))
	converted := make([]broadcastResourceCacheEntry, 0, len(allResources))
	catalogs := newBroadcastResourceCatalogs()
	encodable := true
	var hits, misses uint64
	var hasher resourceContentHasher
	for _, r := range allResources {
		key, hashable := hasher.key(&r)
		entry, found := previous[r.ID]
		if hashable && found && entry.key == key {
			hits++
		} else {
			misses++
			entry = broadcastResourceCacheEntry{key: key, prepared: prepareBroadcastResource(r)}
			entry.frontend = models.ConvertResourceToFrontend(entry.prepared.input)
			if encoded, err := json.Marshal(entry.frontend); err == nil {
				if key, ok := websocket.ResourceEntryKey(encoded); ok {
					entry.encoded, entry.entryKey = encoded, key
				}
			}
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
	cache.misses += misses
	cache.mu.Unlock()
	return result, encodings, catalogs.finish()
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
		for i := 0; i < v.NumField(); i++ {
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
