package websocket

import (
	"encoding/json"
	"sync"
)

// stateDeltaCache shares one broadcast's delta work between the clients that are
// on the same baseline. Clients that have each received the previous broadcast
// all hold the same previous snapshot, so the diff against the current snapshot
// and its encoded message are identical for them; computing them once makes the
// per-client cost of a broadcast a channel send instead of a diff and an encode.
//
// A cache lives for one broadcast only and is never stored on a snapshot:
// holding it there would chain every snapshot to its predecessor.
type stateDeltaCache struct {
	mu         sync.Mutex
	byBaseline map[*clientStateSnapshot]*encodedStateDelta
}

type encodedStateDelta struct {
	data  []byte // nil when empty
	empty bool
	err   error
}

func newStateDeltaCache() *stateDeltaCache {
	return &stateDeltaCache{byBaseline: make(map[*clientStateSnapshot]*encodedStateDelta)}
}

// encodedDelta returns the encoded rawData delta message taking previous to
// current, whether it is empty, and any error. The returned bytes are shared and
// must not be modified. A nil cache computes without sharing.
func (c *stateDeltaCache) encodedDelta(previous, current *clientStateSnapshot) ([]byte, bool, error) {
	if c == nil {
		delta := computeEncodedStateDelta(previous, current)
		stateDeltaDiffs.WithLabelValues("computed").Inc()
		return delta.data, delta.empty, delta.err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if delta, ok := c.byBaseline[previous]; ok {
		stateDeltaDiffs.WithLabelValues("shared").Inc()
		return delta.data, delta.empty, delta.err
	}
	delta := computeEncodedStateDelta(previous, current)
	c.byBaseline[previous] = delta
	stateDeltaDiffs.WithLabelValues("computed").Inc()
	return delta.data, delta.empty, delta.err
}

func computeEncodedStateDelta(previous, current *clientStateSnapshot) *encodedStateDelta {
	delta, err := buildClientStateDelta(previous, current)
	if err != nil {
		return &encodedStateDelta{err: err}
	}
	if len(delta) == 0 {
		return &encodedStateDelta{empty: true}
	}
	data, err := json.Marshal(Message{Type: "rawData", Data: delta})
	if err != nil {
		return &encodedStateDelta{err: err}
	}
	return &encodedStateDelta{data: data}
}
