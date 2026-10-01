package websocket

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func clientWithBaseline(t *testing.T, baseline *clientStateSnapshot) *Client {
	t.Helper()
	return &Client{send: make(chan []byte, 4), stateSnapshot: baseline}
}

func mustSnapshot(t *testing.T, tick int) *clientStateSnapshot {
	t.Helper()
	snapshot, err := buildClientStateSnapshot(realisticFrontendState(40, tick))
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func diffCount(result string) float64 {
	return testutil.ToFloat64(stateDeltaDiffs.WithLabelValues(result))
}

func TestClientsSharingABaselineShareOneDelta(t *testing.T) {
	baseline, current := mustSnapshot(t, 0), mustSnapshot(t, 1)
	clients := []*Client{
		clientWithBaseline(t, baseline), clientWithBaseline(t, baseline), clientWithBaseline(t, baseline),
	}
	computedBefore, sharedBefore := diffCount("computed"), diffCount("shared")

	cache := newStateDeltaCache()
	for _, client := range clients {
		if _, attempted, sent, err := client.queueStateDeltaShared(current, cache); err != nil || !attempted || !sent {
			t.Fatalf("queueStateDeltaShared() attempted=%v sent=%v error=%v", attempted, sent, err)
		}
	}

	if got := diffCount("computed") - computedBefore; got != 1 {
		t.Fatalf("delta computed %v times for 3 clients on one baseline, want 1", got)
	}
	if got := diffCount("shared") - sharedBefore; got != 2 {
		t.Fatalf("delta shared %v times, want 2", got)
	}
	first := <-clients[0].send
	for i, client := range clients {
		if i > 0 {
			if other := <-client.send; !bytes.Equal(other, first) {
				t.Fatalf("client %d received different delta bytes", i)
			}
		}
		if client.stateSnapshot != current {
			t.Fatalf("client %d baseline did not advance to the current snapshot", i)
		}
	}
}

func TestClientsOnDifferentBaselinesGetTheirOwnDelta(t *testing.T) {
	older, newer, current := mustSnapshot(t, 0), mustSnapshot(t, 1), mustSnapshot(t, 2)
	onOlder, onNewer := clientWithBaseline(t, older), clientWithBaseline(t, newer)
	computedBefore := diffCount("computed")

	cache := newStateDeltaCache()
	for _, client := range []*Client{onOlder, onNewer} {
		if _, _, sent, err := client.queueStateDeltaShared(current, cache); err != nil || !sent {
			t.Fatalf("queueStateDeltaShared() sent=%v error=%v", sent, err)
		}
	}
	if got := diffCount("computed") - computedBefore; got != 2 {
		t.Fatalf("computed %v deltas for two different baselines, want 2", got)
	}

	// Each delta must equal what the unshared path produces for that baseline.
	for name, tc := range map[string]struct {
		baseline *clientStateSnapshot
		client   *Client
	}{"older": {older, onOlder}, "newer": {newer, onNewer}} {
		reference := clientWithBaseline(t, tc.baseline)
		if _, _, sent, err := reference.queueStateDelta(current); err != nil || !sent {
			t.Fatalf("%s: reference queueStateDelta() sent=%v error=%v", name, sent, err)
		}
		if want, got := <-reference.send, <-tc.client.send; !bytes.Equal(want, got) {
			t.Fatalf("%s: shared-path delta differs from the unshared path", name)
		}
	}
}

func TestUnchangedStateSharesAnEmptyDelta(t *testing.T) {
	baseline := mustSnapshot(t, 0)
	same := mustSnapshot(t, 0)
	a, b := clientWithBaseline(t, baseline), clientWithBaseline(t, baseline)
	cache := newStateDeltaCache()
	for _, client := range []*Client{a, b} {
		_, attempted, sent, err := client.queueStateDeltaShared(same, cache)
		if err != nil || attempted || !sent {
			t.Fatalf("unchanged state: attempted=%v sent=%v error=%v", attempted, sent, err)
		}
		if len(client.send) != 0 {
			t.Fatal("unchanged state must not send anything")
		}
		if client.stateSnapshot != same {
			t.Fatal("unchanged state must still advance the baseline")
		}
	}
}

func TestFullSendChannelDoesNotPoisonTheSharedDelta(t *testing.T) {
	baseline, current := mustSnapshot(t, 0), mustSnapshot(t, 1)
	full := clientWithBaseline(t, baseline)
	full.send = make(chan []byte) // unbuffered, nobody reading: always full
	healthy := clientWithBaseline(t, baseline)

	cache := newStateDeltaCache()
	if _, attempted, sent, err := full.queueStateDeltaShared(current, cache); err != nil || !attempted || sent {
		t.Fatalf("full client: attempted=%v sent=%v error=%v", attempted, sent, err)
	}
	if full.stateSnapshot != baseline {
		t.Fatal("a client whose delta was not delivered must keep its old baseline")
	}
	if _, _, sent, err := healthy.queueStateDeltaShared(current, cache); err != nil || !sent {
		t.Fatalf("healthy client: sent=%v error=%v", sent, err)
	}
}

func TestDispatchRecordsBroadcastMetrics(t *testing.T) {
	hub := NewHub(nil)
	baseline := mustSnapshot(t, 0)
	client := clientWithBaseline(t, baseline)
	hub.mu.Lock()
	hub.clients[client] = true
	hub.mu.Unlock()

	broadcastsBefore := testutil.ToFloat64(stateBroadcasts)
	bytesBefore := testutil.ToFloat64(stateDeltaBytes)

	hub.dispatchCurrentStateSnapshot(realisticFrontendState(40, 1), "")

	if got := testutil.ToFloat64(stateBroadcasts) - broadcastsBefore; got != 1 {
		t.Fatalf("broadcast counter advanced by %v, want 1", got)
	}
	sent := <-client.send
	if got := testutil.ToFloat64(stateDeltaBytes) - bytesBefore; got != float64(len(sent)) {
		t.Fatalf("delta bytes counter advanced by %v, want %d", got, len(sent))
	}
	if testutil.ToFloat64(stateSnapshotBytes) <= 0 {
		t.Fatal("snapshot size gauge was not set")
	}
}
