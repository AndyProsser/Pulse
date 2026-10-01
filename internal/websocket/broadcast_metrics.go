package websocket

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// State-broadcast instrumentation (#2199). The broadcast worker was the largest
// CPU consumer with a dashboard open, but nothing said how often it ran or
// where each run spent its time; these answer that from the metrics endpoint.
var (
	stateBroadcasts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_websocket_state_broadcasts_total",
		Help: "State broadcasts built and dispatched to at least one WebSocket client.",
	})
	stateBroadcastStageSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pulse_websocket_state_broadcast_stage_seconds",
		Help:    "Time spent per state broadcast by stage: frontend_state (building the state), snapshot (encoding and indexing it), deltas (diffing and queueing for every client).",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
	}, []string{"stage"})
	stateDeltaDiffs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_websocket_state_delta_diffs_total",
		Help: "Per-client state deltas by how they were produced: computed from scratch, or shared with another client on the same baseline.",
	}, []string{"result"})
	stateDeltaBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_websocket_state_delta_bytes_total",
		Help: "Bytes of state delta messages queued to WebSocket clients.",
	})
	stateSnapshotBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulse_websocket_state_snapshot_bytes",
		Help: "Encoded size of the most recent state snapshot.",
	})
)

func init() {
	// Make every series visible from the first scrape.
	stateDeltaDiffs.WithLabelValues("computed")
	stateDeltaDiffs.WithLabelValues("shared")
}
