//go:build race

package websocket

// The race detector makes sync.Pool drop items at random and inflates
// allocation counts, so allocation-measurement tests skip themselves under it.
const raceEnabled = true
