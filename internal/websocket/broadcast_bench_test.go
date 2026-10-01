package websocket

import (
	"fmt"
	"testing"

	"github.com/rcourtman/pulse-go-rewrite/internal/models"
	"github.com/rs/zerolog"
)

// benchmarkBroadcast runs the real once-per-broadcast path (snapshot, delta,
// queue) for a given number of connected clients on ~1,200 realistic resources
// whose live metrics change on every resource each broadcast. It excludes
// building the frontend state and the socket write.
func benchmarkBroadcast(b *testing.B, clients int) {
	defer zerolog.SetGlobalLevel(zerolog.GlobalLevel())
	zerolog.SetGlobalLevel(zerolog.Disabled)
	hub := NewHub(nil)
	states := [2]models.StateFrontend{realisticFrontendState(1220, 0), realisticFrontendState(1220, 1)}
	baseline, err := buildClientStateSnapshot(states[0])
	if err != nil {
		b.Fatal(err)
	}
	connected := make([]*Client, clients)
	hub.mu.Lock()
	for i := range connected {
		connected[i] = &Client{id: fmt.Sprintf("c%d", i), send: make(chan []byte, 4), stateSnapshot: baseline}
		hub.clients[connected[i]] = true
	}
	hub.mu.Unlock()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hub.dispatchCurrentStateSnapshot(states[(i+1)%2], "")
		for _, client := range connected {
			<-client.send
		}
	}
}

func BenchmarkBroadcast1Client(b *testing.B)  { benchmarkBroadcast(b, 1) }
func BenchmarkBroadcast2Clients(b *testing.B) { benchmarkBroadcast(b, 2) }
func BenchmarkBroadcast4Clients(b *testing.B) { benchmarkBroadcast(b, 4) }
