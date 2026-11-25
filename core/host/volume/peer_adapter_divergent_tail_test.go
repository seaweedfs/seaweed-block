package volume

import (
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// The registry hands each added peer's engine this primary's frontier at the
// add as its start position, so a probe that finds the peer above it pins a
// rebuild instead of fencing it into healthy on length.
func TestPeerAdapterRegistry_DivergentTailAboveTheFrontierAtAddIsRebuilt(t *testing.T) {
	for _, c := range []struct {
		name     string
		frontier func() (uint64, error)
		want     string
	}{
		{"frontier known", func() (uint64, error) { return 90, nil }, "StartRebuild"},
		{"frontier unknown", func() (uint64, error) { return 0, errors.New("sync failed") }, "FenceAtEpoch"},
	} {
		t.Run(c.name, func(t *testing.T) {
			reg := NewPeerAdapterRegistry("vol1", "r1")
			reg.SetFrontier(c.frontier)
			reg.OnPeerAdded(newPeerForRegistryTest(t, "r2", 2, 1))
			// The frontier was read at the add; the probe loop primes the
			// adapter when the peer is first degraded.
			if !reg.PrimePending("r2") {
				t.Fatal("r2 was not pending")
			}
			adpt := reg.AdapterFor("r2")
			if adpt == nil {
				t.Fatal("no adapter for r2")
			}
			adpt.OnProbeResult(adapter.ProbeResult{
				ReplicaID: "r2", Success: true, EndpointVersion: 1, TransportEpoch: 2,
				ReplicaFlushedLSN: 100, PrimaryTailLSN: 10, PrimaryHeadLSN: 95,
			})
			if log := strings.Join(adpt.CommandLog(), " "); !strings.Contains(log, c.want) {
				t.Fatalf("commands %q; want %s", log, c.want)
			}
		})
	}
}
