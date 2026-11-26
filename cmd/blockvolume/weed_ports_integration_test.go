//go:build subprocess && chunkstore_integration

package main_test

import "testing"

// TestWeedPortPairsAvoidTheEphemeralRange: no port of a weed port pair lies in
// the host's range for outgoing connections' source ports, where any
// connection could take it between w4FreePortPair's free check and weed's bind
// (a filer's gRPC port inside a host's 43255-47350 failed to bind that way).
func TestWeedPortPairsAvoidTheEphemeralRange(t *testing.T) {
	lo, hi := ephemeralPorts()
	used := map[int]bool{}
	for i := 0; i < 100; i++ {
		p := w4FreePortPair(t, used)
		for _, q := range []int{p, p + 10000} {
			if q >= lo && q <= hi {
				t.Fatalf("pair %d/%d: port %d is in the ephemeral range %d-%d", p, p+10000, q, lo, hi)
			}
		}
	}
}
