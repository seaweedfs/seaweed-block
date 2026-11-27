package main_test

import (
	"os"
	"strconv"
	"testing"
)

// testPortSpan narrows a port range [lo, hi) to this test process's share
// when several process-level test binaries run in parallel on one host
// (CI shards): SWBLOCK_TEST_PORT_SHARD=k of SWBLOCK_TEST_PORT_SHARDS=n
// (1 <= k <= n) gets the k-th of n equal slices, so two shards never draw
// the same port. Unset or invalid, the whole range.
func testPortSpan(lo, hi int) (int, int) {
	n, errN := strconv.Atoi(os.Getenv("SWBLOCK_TEST_PORT_SHARDS"))
	k, errK := strconv.Atoi(os.Getenv("SWBLOCK_TEST_PORT_SHARD"))
	if errN != nil || errK != nil || n < 2 || k < 1 || k > n || hi-lo < n {
		return lo, hi
	}
	w := (hi - lo) / n
	return lo + (k-1)*w, lo + k*w
}

// weedPairRange is the range [20000, hi) of a weed server's HTTP port p,
// whose gRPC port is p+10000, given the host's ephemeral range starting at
// ephemeralLo: every gRPC port stays below the ephemeral range when that
// leaves room, and every HTTP port stays below every gRPC port (hi <= 30000),
// so slicing the HTTP range per shard keeps both kinds of port apart.
func weedPairRange(ephemeralLo int) (int, int) {
	hi := 30000
	if ephemeralLo-10000 > 20000 && ephemeralLo-10000 < hi {
		hi = ephemeralLo - 10000
	}
	return 20000, hi
}

func TestWeedPairSlicesNeverMeetAcrossShards(t *testing.T) {
	t.Setenv("SWBLOCK_TEST_PORT_SHARDS", "4")
	for _, eph := range []int{28000, 32768, 40000, 50000, 61000} {
		type span struct{ k, lo, hi int }
		var spans []span
		for k := 1; k <= 4; k++ {
			t.Setenv("SWBLOCK_TEST_PORT_SHARD", strconv.Itoa(k))
			lo, hi := testPortSpan(weedPairRange(eph))
			spans = append(spans, span{k, lo, hi}, span{k, lo + 10000, hi + 10000}) // HTTP, gRPC
		}
		for i, a := range spans {
			for _, b := range spans[i+1:] {
				if a.k != b.k && a.lo < b.hi && b.lo < a.hi {
					t.Fatalf("ephemeral from %d: shard %d [%d, %d) meets shard %d [%d, %d)", eph, a.k, a.lo, a.hi, b.k, b.lo, b.hi)
				}
			}
		}
	}
}

func TestTestPortSpanGivesEachShardItsOwnSlice(t *testing.T) {
	t.Setenv("SWBLOCK_TEST_PORT_SHARDS", "")
	if lo, hi := testPortSpan(10000, 20000); lo != 10000 || hi != 20000 {
		t.Fatalf("unsharded: [%d, %d)", lo, hi)
	}
	t.Setenv("SWBLOCK_TEST_PORT_SHARDS", "4")
	prev := 10000
	for k := 1; k <= 4; k++ {
		t.Setenv("SWBLOCK_TEST_PORT_SHARD", strconv.Itoa(k))
		lo, hi := testPortSpan(10000, 20000)
		if lo != prev || hi-lo != 2500 {
			t.Fatalf("shard %d of 4: [%d, %d); want [%d, %d)", k, lo, hi, prev, prev+2500)
		}
		prev = hi
	}
	for _, bad := range []string{"0", "5", "x"} {
		t.Setenv("SWBLOCK_TEST_PORT_SHARD", bad)
		if lo, hi := testPortSpan(10000, 20000); lo != 10000 || hi != 20000 {
			t.Fatalf("shard %q of 4: [%d, %d); want the whole range", bad, lo, hi)
		}
	}
}
