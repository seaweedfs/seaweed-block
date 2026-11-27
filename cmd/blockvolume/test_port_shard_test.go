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
