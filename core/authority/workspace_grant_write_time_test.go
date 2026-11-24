package authority

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
)

// W, the store write a lease grant makes under the publisher lock, measured
// on a real disk: the master grants lapsed volumes one at a time after a
// restart, so the last of N is granted about T + 0.1 s + N x W after the
// barrier, and a volume whose grant comes more than 30 s after the barrier
// exits 75. Run with SWB_GRANT_WRITE_DIR set to a directory on the disk the
// authority store uses in production.
func TestWorkspaceGrantStoreWriteTime(t *testing.T) {
	dir := os.Getenv("SWB_GRANT_WRITE_DIR")
	if dir == "" {
		t.Skip("set SWB_GRANT_WRITE_DIR to a directory on the authority store's disk")
	}
	dir, err := os.MkdirTemp(dir, "grant-write-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	s, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	const n = 200
	now := time.Now()
	writes := make([]time.Duration, 0, 2*n)
	for round := 1; round <= 2; round++ { // a first grant, then a re-grant of each volume
		for i := 0; i < n; i++ {
			lease := &WorkspaceLease{ReplicaID: "r1", Epoch: 1, ExpiresAtNS: now.Add(45 * time.Second).UnixNano(), MaxClockSkewNS: int64(20 * time.Second)}
			r := DurableRecord{VolumeID: fmt.Sprintf("v%03d", i), ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
				DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100", WriteSeq: uint64(round),
				Workspace: &WorkspaceState{Lease: lease}}
			start := time.Now()
			if err := s.Put(r); err != nil {
				t.Fatal(err)
			}
			writes = append(writes, time.Since(start))
		}
	}
	sort.Slice(writes, func(i, j int) bool { return writes[i] < writes[j] })
	q := func(p float64) time.Duration { return writes[int(p*float64(len(writes)-1))] }
	p50, p99, worst := q(0.50), q(0.99), writes[len(writes)-1]
	t.Logf("grant store write W over %d writes in %s: p50 %v, p99 %v, max %v", len(writes), dir, p50, p99, worst)
	t.Logf("last of %d volumes granted about T + 0.1 s + %v (p50) / %v (p99); 75-and-restart from N >= %d (p50) / %d (p99)",
		n, time.Duration(n)*p50, time.Duration(n)*p99, int(30*time.Second/max(p50, time.Microsecond)), int(30*time.Second/max(p99, time.Microsecond)))
}
