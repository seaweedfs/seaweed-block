package component

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// transientApplyFailStore fails exactly one replica ApplyEntry once it is
// armed (a transient fault, like a full WAL that later drains) and records
// every call, so the test can see the failing LSN and where replay restarts.
type transientApplyFailStore struct {
	storage.LogicalStorage

	mu        sync.Mutex
	armed     bool
	failedLSN uint64
	calls     []applyCall
}

type applyCall struct {
	lsn    uint64
	failed bool
}

func (s *transientApplyFailStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	s.mu.Lock()
	if s.armed && s.failedLSN == 0 {
		s.failedLSN = lsn
		s.calls = append(s.calls, applyCall{lsn: lsn, failed: true})
		s.mu.Unlock()
		return errors.New("injected transient apply failure")
	}
	s.mu.Unlock()
	err := s.LogicalStorage.ApplyEntry(lba, data, lsn)
	s.mu.Lock()
	s.calls = append(s.calls, applyCall{lsn: lsn, failed: err != nil})
	s.mu.Unlock()
	return err
}

func (s *transientApplyFailStore) arm() {
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
}

func (s *transientApplyFailStore) snapshot() (uint64, []applyCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failedLSN, append([]applyCall(nil), s.calls...)
}

func waitPeerState(c *Cluster, want string, timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		if st := c.primary.RepVol.PeerStatuses(); len(st) == 1 {
			last = st[0].State
			if last == want {
				return last, true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return last, false
}

// #89 closed loop through the production primary path: live ship over the
// no-hook replica listener, a transient ApplyEntry failure on the replica, the
// primary's barrier marking the peer degraded, the probe loop recovering it
// from the replica's frontier, and the failed LSN replayed first with exact
// bytes, then a steady write and barrier on the recovered peer.
//
// Before the fix the replica skipped the failed LSN, applied the later ones
// and acked the barrier, so the peer was never degraded and the block stayed
// missing on the replica.
func TestReplicaApplyFailure_PrimaryDegradesRecoversAndReplaysFailedLSN(t *testing.T) {
	var replica *transientApplyFailStore
	factory := func(t *testing.T, dir, label string, blocks uint32, blockSize int) (storage.LogicalStorage, func()) {
		st, cleanup := Walstore(t, dir, label, blocks, blockSize)
		if label == "primary" {
			// This control requires WAL replay of the failed LSN. A primary
			// checkpoint may otherwise retire the WAL before recovery starts,
			// legitimately selecting BASE rebuild rather than ApplyEntry replay.
			st.(*storage.WALStore).DisableAutoFlushForRecoveryTest()
		}
		if label == "replica-0" {
			replica = &transientApplyFailStore{LogicalStorage: st}
			return replica, cleanup
		}
		return st, cleanup
	}
	c := NewCluster(t, factory).
		WithReplicas(1).
		WithBlockGeometry(64, 4096).
		WithLiveShip().
		Start()
	// Production composition (as cmd/blockvolume): a PeerAdapterRegistry
	// on the volume's peer lifecycle, so each peer's engine drives a
	// PeerCommandExecutor that rotates the live session and marks the peer
	// healthy when a recovery session completes.
	registry := volume.NewPeerAdapterRegistry("vol-component", "primary-component")
	t.Cleanup(registry.Close)
	if err := c.primary.RepVol.ConfigurePeerLifecycleHook(registry.OnPeerAdded, registry.OnPeerRemoved); err != nil {
		t.Fatalf("ConfigurePeerLifecycleHook: %v", err)
	}
	waitRegisteredIdle(t, registry)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		writeOneBlock(t, c, uint32(i), byte(0xA0+i))
	}
	_, _, head := c.primary.Store.Boundaries()
	if err := c.primary.RepVol.Sync(ctx, head); err != nil {
		t.Fatalf("baseline Sync: %v", err)
	}
	c.WaitForConverge(2 * time.Second)
	if st, ok := waitPeerState(c, "healthy", time.Second); !ok {
		t.Fatalf("baseline peer state %q, want healthy", st)
	}

	// The next replica apply fails once. Three writes ship behind it, then a
	// barrier for all of them.
	replica.arm()
	for i := 2; i < 5; i++ {
		writeOneBlock(t, c, uint32(i), byte(0xB0+i))
	}
	_, _, head = c.primary.Store.Boundaries()
	_ = c.primary.RepVol.Sync(ctx, head) // best effort: a failed barrier degrades the peer, Sync itself succeeds

	failedLSN, calls := replica.snapshot()
	if failedLSN == 0 {
		t.Fatalf("the injected failure never ran; replica calls %v", calls)
	}
	if st, ok := waitPeerState(c, "degraded", 2*time.Second); !ok {
		t.Fatalf("peer state %q after the failed apply of LSN %d, want degraded (calls %v)", st, failedLSN, calls)
	}
	_, _, rH := c.Replica(0).Store.Boundaries()
	if rH != failedLSN-1 {
		t.Fatalf("replica frontier H=%d before recovery, want %d (just below the failed LSN %d); calls %v", rH, failedLSN-1, failedLSN, calls)
	}
	t.Logf("injected failure at LSN %d; peer degraded; replica frontier before recovery H=%d; primary head %d", failedLSN, rH, head)

	if _, retained, _ := c.primary.Store.Boundaries(); retained > failedLSN {
		t.Fatalf("WAL replay fixture retired failed LSN %d (retained=%d)", failedLSN, retained)
	}
	cfg := replication.ProbeLoopConfig{Interval: 30 * time.Millisecond, MaxConcurrent: 1, CooldownBase: 20 * time.Millisecond, CooldownCap: 200 * time.Millisecond}
	if err := c.primary.RepVol.ConfigureProbeLoop(cfg, volume.ProductionProbeFnWithPrime(registry.AdapterForPeer, registry.PrimePendingFor), time.Now); err != nil {
		t.Fatalf("ConfigureProbeLoop: %v", err)
	}
	if err := c.primary.RepVol.StartProbeLoop(); err != nil {
		t.Fatalf("StartProbeLoop: %v", err)
	}
	c.WaitForConverge(8 * time.Second)
	c.AssertReplicaConverged(0)

	// Replay must restart exactly at the failed LSN (replica R+1): the first
	// call after the failure is that LSN and succeeds, and no later LSN is
	// applied before it.
	_, calls = replica.snapshot()
	at := -1
	for i, call := range calls {
		if call.failed && call.lsn == failedLSN {
			at = i
			break
		}
	}
	if at < 0 || at+1 >= len(calls) {
		t.Fatalf("no replay after the failed LSN %d: calls %v", failedLSN, calls)
	}
	if next := calls[at+1]; next.lsn != failedLSN || next.failed {
		t.Fatalf("first apply after the failure is %+v, want LSN %d succeeding; calls %v", next, failedLSN, calls)
	}
	t.Logf("replica ApplyEntry calls (lsn, failed): %v", calls)
	for lba := uint32(2); lba < 5; lba++ {
		want := bytes.Repeat([]byte{byte(0xB0 + lba)}, 4096)
		got, err := c.Replica(0).Store.Read(lba)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("replica LBA %d after recovery: err=%v, first byte %#x, want all %#x", lba, err, firstByte(got), byte(0xB0+lba))
		}
	}

	// Steady path after recovery: one more write and a barrier, peer healthy.
	if st, ok := waitPeerState(c, "healthy", 5*time.Second); !ok {
		t.Fatalf("peer state %q after recovery, want healthy", st)
	}
	writeOneBlock(t, c, 5, 0xC5)
	_, _, head = c.primary.Store.Boundaries()
	if err := c.primary.RepVol.Sync(ctx, head); err != nil {
		t.Fatalf("Sync after recovery: %v", err)
	}
	c.WaitForConverge(2 * time.Second)
	if st, ok := waitPeerState(c, "healthy", time.Second); !ok {
		t.Fatalf("peer state %q after the post-recovery write, want healthy", st)
	}
	if _, _, rH := c.Replica(0).Store.Boundaries(); rH != head {
		t.Fatalf("replica frontier H=%d after the post-recovery write, want %d", rH, head)
	}
}

// waitAdmitted waits for the registry adapter's assignment probe to succeed.
// waitRegisteredIdle waits for replica-0's registry adapter and checks it has
// issued nothing: a healthy peer belongs to the volume's live session, and
// its adapter is primed only when the probe loop first dispatches for it
// (once it is degraded).
func waitRegisteredIdle(t *testing.T, registry *volume.PeerAdapterRegistry) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a := registry.AdapterFor("replica-0"); a != nil {
			if cmds := a.CommandLog(); len(cmds) != 0 {
				t.Fatalf("replica-0's adapter acted on a healthy peer: %v", cmds)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("replica-0 has no registry adapter")
}

func firstByte(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}
