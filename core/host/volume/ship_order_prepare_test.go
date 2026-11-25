package volume

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// shipRebaser is the replication slot as primary adoption sees it: it
// counts the moves of its ship order to the store and fails the first fail
// of them.
type shipRebaser struct {
	calls int
	fail  int
}

func (r *shipRebaser) UpdateReplicaSet(uint64, []replication.ReplicaTarget) error { return nil }
func (r *shipRebaser) RebaseShipToStore() error {
	r.calls++
	if r.fail > 0 {
		r.fail--
		return errors.New("a write is pending")
	}
	return nil
}

type roleListener struct{}

func (roleListener) FenceAsPrimary(uint64) error { return nil }
func (roleListener) AdmitAsReplica(uint64) error { return nil }

func shipHost(t *testing.T, fail int) (*Host, *shipRebaser) {
	t.Helper()
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "s1", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19403", CtrlAddr: "127.0.0.1:19404"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if err := h.SetSelfFence(roleListener{}); err != nil {
		t.Fatal(err)
	}
	r := &shipRebaser{fail: fail}
	h.replicationMu.Lock()
	h.replication = r
	h.replicationMu.Unlock()
	return h, r
}

func primaryLine(epoch uint64) *control.AssignmentFact {
	return &control.AssignmentFact{VolumeId: "v1", ReplicaId: "r1", Epoch: epoch, EndpointVersion: 1, DataAddr: "127.0.0.1:19403", CtrlAddr: "127.0.0.1:19404", ServerId: "s1"}
}

// The ship order is moved to the store whenever this host takes the
// primary role without having moved it since its last replica role: not
// skipped because a primary fence ran first (the excluded-replica path, or a
// fence before a refused move), and not repeated for a primary continued.
func TestPrimaryAdoptionMovesTheShipOrderUntilItSucceeded(t *testing.T) {
	t.Run("excluded, then primary", func(t *testing.T) {
		h, r := shipHost(t, 0)
		// A line naming another replica and not this one: the excluded path
		// fences the listener as primary only to close ingress.
		h.applyFact(&control.AssignmentFact{VolumeId: "v1", ReplicaId: "r2", Epoch: 2, EndpointVersion: 1, DataAddr: "127.0.0.1:19503", CtrlAddr: "127.0.0.1:19504"})
		h.applyFact(primaryLine(3))
		if r.calls != 1 {
			t.Fatalf("ship order moved %d times on taking the primary role after an excluded fence; want 1", r.calls)
		}
	})
	t.Run("a refused move, then the same line replayed", func(t *testing.T) {
		h, r := shipHost(t, 1)
		h.applyFact(primaryLine(3))
		h.applyFact(primaryLine(3))
		if r.calls != 2 {
			t.Fatalf("ship order move attempts %d; want the replay to try again (2)", r.calls)
		}
	})
	t.Run("a primary continued at the next epoch", func(t *testing.T) {
		h, r := shipHost(t, 0)
		h.applyFact(primaryLine(3))
		h.applyFact(primaryLine(4))
		if r.calls != 1 {
			t.Fatalf("ship order moved %d times for a primary continued; want 1 (its order is its own)", r.calls)
		}
	})
}
