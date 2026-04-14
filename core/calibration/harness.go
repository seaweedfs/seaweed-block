package calibration

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// harness holds the shared scaffolding each scenario builds on top of:
// a primary store, a replica store, a replica listener, a block executor
// wired to the listener, and a V3 adapter wrapped around that executor.
//
// This is the SAME scaffolding the sparrow demo uses — scenarios do not
// short-circuit to the engine directly. That preserves the "accepted
// route" guarantee Phase 06 promises.
type harness struct {
	primaryStore *storage.BlockStore
	replicaStore *storage.BlockStore
	listener     *transport.ReplicaListener
	executor     *transport.BlockExecutor
	adapter      *adapter.VolumeReplicaAdapter
}

func newHarness() (*harness, error) {
	primary := storage.NewBlockStore(256, 4096)
	replica := storage.NewBlockStore(256, 4096)

	ln, err := transport.NewReplicaListener("127.0.0.1:0", replica)
	if err != nil {
		return nil, fmt.Errorf("replica listener: %w", err)
	}
	ln.Serve()
	exec := transport.NewBlockExecutor(primary, ln.Addr(), 1)
	a := adapter.NewVolumeReplicaAdapter(exec)
	return &harness{
		primaryStore: primary,
		replicaStore: replica,
		listener:     ln,
		executor:     exec,
		adapter:      a,
	}, nil
}

// close tears down the transport. Safe to call in a defer.
func (h *harness) close() {
	if h.listener != nil {
		h.listener.Stop()
	}
}

// writeBlocks writes N blocks to the primary with deterministic content
// (LBA i gets data[0]=byte(i+1)). Used by C2/C3/C5 setups.
func (h *harness) writeBlocks(n uint32) {
	for i := uint32(0); i < n; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		h.primaryStore.Write(i, data)
	}
	h.primaryStore.Sync()
}

// mirrorToReplica copies the primary's current blocks into the replica
// and marks the replica's synced frontier at the primary's head. Used
// by C1 setup where the replica is already caught up.
func (h *harness) mirrorToReplica() {
	blocks := h.primaryStore.AllBlocks()
	_, _, pH := h.primaryStore.Boundaries()
	for lba, data := range blocks {
		_ = h.replicaStore.ApplyEntry(lba, data, pH)
	}
	h.replicaStore.Sync()
}

// advanceWALPastReplica moves the primary's WAL tail past the replica's
// frontier so the engine classifies the gap as long (R < S) and chooses
// rebuild. Used by C3 setup.
func (h *harness) advanceWALPastReplica() {
	h.primaryStore.AdvanceWALTail(h.primaryStore.NextLSN())
}

// assign sends a fresh AssignmentInfo through the adapter. Triggers the
// auto-probe.
func (h *harness) assign(epoch, endpointVersion uint64) {
	h.adapter.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: epoch, EndpointVersion: endpointVersion,
		DataAddr: h.listener.Addr(), CtrlAddr: h.listener.Addr(),
	})
}

// waitForDecisionFinal polls the adapter's projection until:
//   - mode becomes healthy (success path), OR
//   - a timeout elapses (evidence of a stuck decision).
//
// Returns the final projection regardless of outcome so the caller
// can report observed state honestly even on timeout.
func (h *harness) waitForDecisionFinal(timeout time.Duration) engine.ReplicaProjection {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p := h.adapter.Projection()
		if p.Mode == engine.ModeHealthy {
			return p
		}
		time.Sleep(20 * time.Millisecond)
	}
	return h.adapter.Projection()
}

// commandLog returns the adapter's accumulated command log as a slice of
// command-kind strings. This is the ground truth for command-path
// assertions — it contains what actually ran, not what we planned.
func (h *harness) commandLog() []string {
	return h.adapter.CommandLog()
}

// dataMatches returns true iff every LBA that exists on the primary is
// byte-identical on the replica. Used for C2/C3 data-integrity evidence.
func (h *harness) dataMatches() bool {
	primary := h.primaryStore.AllBlocks()
	for lba, want := range primary {
		got, err := h.replicaStore.Read(lba)
		if err != nil {
			return false
		}
		if len(got) != len(want) {
			return false
		}
		for i := range want {
			if got[i] != want[i] {
				return false
			}
		}
	}
	return true
}
