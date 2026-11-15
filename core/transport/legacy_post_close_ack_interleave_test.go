package transport

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// closeHookExecutor runs a hook right after the adapter's close callback
// returns, i.e. between the session close and the post-close durable
// ack that finishSession sends next.
type closeHookExecutor struct {
	*BlockExecutor
	afterClose func(adapter.SessionCloseResult)
}

func (x *closeHookExecutor) SetOnSessionClose(fn adapter.OnSessionClose) {
	x.BlockExecutor.SetOnSessionClose(func(r adapter.SessionCloseResult) {
		fn(r)
		if x.afterClose != nil {
			x.afterClose(r)
		}
	})
}

// An identity change between the close and its post-close ack: the ack
// carries the session's lineage, so the engine refuses it as stale
// instead of taking it as progress at the new identity.
func TestLegacyPostCloseAck_IdentityChangeBetweenCallbacksIsStale(t *testing.T) {
	primary, _, listener := setupPrimaryReplica(t)
	writeTestBlocks(primary, 5)
	x := &closeHookExecutor{BlockExecutor: NewBlockExecutor(primary, listener.Addr())}

	var a *adapter.VolumeReplicaAdapter
	var once sync.Once
	moved := make(chan struct{})
	x.afterClose = func(r adapter.SessionCloseResult) {
		if !r.Success {
			return
		}
		once.Do(func() {
			a.OnAssignment(adapter.AssignmentInfo{
				VolumeID: "v1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 2,
				DataAddr: listener.Addr(), CtrlAddr: listener.Addr(),
			})
			close(moved)
		})
	}
	a = adapter.NewVolumeReplicaAdapter(x)
	a.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: listener.Addr(), CtrlAddr: listener.Addr(),
	})

	select {
	case <-moved:
	case <-time.After(10 * time.Second):
		t.Fatalf("no successful recovery close; commands=%v trace=%v", a.CommandLog(), a.Trace())
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, te := range a.Trace() {
			switch te.Step {
			case "stale_durable_ack":
				return
			case "durable_ack_observed", "post_close_durable_ack":
				t.Fatalf("post-close ack from the epoch-1 session was taken at the new identity: %s %s", te.Step, te.Detail)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no stale_durable_ack after the identity change; trace=%v", a.Trace())
}
