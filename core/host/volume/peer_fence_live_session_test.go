package volume

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// A replica that is already at the primary's end (R = H) is fenced at the
// peer's epoch with a session the fence mints (as the adapter's
// FenceAtEpoch does, from the peer's one session order), above the live
// session the peer drew at its creation. The replica then holds the
// fence's lineage. The engine's healthy verdict sends its barrier on the
// live session: it must be acknowledged and the peer healed, so the live
// session must be re-drawn above the fence's once the fence completes.
// Without that the replica refuses the barrier as stale, forever.
func TestAFencedCaughtUpPeerIsHealedOnItsLiveSession(t *testing.T) {
	for _, fence := range []bool{true, false} {
		name := map[bool]string{true: "fenced first", false: "control: no fence"}[fence]
		t.Run(name, func(t *testing.T) {
			peer, exec := fencePeerRig(t)
			if fence {
				done := make(chan adapter.FenceResult, 1)
				exec.SetOnFenceComplete(func(r adapter.FenceResult) { done <- r })
				sid := exec.SessionIDs().Next() // the adapter's FenceAtEpoch mint
				if err := exec.Fence("r2", sid, 2, 1); err != nil {
					t.Fatal(err)
				}
				select {
				case r := <-done:
					if !r.Success {
						t.Fatalf("fence failed: %s", r.FailReason)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("the fence did not complete")
				}
			}
			exec.PublishHealthy("r2")
			if s := peer.State(); s != replication.ReplicaHealthy {
				t.Fatalf("peer %s after the healthy verdict; want healthy (its barrier on the live session must be acknowledged)", s)
			}
		})
	}
}

// fencePeerRig is a primary volume with one degraded peer r2 at epoch 2
// whose replica (a real listener) is already at the primary's end.
func fencePeerRig(t *testing.T) (*replication.ReplicaPeer, *PeerCommandExecutor) {
	t.Helper()
	const blockSize, n = 4096, 4
	primary := storage.NewBlockStore(64, blockSize)
	replica := storage.NewBlockStore(64, blockSize)
	for i := 0; i < n; i++ {
		data := make([]byte, blockSize)
		data[0] = byte(i + 1)
		lsn, err := primary.Write(uint32(i), data)
		if err != nil {
			t.Fatal(err)
		}
		if err := replica.ApplyEntry(uint32(i), data, lsn); err != nil {
			t.Fatal(err)
		}
	}
	own, _ := transport.LoadEpochTable("", "vol1")
	_ = own.Replace([]transport.EpochRow{{Epoch: 2, FirstLSN: 1}})
	theirs, _ := transport.LoadEpochTable("", "vol1")
	_ = theirs.Replace([]transport.EpochRow{{Epoch: 2, FirstLSN: 1}})

	// Nothing listens yet: the seed barrier fails and the peer is
	// degraded (the rejoining replica is not up).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	v := replication.NewReplicationVolume("vol1", primary)
	v.SetDurabilityMode(replication.DurabilitySyncQuorum)
	v.SetEpochTable(own)
	t.Cleanup(func() { _ = v.Close() })
	var peer *replication.ReplicaPeer
	if err := v.ConfigurePeerLifecycleHook(func(p *replication.ReplicaPeer) { peer = p }, func(string) {}); err != nil {
		t.Fatal(err)
	}
	if err := v.UpdateReplicaSet(1, []replication.ReplicaTarget{{ReplicaID: "r2", DataAddr: addr, ControlAddr: addr, Epoch: 2, EndpointVersion: 1}}); err != nil {
		t.Fatal(err)
	}
	if peer == nil || peer.State() != replication.ReplicaDegraded {
		t.Fatalf("peer %v; want degraded after an unanswered seed barrier", peer)
	}

	listener, err := transport.NewReplicaListener(addr, replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.SetEpochTable(theirs)
	listener.Serve()
	t.Cleanup(func() { _ = v.Close(); listener.Stop() })

	return peer, NewPeerCommandExecutor(peer)
}

// The order of the re-draw: the live session moves above the fence's only
// after the fence's own barrier is acknowledged. Moved before (here: the
// fence's session drawn, the live one re-drawn above it, then the fence
// sent), the replica holds the higher live lineage and refuses the fence.
func TestTheLiveSessionMovesOnlyAfterTheFenceIsAcknowledged(t *testing.T) {
	peer, exec := fencePeerRig(t)
	done := make(chan adapter.FenceResult, 1)
	exec.SetOnFenceComplete(func(r adapter.FenceResult) { done <- r })
	sid := exec.SessionIDs().Next()
	if err := peer.RefreshLiveShipSessionAfter(sid, 0, "re-drawn too early"); err != nil {
		t.Fatal(err)
	}
	exec.PublishHealthy("r2") // the replica now holds the live lineage above sid
	if s := peer.State(); s != replication.ReplicaHealthy {
		t.Fatalf("fixture: peer %s; want healthy on the re-drawn live session", s)
	}
	if err := exec.Fence("r2", sid, 2, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.Success {
			t.Fatal("a fence below the replica's held live lineage was acknowledged; want it refused (which is why the re-draw waits for the fence's ack)")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the fence did not complete")
	}
}

// The lost acknowledgement: the replica admits a session (it holds that
// lineage from then on) but the sender sees the exchange fail, its reply
// lost. The live session is re-drawn above it all the same, so the next
// live frame is not refused as stale. Both cases are driven here: a fence
// the replica admitted whose result is a failure, and a recovery session
// (standing in: the replica holds its lineage) whose close failed.
func TestAnAdmittedSessionWhoseReplyIsLostStillMovesTheLiveSession(t *testing.T) {
	for _, kind := range []string{"fence", "recovery session"} {
		t.Run(kind, func(t *testing.T) {
			peer, exec := fencePeerRig(t)
			sid := exec.SessionIDs().Next()
			// The replica admits a barrier at sid: it now holds sid's lineage.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := peer.Fence(ctx, transport.RecoveryLineage{SessionID: sid, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}); err != nil {
				t.Fatalf("fixture: the replica did not admit the session: %v", err)
			}
			// The sender saw it fail (the reply lost).
			if kind == "fence" {
				exec.afterFence(adapter.FenceResult{ReplicaID: "r2", SessionID: sid, Epoch: 2, EndpointVersion: 1, Success: false, FailReason: "reply lost"})
			} else {
				exec.afterSessionClose(adapter.SessionCloseResult{ReplicaID: "r2", SessionID: sid, Success: false, FailReason: "final ack lost"})
			}
			exec.PublishHealthy("r2")
			if s := peer.State(); s != replication.ReplicaHealthy {
				t.Fatalf("peer %s after the healthy verdict; want healthy: the live session must be above the admitted %d", s, sid)
			}
		})
	}
}

// Every command kind for which the adapter draws a session from the peer's
// order is listed with what keeps the live session above it: recovery
// sessions re-draw at their close and fences at their completion, both on
// any outcome; a probe does not move the replica's held lineage. A new
// drawing kind fails here until it is classified.
func TestEverySessionDrawIsFollowedByALiveRedraw(t *testing.T) {
	want := map[string]string{
		"StartCatchUp":  "recovery session: afterSessionClose re-draws",
		"StartRebuild":  "recovery session: afterSessionClose re-draws",
		"StartRecovery": "recovery session: afterSessionClose re-draws",
		"FenceAtEpoch":  "fence: afterFence re-draws",
		"ProbeReplica":  "non-moving: the replica validates a probe without taking its lineage",
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join("..", "..", "adapter", "adapter.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}
		draws := false
		for _, st := range cc.Body {
			ast.Inspect(st, func(m ast.Node) bool {
				if call, ok := m.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Next" || sel.Sel.Name == "NextAfter") {
						if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "sessionIDs" {
							draws = true
						}
					}
				}
				return true
			})
		}
		if !draws {
			return true
		}
		for _, e := range cc.List {
			if sel, ok := e.(*ast.SelectorExpr); ok {
				got[sel.Sel.Name] = true
			}
		}
		return true
	})
	for kind := range got {
		if _, ok := want[kind]; !ok {
			t.Errorf("the adapter draws a session for %s, which is not classified: re-draw the live session above it, or declare it non-moving", kind)
		}
	}
	for kind, why := range want {
		if !got[kind] {
			t.Errorf("listed drawing kind %s (%s) no longer draws a session; update the list", kind, why)
		}
	}
}
