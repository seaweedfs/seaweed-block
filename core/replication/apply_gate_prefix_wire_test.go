package replication

import (
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

type prefixResult struct {
	lineage transport.RecoveryLineage
	err     error
}

type observedPrefixGate struct {
	*ReplicaApplyGate
	certified chan prefixResult
}

func (g *observedPrefixGate) CertifyRecoveryPrefix(l transport.RecoveryLineage) error {
	err := g.ReplicaApplyGate.CertifyRecoveryPrefix(l)
	g.certified <- prefixResult{l, err}
	return err
}

// Real primary engine/adapter/executor -> role-gated replica -> shared gate.
func TestApplyGate_HealthyRecoveryPublishesWirePrefix(t *testing.T) {
	primary := storage.NewBlockStore(64, blockSize)
	for lba := uint32(0); lba < 32; lba++ {
		if _, err := primary.Write(lba, makeData(byte(lba+1))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	store := storage.NewBlockStore(64, blockSize)
	gate := &observedPrefixGate{ReplicaApplyGate: NewReplicaApplyGate(store), certified: make(chan prefixResult, 8)}
	if err := gate.ApplyLive(liveLineage(1), 0, makeData(1), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	listener, err := transport.NewReplicaListenerWithFloorAndApplyHook("127.0.0.1:0", store, "v1", "", gate)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.AdmitAsReplica(1); err != nil {
		listener.StopHard()
		t.Fatal(err)
	}
	listener.Serve()
	executor := transport.NewBlockExecutor(primary, listener.Addr())
	t.Cleanup(func() { executor.Stop(); listener.StopHard() })
	a := adapter.NewVolumeReplicaAdapter(executor)
	a.OnAssignment(adapter.AssignmentInfo{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, DataAddr: listener.Addr()})
	var certified transport.RecoveryLineage
	select {
	case result := <-gate.certified:
		if result.err != nil {
			t.Fatal(result.err)
		}
		certified = result.lineage
	case <-time.After(5 * time.Second):
		t.Fatal("healthy recovery did not publish wire prefix certificate")
	}
	if got := gate.CertifiedPrefix(); got != 32 {
		t.Fatalf("wire certified prefix=%d want32", got)
	}
	tracked, _, _, found := gate.SessionState(certified.SessionID)
	if !found || tracked != 0 {
		t.Fatalf("wire recovery did not prune: found=%v tracked=%d", found, tracked)
	}
}

func TestApplyGate_WirePrefixCannotIntroduceOrReviveSession(t *testing.T) {
	gate, store := newGate(t)
	if err := gate.ApplyRecovery(recoveryLineage(1, 1), 0, makeData(1), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	listener, err := transport.NewReplicaListenerWithFloorAndApplyHook("127.0.0.1:0", store, "v1", "", gate)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.AdmitAsReplica(1); err != nil {
		listener.StopHard()
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.StopHard)
	send := func(l transport.RecoveryLineage, certificate bool) {
		t.Helper()
		c, err := net.DialTimeout("tcp", listener.Addr(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if certificate {
			if err := transport.WriteMsg(c, transport.MsgCertifiedPrefix, transport.EncodeLineage(l)); err != nil {
				t.Fatal(err)
			}
		}
		if err := transport.WriteMsg(c, transport.MsgBarrierReq, transport.EncodeLineage(l)); err != nil {
			if certificate {
				return
			}
			t.Fatal(err)
		}
		kind, _, err := transport.ReadMsg(c)
		if !certificate && (err != nil || kind != transport.MsgBarrierResp) {
			t.Fatalf("setup barrier: kind=%d err=%v", kind, err)
		}
	}
	send(recoveryLineage(1, 1), true) // unseen: rejected, must not introduce lineage
	if gate.CertifiedPrefix() != 0 {
		t.Fatal("unseen session certified prefix")
	}
	send(recoveryLineage(2, 1), false)
	send(recoveryLineage(1, 1), true) // superseded: cannot certify even with valid R
	if gate.CertifiedPrefix() != 0 {
		t.Fatal("stale session certified prefix")
	}
}
