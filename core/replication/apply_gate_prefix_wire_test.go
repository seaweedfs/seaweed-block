package replication

import (
	"bytes"
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
	t.Run("catchup_from_certified_base", func(t *testing.T) { runHealthyRecoveryPrefix(t, true) })
	t.Run("full_base", func(t *testing.T) { runHealthyRecoveryPrefix(t, false) })
}
func runHealthyRecoveryPrefix(t *testing.T, seeded bool) {
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
	if seeded {
		if err := gate.ApplyLive(liveLineage(1), 0, makeData(1), 1); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Sync(); err != nil {
			t.Fatal(err)
		}
		// This initial BASE is known complete; Sync alone must not establish it.
		if err := gate.ReplicaApplyGate.CertifyRecoveryPrefix(recoveryLineage(1, 1)); err != nil {
			t.Fatal(err)
		}
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
	if (seeded && !found) || tracked != 0 {
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

// A clean recovery close cannot certify a pre-existing hole in its starting R.
func TestApplyGate_WirePrefixRequiresCoveredStartingImage(t *testing.T) {
	gate, store := newGate(t)
	if err := gate.ApplyLive(liveLineage(1), 5, makeData(0xBB), 120); err != nil {
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
	lineage := recoveryLineage(2, 120)
	dial := func() net.Conn {
		t.Helper()
		c, err := net.DialTimeout("tcp", listener.Addr(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return c
	}
	barrier := func(c net.Conn) error {
		if err := transport.WriteMsg(c, transport.MsgBarrierReq, transport.EncodeLineage(lineage)); err != nil {
			return err
		}
		_, _, err := transport.ReadMsg(c)
		return err
	}
	c := dial()
	if err := transport.WriteMsg(c, transport.MsgRecoveryLaneStart, transport.EncodeRecoveryLaneStart(lineage, 0)); err != nil {
		t.Fatal(err)
	}
	if err := barrier(c); err != nil {
		t.Fatal(err)
	}
	if err := transport.WriteMsg(c, transport.MsgCertifiedPrefix, transport.EncodeLineage(lineage)); err != nil {
		t.Fatal(err)
	}
	_ = barrier(c) // certificate refusal closes this connection
	if got := gate.CertifiedPrefix(); got != 0 {
		t.Fatalf("uncertified starting R advanced P=%d", got)
	}
	c = dial()
	if err := transport.WriteMsg(c, transport.MsgRecoveryLaneStart, transport.EncodeRecoveryLaneStart(lineage, 0)); err != nil {
		t.Fatal(err)
	}
	want := makeData(0xAA)
	if err := transport.WriteMsg(c, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: lineage, LBA: 6, LSN: 95, Data: want})); err != nil {
		t.Fatal(err)
	}
	if err := barrier(c); err != nil {
		t.Fatal(err)
	}
	got, err := store.Read(6)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("uncertified starting image swallowed recovery hole: err=%v", err)
	}
}
