package main

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

func TestReplicationIngressSharesApplyVersionsAcrossSessions(t *testing.T) {
	store := storage.NewBlockStore(64, 4096)
	store.AdvanceFrontier(119) // the replica holds 1..119 (a rebuilt base): 120 is its next LSN
	r := newReplicationIngress(flags{dataAddr: "127.0.0.1:0", volumeID: "vol1"}, store)
	if err := r.AdmitAsReplica(1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Stop)
	// This replica's history is epoch 1: a live entry of an epoch the table
	// has no row for is accepted only at the store's next LSN.
	if err := r.EpochTable().Replace([]transport.EpochRow{{Epoch: 1, FirstLSN: 1}}); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{0xBB}, 4096)
	ship := func(session, lsn uint64, data []byte, recovery bool) {
		c, err := net.DialTimeout("tcp", r.listener.Addr(), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err := c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		lineage := transport.RecoveryLineage{SessionID: session, Epoch: 1, EndpointVersion: 1, TargetLSN: 120}
		send := func(kind byte, data []byte) {
			if err := transport.WriteMsg(c, kind, data); err != nil {
				t.Fatal(err)
			}
		}
		if recovery {
			end, _ := store.Sync() // the catch-up starts after the replica's durable end
			send(transport.MsgRecoveryLaneStart, transport.EncodeRecoveryLaneStart(lineage, end))
		}
		send(transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: lineage, LBA: 5, Data: data, LSN: lsn}))
		send(transport.MsgBarrierReq, transport.EncodeLineage(lineage))
		kind, _, err := transport.ReadMsg(c)
		if err != nil || kind != transport.MsgBarrierResp {
			t.Fatalf("barrier: kind=%d err=%v", kind, err)
		}
	}
	ship(1, 120, want, false)
	ship(2, 95, bytes.Repeat([]byte{0xAA}, 4096), true)
	got, err := store.Read(5)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("production ingress allowed cross-session byte regression: err=%v", err)
	}
}
