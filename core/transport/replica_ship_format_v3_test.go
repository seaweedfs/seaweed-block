//go:build swblock_format_v3_test

package transport

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The ordinary ship path onto a dark v3 replica, through the real
// MsgShipEntry handler (replica.go, no apply hook): entries apply in LSN
// order, a barrier acknowledges them, and a crash and reopen keep them. An
// out-of-order entry ends the connection (the store refuses it). Mutant:
// ApplyEntry refused on v3 (errV3NotYet) fails here.
func TestReplicaV3ShipEntriesApplyAndSurviveACrash(t *testing.T) {
	path := t.TempDir() + "/ship.v3"
	s, err := storage.CreateWALStoreV3Test(path, 64, v3RepBlock)
	if err != nil {
		t.Fatal(err)
	}
	l := v3Listen(t, s)
	c := v3Dial(t, l)
	lin := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 3}
	for lsn := uint64(1); lsn <= 3; lsn++ {
		if err := WriteMsg(c, MsgShipEntry, EncodeShipEntry(ShipEntry{Lineage: lin, LBA: uint32(lsn), LSN: lsn, Data: v3RepBlk(byte(lsn))})); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteMsg(c, MsgBarrierReq, EncodeLineage(lin)); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	mt, payload, err := ReadMsg(c)
	if err != nil || mt != MsgBarrierResp {
		t.Fatalf("barrier: type %d err %v", mt, err)
	}
	if resp, err := DecodeBarrierResp(payload); err != nil || resp.AchievedLSN < 3 {
		t.Fatalf("barrier AchievedLSN %d err %v, want >= 3", resp.AchievedLSN, err)
	}
	if err := WriteMsg(c, MsgShipEntry, EncodeShipEntry(ShipEntry{Lineage: lin, LBA: 7, LSN: 2, Data: v3RepBlk(0x77)})); err == nil {
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		if _, _, err := ReadMsg(c); err == nil {
			t.Fatal("an out-of-order ship entry did not end the connection")
		}
	}
	l.Stop()
	r, err := storage.CrashReopenV3Test(s, path)
	if err != nil {
		t.Fatalf("crash and reopen: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	for lba := uint32(1); lba <= 3; lba++ {
		if got := v3Read(t, r, lba); !bytes.Equal(got, v3RepBlk(byte(lba))) {
			t.Fatalf("LBA %d = %x... after reopen", lba, got[0])
		}
	}
	if got := v3Read(t, r, 7); bytes.Equal(got, v3RepBlk(0x77)) {
		t.Fatal("the out-of-order entry was applied")
	}
}
