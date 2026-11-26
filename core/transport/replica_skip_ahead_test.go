package transport

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// recordingApplyStore records every LSN ApplyEntry is called with.
type recordingApplyStore struct {
	storage.LogicalStorage
	calls chan uint64
}

func (s *recordingApplyStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	s.calls <- lsn
	return s.LogicalStorage.ApplyEntry(lba, data, lsn)
}

// A live frame whose LSN skips ahead of the replica's next LSN, inside an
// epoch, is refused: the entry is never applied at that position and no
// barrier acknowledges a frontier past the gap (an entry is applied only at
// the position its LSN names, the next one).
func TestReplicaRefusesASkippedAheadLSNFrame(t *testing.T) {
	wal, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "r.wal"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	store := &recordingApplyStore{LogicalStorage: wal, calls: make(chan uint64, 16)}
	listener, err := NewReplicaListener("127.0.0.1:0", store) // production no-hook path
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	lineage := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}

	conn := g2Dial(t, listener.Addr())
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	for _, lsn := range []uint64{1, 2, 4} { // 3 is skipped
		if err := WriteMsg(conn, MsgShipEntry, g2ShipPayload(lineage, lsn)); err != nil {
			t.Fatalf("ship LSN %d: %v", lsn, err)
		}
	}
	_ = WriteMsg(conn, MsgBarrierReq, EncodeLineage(lineage))
	_, _, _ = ReadMsg(conn) // closed, or the barrier's answer: the fresh barrier below decides

	fresh := g2Dial(t, listener.Addr())
	_ = fresh.SetDeadline(time.Now().Add(5 * time.Second))
	if err := WriteMsg(fresh, MsgBarrierReq, EncodeLineage(lineage)); err != nil {
		t.Fatal(err)
	}
	msgType, payload, err := ReadMsg(fresh)
	if err != nil || msgType != MsgBarrierResp {
		t.Fatalf("barrier on a fresh connection: type=0x%02x err=%v", msgType, err)
	}
	resp, err := DecodeBarrierResp(payload)
	if err != nil {
		t.Fatal(err)
	}
	if resp.AchievedLSN != 2 {
		t.Fatalf("barrier after LSN 1, 2, 4 (3 skipped): AchievedLSN %d; want 2 (nothing past the gap)", resp.AchievedLSN)
	}
	close(store.calls)
	for lsn := range store.calls {
		if lsn == 4 {
			t.Fatal("the skipped-ahead LSN 4 was applied")
		}
	}
}

// The gap is filled the way the primary recovers a replica that closed the
// connection: a barrier on a new connection reports the replica's end (2),
// and the entries from there (3, 4) land in order; the barrier then reports
// 4 and every LSN was applied once, in order.
func TestReplicaSkippedAheadRefusalIsFilledFromTheBarrierEnd(t *testing.T) {
	wal, err := storage.CreateWALStore(filepath.Join(t.TempDir(), "r.wal"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	store := &recordingApplyStore{LogicalStorage: wal, calls: make(chan uint64, 16)}
	listener, err := NewReplicaListener("127.0.0.1:0", store)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	lineage := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}
	barrier := func() uint64 {
		t.Helper()
		c := g2Dial(t, listener.Addr())
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if err := WriteMsg(c, MsgBarrierReq, EncodeLineage(lineage)); err != nil {
			t.Fatal(err)
		}
		msgType, payload, err := ReadMsg(c)
		if err != nil || msgType != MsgBarrierResp {
			t.Fatalf("barrier: type=0x%02x err=%v", msgType, err)
		}
		resp, err := DecodeBarrierResp(payload)
		if err != nil {
			t.Fatal(err)
		}
		return resp.AchievedLSN
	}

	conn := g2Dial(t, listener.Addr())
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	for _, lsn := range []uint64{1, 2, 4} {
		_ = WriteMsg(conn, MsgShipEntry, g2ShipPayload(lineage, lsn))
	}
	_, _, _ = ReadMsg(conn) // the replica closes it on the refused entry
	var applied []uint64
	next := func() uint64 { // one apply, in arrival order
		t.Helper()
		select {
		case lsn := <-store.calls:
			applied = append(applied, lsn)
			return lsn
		case <-time.After(5 * time.Second):
			t.Fatalf("applied %v; no further apply", applied)
			return 0
		}
	}
	next()
	next()
	end := barrier()
	if end != 2 {
		t.Fatalf("barrier end after the refusal: %d; want 2", end)
	}
	again := g2Dial(t, listener.Addr()) // the primary reconnects and catches up from the end
	_ = again.SetDeadline(time.Now().Add(5 * time.Second))
	for lsn := end + 1; lsn <= 4; lsn++ {
		if err := WriteMsg(again, MsgShipEntry, g2ShipPayload(lineage, lsn)); err != nil {
			t.Fatalf("catch-up LSN %d: %v", lsn, err)
		}
	}
	next()
	next()
	if got := barrier(); got != 4 {
		t.Fatalf("barrier end after the catch-up: %d; want 4", got)
	}
	select {
	case lsn := <-store.calls:
		t.Fatalf("applied %v and then %d again", applied, lsn)
	default:
	}
	if len(applied) != 4 || applied[0] != 1 || applied[1] != 2 || applied[2] != 3 || applied[3] != 4 {
		t.Fatalf("applied %v; want 1, 2, 3, 4 in order", applied)
	}
}
