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
