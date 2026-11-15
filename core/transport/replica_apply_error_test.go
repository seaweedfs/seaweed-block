package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// failingApplyStore refuses ApplyEntry for one LSN, the way walstore does when
// its WAL is full, and applies every other entry normally.
type failingApplyStore struct {
	storage.LogicalStorage
	failLSN uint64
}

func (s *failingApplyStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	if lsn == s.failLSN {
		return errors.New("test: WAL full")
	}
	return s.LogicalStorage.ApplyEntry(lba, data, lsn)
}

// A replica that fails to apply LSN 2 must not ack a barrier at or past LSN 2:
// neither on the connection that carried the failed entry, nor on a fresh
// connection afterwards. Before the fix, the production (no-hook) listener only
// logged the failure, applied LSN 3, and acked the barrier with AchievedLSN 3.
func TestReplicaApplyFailureStopsBarrierAtTheGap(t *testing.T) {
	store := &failingApplyStore{LogicalStorage: storage.NewBlockStore(64, 4096), failLSN: 2}
	listener, err := NewReplicaListener("127.0.0.1:0", store) // production no-hook path
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	lineage := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}

	conn := g2Dial(t, listener.Addr())
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	for lsn := uint64(1); lsn <= 3; lsn++ {
		if err := WriteMsg(conn, MsgShipEntry, g2ShipPayload(lineage, lsn)); err != nil {
			// The listener may already have closed the connection after LSN 2.
			break
		}
	}
	_ = WriteMsg(conn, MsgBarrierReq, EncodeLineage(lineage))
	if msgType, payload, err := ReadMsg(conn); err == nil {
		if msgType != MsgBarrierResp {
			t.Fatalf("unexpected reply type 0x%02x", msgType)
		}
		resp, err := DecodeBarrierResp(payload)
		if err != nil {
			t.Fatal(err)
		}
		if resp.AchievedLSN >= 2 {
			t.Fatalf("barrier on the same connection acked AchievedLSN %d past the failed LSN 2", resp.AchievedLSN)
		}
	}

	// A fresh connection, as the primary's next barrier would use, must also
	// stay below the gap: nothing after the failed entry may have been applied.
	fresh := g2Dial(t, listener.Addr())
	_ = fresh.SetDeadline(time.Now().Add(2 * time.Second))
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
	if resp.AchievedLSN != 1 {
		t.Fatalf("barrier on a fresh connection acked AchievedLSN %d, want 1 (the last LSN before the gap)", resp.AchievedLSN)
	}
}

// Control: with no failing apply, the same sequence acks every shipped LSN, so
// the test above fails only because of the gap.
func TestReplicaApplyNoFailureAcksAllShipped(t *testing.T) {
	store := &failingApplyStore{LogicalStorage: storage.NewBlockStore(64, 4096)} // failLSN 0: never fails
	listener, err := NewReplicaListener("127.0.0.1:0", store)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	lineage := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}

	conn := g2Dial(t, listener.Addr())
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	for lsn := uint64(1); lsn <= 3; lsn++ {
		if err := WriteMsg(conn, MsgShipEntry, g2ShipPayload(lineage, lsn)); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteMsg(conn, MsgBarrierReq, EncodeLineage(lineage)); err != nil {
		t.Fatal(err)
	}
	msgType, payload, err := ReadMsg(conn)
	if err != nil || msgType != MsgBarrierResp {
		t.Fatalf("barrier: type=0x%02x err=%v", msgType, err)
	}
	resp, err := DecodeBarrierResp(payload)
	if err != nil {
		t.Fatal(err)
	}
	if resp.AchievedLSN != 3 {
		t.Fatalf("barrier acked AchievedLSN %d, want 3", resp.AchievedLSN)
	}
}
