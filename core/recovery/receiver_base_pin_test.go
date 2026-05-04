package recovery

import (
	"net"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestReceiver_SessionStart_UsesFromLSNAsBaseFrontierHint(t *testing.T) {
	replica := storage.NewBlockStore(0, 4096)
	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	receiver := NewReceiver(replica, replicaConn)
	done := make(chan struct{})
	var (
		achieved uint64
		runErr   error
	)
	go func() {
		defer close(done)
		achieved, runErr = receiver.Run()
	}()

	const (
		basePinLSN = uint64(7)
		compatBand = uint64(99)
		sessionID  = uint64(42)
		numBlocks  = uint32(0)
	)
	if err := writeFrame(primaryConn, frameSessionStart, encodeSessionStart(sessionStartPayload{
		SessionID: sessionID,
		FromLSN:   basePinLSN,
		TargetLSN: compatBand,
		NumBlocks: numBlocks,
	})); err != nil {
		t.Fatalf("write SessionStart: %v", err)
	}
	if err := writeFrame(primaryConn, frameBaseDone, nil); err != nil {
		t.Fatalf("write BaseDone: %v", err)
	}
	ft, payload, err := readFrame(primaryConn)
	if err != nil {
		t.Fatalf("read BaseBatchAck: %v", err)
	}
	if ft != frameBaseBatchAck {
		t.Fatalf("frame type=%v want BaseBatchAck", ft)
	}
	if _, err := decodeBaseBatchAck(payload); err != nil {
		t.Fatalf("decode BaseBatchAck: %v", err)
	}
	if err := writeFrame(primaryConn, frameBarrierReq, nil); err != nil {
		t.Fatalf("write BarrierReq: %v", err)
	}
	ft, payload, err = readFrame(primaryConn)
	if err != nil {
		t.Fatalf("read BarrierResp: %v", err)
	}
	if ft != frameBarrierResp {
		t.Fatalf("frame type=%v want BarrierResp", ft)
	}
	wireAchieved, err := decodeBarrierResp(payload)
	if err != nil {
		t.Fatalf("decode BarrierResp: %v", err)
	}
	<-done
	if runErr != nil {
		t.Fatalf("receiver.Run: %v", runErr)
	}
	if achieved != basePinLSN || wireAchieved != basePinLSN {
		t.Fatalf("achieved=(run:%d wire:%d), want base pin %d; compat band %d must not drive BASE frontier",
			achieved, wireAchieved, basePinLSN, compatBand)
	}
	st := receiver.Session().Status()
	if st.BaseFrontierHint != basePinLSN {
		t.Fatalf("BaseFrontierHint=%d want FromLSN/BasePinLSN %d", st.BaseFrontierHint, basePinLSN)
	}
	if st.TargetLSN != basePinLSN {
		t.Fatalf("TargetLSN legacy status alias=%d want BaseFrontierHint %d", st.TargetLSN, basePinLSN)
	}
}

func TestReceiver_DurableProgressAck_DoesNotExceedDurableSyncFrontier(t *testing.T) {
	store := &receiverAckStore{
		numBlocks:    4,
		blockSize:    4096,
		syncFrontier: 7,
	}
	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	session := NewRebuildSession(store, 0)
	if err := session.ApplyWALEntry(WALKindSessionLive, 1, make([]byte, store.blockSize), 10); err != nil {
		t.Fatalf("ApplyWALEntry: %v", err)
	}
	receiver := NewReceiver(store, replicaConn)
	receiver.sessionID = 99
	receiver.session = session
	receiver.baseInstalledUpper = 3

	errCh := make(chan error, 1)
	go func() {
		errCh <- receiver.sendAck()
	}()

	ft, payload, err := readFrame(primaryConn)
	if err != nil {
		t.Fatalf("read BaseBatchAck: %v", err)
	}
	if ft != frameBaseBatchAck {
		t.Fatalf("frame type=%v want BaseBatchAck", ft)
	}
	ack, err := decodeBaseBatchAck(payload)
	if err != nil {
		t.Fatalf("decode BaseBatchAck: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("sendAck: %v", err)
	}
	if ack.AcknowledgedLSN != store.syncFrontier {
		t.Fatalf("AcknowledgedLSN=%d want durable sync frontier %d; WALApplied=10 must not overclaim",
			ack.AcknowledgedLSN, store.syncFrontier)
	}
	if ack.BaseLBAUpper != receiver.baseInstalledUpper {
		t.Fatalf("BaseLBAUpper=%d want advisory base progress %d", ack.BaseLBAUpper, receiver.baseInstalledUpper)
	}
	if store.syncCalls != 1 {
		t.Fatalf("Sync calls=%d want 1", store.syncCalls)
	}
}

type receiverAckStore struct {
	numBlocks    uint32
	blockSize    int
	syncFrontier uint64
	head         uint64
	syncCalls    int
}

func (s *receiverAckStore) Write(lba uint32, data []byte) (uint64, error) {
	s.head++
	return s.head, nil
}

func (s *receiverAckStore) Read(lba uint32) ([]byte, error) {
	return make([]byte, s.blockSize), nil
}

func (s *receiverAckStore) Sync() (uint64, error) {
	s.syncCalls++
	return s.syncFrontier, nil
}

func (s *receiverAckStore) Recover() (uint64, error) { return s.syncFrontier, nil }

func (s *receiverAckStore) Boundaries() (uint64, uint64, uint64) {
	return s.syncFrontier, 0, s.head
}

func (s *receiverAckStore) NextLSN() uint64 { return s.head + 1 }

func (s *receiverAckStore) NumBlocks() uint32 { return s.numBlocks }

func (s *receiverAckStore) BlockSize() int { return s.blockSize }

func (s *receiverAckStore) AdvanceFrontier(lsn uint64) {
	if lsn > s.head {
		s.head = lsn
	}
}

func (s *receiverAckStore) AdvanceWALTail(newTail uint64) {}

func (s *receiverAckStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	if lsn > s.head {
		s.head = lsn
	}
	return nil
}

func (s *receiverAckStore) WriteExtentDirect(lba uint32, data []byte) error { return nil }

func (s *receiverAckStore) AllBlocks() map[uint32][]byte { return nil }

func (s *receiverAckStore) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	return nil
}

func (s *receiverAckStore) RecoveryMode() storage.RecoveryMode {
	return storage.RecoveryModeWALReplay
}

func (s *receiverAckStore) AppliedLSNs() (map[uint32]uint64, error) {
	return nil, storage.ErrAppliedLSNsNotTracked
}

func (s *receiverAckStore) Close() error { return nil }
