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
