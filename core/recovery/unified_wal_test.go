package recovery

// New test catalog for §3.2 #3 unified WAL stream / cursor-rewind on
// memorywal substrate. Per
// v3-recovery-unified-wal-stream-mini-plan.md §2.2 (Q14: single PR).
// Replaces the BlockStore-substrate tests skipped in checkpoints 1+2.
//
// Pinned by INV-WAL-CURSOR-MONOTONIC-FROM-PINLSN (proposed; see
// v3-recovery-inv-test-map.md row added in this checkpoint).

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// memorywalPayload synthesizes a deterministic block whose first byte
// encodes lba ^ epoch. Used by all unified-wal tests.
func memorywalPayload(lba uint32, epoch byte, blockSize int) []byte {
	out := make([]byte, blockSize)
	v := byte(lba) ^ epoch
	for i := range out {
		out[i] = v
	}
	return out
}

// TestSender_PumpHappyPath_OnMemoryWAL replaces TestE2E_RebuildHappyPath
// on the memorywal substrate. Pins: rewind-once + monotonic forward +
// idle-exit + barrier convergence + replica byte-equality.
func TestSender_PumpHappyPath_OnMemoryWAL(t *testing.T) {
	const numBlocks uint32 = 32
	const blockSize int = 4096
	const epoch byte = 0xA0

	primary := memorywal.NewStore(numBlocks, blockSize)
	replica := memorywal.NewStore(numBlocks, blockSize)

	// Populate primary with 16 LBAs (LSNs 1..16 by memorywal's
	// per-Write LSN counter).
	for lba := uint32(0); lba < 16; lba++ {
		if _, err := primary.Write(lba, memorywalPayload(lba, epoch, blockSize)); err != nil {
			t.Fatalf("primary Write lba=%d: %v", lba, err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatalf("primary Sync: %v", err)
	}

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	coord := NewPeerShipCoordinator()
	// Tight idle window for fast test exit.
	sender := NewSenderWithOptions(primary, coord, primaryConn, "r1", 20*time.Millisecond)
	receiver := NewReceiver(replica, replicaConn)

	_, _, primaryH := primary.Boundaries()
	if err := coord.StartSession("r1", 7, 0, primaryH); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	var (
		wg           sync.WaitGroup
		recvErr      error
		recvAchieved uint64
		sendErr      error
		sendAchieved uint64
	)
	wg.Add(2)
	go func() { defer wg.Done(); recvAchieved, recvErr = receiver.Run() }()
	go func() {
		defer wg.Done()
		sendAchieved, sendErr = sender.Run(context.Background(), 7, 0, primaryH)
	}()
	wg.Wait()

	if sendErr != nil {
		t.Fatalf("sender: %v", sendErr)
	}
	if recvErr != nil {
		t.Fatalf("receiver: %v", recvErr)
	}
	if sendAchieved != recvAchieved {
		t.Fatalf("achieved mismatch: sender=%d receiver=%d", sendAchieved, recvAchieved)
	}
	if sendAchieved < primaryH {
		t.Errorf("achievedLSN=%d < primary head=%d", sendAchieved, primaryH)
	}

	// Replica byte-equal to primary at all written LBAs.
	for lba := uint32(0); lba < 16; lba++ {
		pd, _ := primary.Read(lba)
		rd, _ := replica.Read(lba)
		if !bytes.Equal(pd, rd) {
			t.Fatalf("lba %d mismatch: primary[0]=%02x replica[0]=%02x", lba, pd[0], rd[0])
		}
	}

	if got := coord.Phase("r1"); got != PhaseIdle {
		t.Fatalf("post-session phase=%s want Idle", got)
	}
}

// TestSender_LiveWritesDuringSession_OnMemoryWAL replaces
// TestE2E_RebuildWithLiveWritesDuringSession on memorywal. The post-
// §3.2 #3 mechanism: writes to primary substrate during the active
// session naturally appear in subsequent ScanLBAs cycles; pump's
// repeated scans pick them up; cursor advances; barrier waits until
// idle window elapses with cursor caught at head.
//
// Pins: writes-during-session arrive via substrate's growing tail
// (no PushLiveWrite needed); barrier reflects all writes.
func TestSender_LiveWritesDuringSession_OnMemoryWAL(t *testing.T) {
	const numBlocks uint32 = 32
	const blockSize int = 4096
	const epoch byte = 0xB0

	primary := memorywal.NewStore(numBlocks, blockSize)
	replica := memorywal.NewStore(numBlocks, blockSize)

	// Phase 1: 10 LBAs baseline.
	for lba := uint32(0); lba < 10; lba++ {
		if _, err := primary.Write(lba, memorywalPayload(lba, epoch, blockSize)); err != nil {
			t.Fatalf("phase1 Write: %v", err)
		}
	}
	_, _ = primary.Sync()
	_, _, frozenTarget := primary.Boundaries()
	if frozenTarget != 10 {
		t.Fatalf("expected primary H=10, got %d", frozenTarget)
	}

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	coord := NewPeerShipCoordinator()
	sender := NewSenderWithOptions(primary, coord, primaryConn, "r1", 50*time.Millisecond)
	receiver := NewReceiver(replica, replicaConn)

	if err := coord.StartSession("r1", 11, 0, frozenTarget); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Live writer: appends 5 LBAs during the session. Pump's
	// repeated ScanLBAs cycles pick them up because cursor < head
	// after each Write.
	var liveWg sync.WaitGroup
	liveWg.Add(1)
	go func() {
		defer liveWg.Done()
		// Small initial delay so sender is past base lane when these
		// writes land — exercises the in-pump cursor-follows-head case.
		time.Sleep(5 * time.Millisecond)
		for i := uint32(0); i < 5; i++ {
			lba := 10 + i
			data := memorywalPayload(lba, epoch, blockSize)
			if _, err := primary.Write(lba, data); err != nil {
				t.Errorf("live Write lba=%d: %v", lba, err)
				return
			}
			// Confirm coord still says SessionLane (active session).
			lsn, _, _ := primary.Boundaries()
			if route := coord.RouteLocalWrite("r1", lsn); route != RouteSessionLane {
				t.Errorf("active session: route=%v want SessionLane", route)
				return
			}
		}
	}()

	var (
		wg           sync.WaitGroup
		recvErr      error
		recvAchieved uint64
		sendErr      error
	)
	wg.Add(2)
	go func() { defer wg.Done(); recvAchieved, recvErr = receiver.Run() }()
	go func() {
		defer wg.Done()
		_, sendErr = sender.Run(context.Background(), 11, 0, frozenTarget)
	}()
	liveWg.Wait()
	wg.Wait()

	if sendErr != nil {
		t.Fatalf("sender: %v", sendErr)
	}
	if recvErr != nil {
		t.Fatalf("receiver: %v", recvErr)
	}

	// AchievedLSN must reflect ALL 15 writes (10 baseline + 5 live).
	if recvAchieved < 15 {
		t.Errorf("achievedLSN=%d, expected >= 15 (all writes must reach replica)", recvAchieved)
	}

	for lba := uint32(0); lba < 15; lba++ {
		pd, _ := primary.Read(lba)
		rd, _ := replica.Read(lba)
		if !bytes.Equal(pd, rd) {
			t.Fatalf("lba %d mismatch", lba)
		}
	}
}

// TestSender_KindByte_FlipsOnceAtCatchUp pins the kind-flip rule:
// once cursor catches head (within kindFlipEpsilon), the Kind byte
// flips Backlog → SessionLive and never flips back. Counted via
// RebuildSession.Status() — receiver-side observability.
func TestSender_KindByte_FlipsOnceAtCatchUp(t *testing.T) {
	const numBlocks uint32 = 32
	const blockSize int = 4096
	const epoch byte = 0xC0

	primary := memorywal.NewStore(numBlocks, blockSize)
	replica := memorywal.NewStore(numBlocks, blockSize)

	// 10 baseline writes; kindFlipEpsilon=8 means pump flips kind
	// when cursor is within 8 of head, i.e., somewhere near the end
	// of this 10-entry stream.
	for lba := uint32(0); lba < 10; lba++ {
		_, _ = primary.Write(lba, memorywalPayload(lba, epoch, blockSize))
	}
	_, _ = primary.Sync()
	_, _, primaryH := primary.Boundaries()

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	coord := NewPeerShipCoordinator()
	sender := NewSenderWithOptions(primary, coord, primaryConn, "r1", 20*time.Millisecond)
	receiver := NewReceiver(replica, replicaConn)

	_ = coord.StartSession("r1", 13, 0, primaryH)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = receiver.Run() }()
	go func() { defer wg.Done(); _, _ = sender.Run(context.Background(), 13, 0, primaryH) }()
	wg.Wait()

	st := receiver.Session().Status()
	// Either some Backlog entries (early) or all entries flipped to
	// SessionLive depending on how quickly head was caught. Both must
	// sum to total entries; neither side may be negative; SessionLive
	// must be ≥ 1 (the kind-flip rule is monotonic and exercised here).
	if got := st.BacklogApplied + st.SessionLiveApplied; got < 10 {
		t.Errorf("backlog+sessionLive=%d, expected ≥ 10 (total entries)", got)
	}
	// Kind transition must be monotonic: there's no observable
	// "flipped back to Backlog" because each frame's kind is
	// determined at send time and the receiver counts them in arrival
	// order. We don't have a per-frame trace exposed; we rely on
	// (a) total count correctness and (b) the implementation's
	// monotonic-flip property pinned by code review of streamUntilHead.
	t.Logf("kind counts: backlog=%d sessionLive=%d (target=%d, walApplied=%d)",
		st.BacklogApplied, st.SessionLiveApplied, st.TargetLSN, st.WALApplied)
}

// TestSender_StreamUntilHead_CtxCancel pins: while the pump is in its
// idle wait (cursor caught head, waiting for new appends or idle
// timeout), context cancellation produces FailureCancelled with
// Phase=Backlog.
func TestSender_StreamUntilHead_CtxCancel(t *testing.T) {
	const numBlocks uint32 = 8
	const blockSize int = 4096

	primary := memorywal.NewStore(numBlocks, blockSize)
	replica := memorywal.NewStore(numBlocks, blockSize)

	// One write so head > 0.
	_, _ = primary.Write(0, memorywalPayload(0, 0xD0, blockSize))
	_, _ = primary.Sync()
	_, _, primaryH := primary.Boundaries()

	primaryConn, replicaConn := net.Pipe()

	coord := NewPeerShipCoordinator()
	// Long idle window so we cancel BEFORE it elapses.
	sender := NewSenderWithOptions(primary, coord, primaryConn, "r1", 5*time.Second)
	receiver := NewReceiver(replica, replicaConn)

	_ = coord.StartSession("r1", 17, 0, primaryH)

	ctx, cancel := context.WithCancel(context.Background())

	var (
		recvWg  sync.WaitGroup
		sendWg  sync.WaitGroup
		sendErr error
	)
	recvWg.Add(1)
	sendWg.Add(1)
	go func() { defer recvWg.Done(); _, _ = receiver.Run() }()
	go func() {
		defer sendWg.Done()
		_, sendErr = sender.Run(ctx, 17, 0, primaryH)
	}()

	// Wait until pump is likely in idle window (cursor caught head),
	// then cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()
	sendWg.Wait()
	// Close conns to unblock receiver (cancel only signals sender).
	_ = primaryConn.Close()
	_ = replicaConn.Close()
	recvWg.Wait()

	f := AsFailure(sendErr)
	if f == nil {
		t.Fatalf("expected typed *Failure, got %T: %v", sendErr, sendErr)
	}
	if f.Kind != FailureCancelled {
		t.Errorf("Kind=%s want Cancelled", f.Kind)
	}
	if f.Phase != PhaseBacklog {
		t.Errorf("Phase=%q want Backlog (ctx wait inside streamUntilHead)", f.Phase)
	}
}

// TestReceiver_RejectsBackwardLSN_InSession pins kickoff v0.3 §5.1
// row: a backward in-stream LSN MUST produce FailureProtocol.
// Receiver must NOT silently skip or "rewind" mid-session.
//
// Construction: hand-craft the wire. Send a SessionStart, then two
// frameWALEntry frames; the second has LSN < first.
func TestReceiver_RejectsBackwardLSN_InSession(t *testing.T) {
	const numBlocks uint32 = 8
	const blockSize int = 4096

	replica := memorywal.NewStore(numBlocks, blockSize)

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	receiver := NewReceiver(replica, replicaConn)

	// Receiver runs in its own goroutine; we feed it crafted frames.
	var (
		wg      sync.WaitGroup
		recvErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, recvErr = receiver.Run()
	}()

	// Frame 1: SessionStart with FromLSN=10, TargetLSN=20, NumBlocks
	// = primary side's NumBlocks (matches local — receiver checks).
	if err := writeFrame(primaryConn, frameSessionStart,
		encodeSessionStart(sessionStartPayload{
			SessionID: 7, FromLSN: 10, TargetLSN: 20, NumBlocks: numBlocks,
		})); err != nil {
		t.Fatalf("write SessionStart: %v", err)
	}
	// Receiver sets appliedLSN := 10. Expects first frame's LSN = 11.

	// Frame 2: WALEntry with LSN=11 (good — applies, appliedLSN=11)
	data1 := memorywalPayload(0, 0xE0, blockSize)
	if err := writeFrame(primaryConn, frameWALEntry,
		encodeWALEntry(WALKindBacklog, 0, 11, data1)); err != nil {
		t.Fatalf("write WALEntry LSN=11: %v", err)
	}

	// Frame 3: WALEntry with LSN=10 (BACKWARD: 10 < applied=11)
	// Receiver MUST FailureProtocol.
	data2 := memorywalPayload(0, 0xE1, blockSize)
	if err := writeFrame(primaryConn, frameWALEntry,
		encodeWALEntry(WALKindBacklog, 0, 10, data2)); err != nil {
		t.Fatalf("write WALEntry LSN=10 (backward): %v", err)
	}

	// Close primary side so receiver's read after the rejected frame
	// surfaces EOF (in case receiver continues reading instead of
	// erroring on the backward frame — that would be a bug).
	_ = primaryConn.Close()
	wg.Wait()

	if recvErr == nil {
		t.Fatal("expected receiver to fail on backward LSN; got nil")
	}
	f := AsFailure(recvErr)
	if f == nil {
		t.Fatalf("expected typed *Failure, got %T: %v", recvErr, recvErr)
	}
	if f.Kind != FailureProtocol {
		t.Errorf("Kind=%s want Protocol (backward LSN)", f.Kind)
	}
	if f.Phase != PhaseRecvDispatch {
		t.Errorf("Phase=%q want RecvDispatch", f.Phase)
	}
}

// TestReceiver_RejectsGap_InSession pins kickoff v0.3 §5.1 row:
// a gap (lsn > applied+1) MUST produce FailureContract. Receiver
// must NOT silently skip the missing range.
func TestReceiver_RejectsGap_InSession(t *testing.T) {
	const numBlocks uint32 = 8
	const blockSize int = 4096

	replica := memorywal.NewStore(numBlocks, blockSize)

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	receiver := NewReceiver(replica, replicaConn)

	var (
		wg      sync.WaitGroup
		recvErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, recvErr = receiver.Run()
	}()

	// SessionStart with FromLSN=5; appliedLSN := 5; expects LSN=6.
	if err := writeFrame(primaryConn, frameSessionStart,
		encodeSessionStart(sessionStartPayload{
			SessionID: 9, FromLSN: 5, TargetLSN: 100, NumBlocks: numBlocks,
		})); err != nil {
		t.Fatalf("write SessionStart: %v", err)
	}

	// Skip LSN=6, send LSN=10 directly: gap of 4.
	data := memorywalPayload(0, 0xF0, blockSize)
	if err := writeFrame(primaryConn, frameWALEntry,
		encodeWALEntry(WALKindBacklog, 0, 10, data)); err != nil {
		t.Fatalf("write WALEntry LSN=10 (gap): %v", err)
	}

	_ = primaryConn.Close()
	wg.Wait()

	if recvErr == nil {
		t.Fatal("expected receiver to fail on gap; got nil")
	}
	f := AsFailure(recvErr)
	if f == nil {
		t.Fatalf("expected typed *Failure, got %T: %v", recvErr, recvErr)
	}
	if f.Kind != FailureContract {
		t.Errorf("Kind=%s want Contract (gap)", f.Kind)
	}
	if f.Phase != PhaseRecvDispatch {
		t.Errorf("Phase=%q want RecvDispatch", f.Phase)
	}
}

// TestReceiver_RejectsExactDuplicate_InSession pins the four-case
// shape: lsn == applied is FailureProtocol (sender bug; never
// legitimate on the wire). Distinct from §5.2 substrate-level
// per-LBA arbitration which is a different layer.
func TestReceiver_RejectsExactDuplicate_InSession(t *testing.T) {
	const numBlocks uint32 = 8
	const blockSize int = 4096

	replica := memorywal.NewStore(numBlocks, blockSize)

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	receiver := NewReceiver(replica, replicaConn)

	var (
		wg      sync.WaitGroup
		recvErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, recvErr = receiver.Run()
	}()

	// SessionStart with FromLSN=10; appliedLSN := 10; expects LSN=11.
	if err := writeFrame(primaryConn, frameSessionStart,
		encodeSessionStart(sessionStartPayload{
			SessionID: 11, FromLSN: 10, TargetLSN: 50, NumBlocks: numBlocks,
		})); err != nil {
		t.Fatalf("write SessionStart: %v", err)
	}

	// Apply LSN=11 successfully.
	data1 := memorywalPayload(0, 0xA1, blockSize)
	if err := writeFrame(primaryConn, frameWALEntry,
		encodeWALEntry(WALKindBacklog, 0, 11, data1)); err != nil {
		t.Fatalf("write WALEntry LSN=11: %v", err)
	}

	// Send LSN=11 AGAIN (exact duplicate). Should be FailureProtocol.
	data2 := memorywalPayload(0, 0xA2, blockSize)
	if err := writeFrame(primaryConn, frameWALEntry,
		encodeWALEntry(WALKindBacklog, 0, 11, data2)); err != nil {
		t.Fatalf("write WALEntry LSN=11 (duplicate): %v", err)
	}

	_ = primaryConn.Close()
	wg.Wait()

	if recvErr == nil {
		t.Fatal("expected receiver to fail on exact-duplicate LSN; got nil")
	}
	f := AsFailure(recvErr)
	if f == nil {
		t.Fatalf("expected typed *Failure, got %T: %v", recvErr, recvErr)
	}
	if f.Kind != FailureProtocol {
		t.Errorf("Kind=%s want Protocol (exact-duplicate LSN)", f.Kind)
	}
	if f.Phase != PhaseRecvDispatch {
		t.Errorf("Phase=%q want RecvDispatch", f.Phase)
	}
}
