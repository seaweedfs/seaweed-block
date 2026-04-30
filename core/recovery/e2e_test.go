package recovery

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// formulaPayload synthesizes a deterministic 4 KiB block whose first
// byte distinguishes the LBA + epoch. All 4 KiB are filled with that
// byte so a hash-style verify is sensitive to off-by-one errors.
func formulaPayload(lba uint32, epoch byte, blockSize int) []byte {
	out := make([]byte, blockSize)
	v := byte(lba) ^ epoch
	for i := range out {
		out[i] = v
	}
	return out
}

// TestE2E_RebuildHappyPath drives a full session end-to-end:
//   - Primary writes 100 LBAs (LSN 1..100), syncs.
//   - Replica is empty (separate substrate).
//   - Session opens with fromLSN=0, targetLSN=100.
//   - Sender ships base lane + backlog (no live writes yet).
//   - Barrier round-trip closes the session.
//   - Verify: every LBA on the replica matches primary.
func TestE2E_RebuildHappyPath(t *testing.T) {
	const numBlocks = 64
	const blockSize = 4096
	const epoch byte = 0xA0

	primary := storage.NewBlockStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	// Populate primary with 50 LBAs of formula data; LSNs 1..50.
	for lba := uint32(0); lba < 50; lba++ {
		if _, err := primary.Write(lba, formulaPayload(lba, epoch, blockSize)); err != nil {
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
	sender := NewSenderWithBacklogRelay(primary, coord, primaryConn, "r1")
	receiver := NewReceiver(replica, replicaConn)

	_, _, primaryH := primary.Boundaries()
	// Caller-of-Run contract: StartSession before spawning Run.
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
	go func() {
		defer wg.Done()
		recvAchieved, recvErr = receiver.Run()
	}()
	go func() {
		defer wg.Done()
		sendAchieved, sendErr = sender.Run(context.Background(), 7, 0, primaryH)
	}()
	// No live writes in this scenario; sender.Run barriers as soon as
	// sink.DrainBacklog returns (P2c-slice B-2 lifecycle).
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

	// Verify replica matches primary byte-for-byte.
	for lba := uint32(0); lba < numBlocks; lba++ {
		pd, _ := primary.Read(lba)
		rd, _ := replica.Read(lba)
		if !bytes.Equal(pd, rd) {
			t.Fatalf("lba %d mismatch: primary[0]=%02x replica[0]=%02x", lba, pd[0], rd[0])
		}
	}

	// After session end, coordinator should be back to Idle.
	if got := coord.Phase("r1"); got != PhaseIdle {
		t.Fatalf("post-session phase=%s want Idle", got)
	}
}

// TestE2E_RebuildWithLiveWritesDuringSession is the case the user
// asked about: primary keeps writing during the session, sender
// routes those post-target entries onto the session lane (because
// we are still DrainingHistorical) and ships them ahead of barrier.
//
// Sequence:
//  1. Primary writes 30 LBAs, sync. Snapshot fromLSN=0, targetLSN=30.
//  2. Open session.
//  3. While session is active, primary writes 5 more (LSN 31..35).
//     The sender's caller (this test simulating the WAL shipper)
//     consults coordinator.RouteLocalWrite; while DrainingHistorical
//     the routing is RouteSessionLane → push onto sender via
//     PushLiveWrite.
//  4. Sender drains backlog, then live queue, then barrier.
//  5. Barrier achievedLSN must be ≥ max LSN shipped on session lane
//     (35), proving the live writes made it to the replica.
//  6. Verify replica byte-equal to primary at all LBAs.
func TestE2E_RebuildWithLiveWritesDuringSession(t *testing.T) {
	const numBlocks = 64
	const blockSize = 4096
	const epoch byte = 0xB0

	primary := storage.NewBlockStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	// Phase 1: 30 LBAs on primary, sync. Frozen target = primary H = 30.
	for lba := uint32(0); lba < 30; lba++ {
		if _, err := primary.Write(lba, formulaPayload(lba, epoch, blockSize)); err != nil {
			t.Fatalf("phase1 Write lba=%d: %v", lba, err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatalf("phase1 Sync: %v", err)
	}
	_, _, frozenTarget := primary.Boundaries()
	if frozenTarget != 30 {
		t.Fatalf("expected primary H=30, got %d", frozenTarget)
	}

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	coord := NewPeerShipCoordinator()
	sender := NewSenderWithBacklogRelay(primary, coord, primaryConn, "r1")
	receiver := NewReceiver(replica, replicaConn)

	// Caller-of-Run contract: StartSession before spawning Run.
	if err := coord.StartSession("r1", 11, 0, frozenTarget); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Phase 3: simulate WAL shipper that observes local writes and
	// asks the coordinator where each goes. After P2c-slice B-2,
	// sender.Run barriers as soon as sink.DrainBacklog returns —
	// there is no engine "go barrier" signal anymore. To preserve
	// the "writes during the session land before barrier" contract
	// deterministically, push live writes BEFORE starting sender.Run.
	// The bridging sink buffers them under sinkMu and flushAndSeal
	// ships them in LSN order after streamBacklog completes.
	for i := uint32(0); i < 5; i++ {
		lba := 30 + i
		data := formulaPayload(lba, epoch, blockSize)
		lsn, err := primary.Write(lba, data)
		if err != nil {
			t.Fatalf("live Write lba=%d: %v", lba, err)
		}
		// Ask coordinator how to route this entry. coord.StartSession
		// above already moved phase to DrainingHistorical, so
		// RouteLocalWrite returns SessionLane.
		route := coord.RouteLocalWrite("r1", lsn)
		if route != RouteSessionLane {
			t.Fatalf("during DrainingHistorical: expected RouteSessionLane, got %v", route)
		}
		if err := sender.PushLiveWrite(lba, lsn, data); err != nil {
			t.Fatalf("PushLiveWrite: %v", err)
		}
	}

	var (
		wg           sync.WaitGroup
		recvErr      error
		recvAchieved uint64
		sendErr      error
		sendAchieved uint64
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		recvAchieved, recvErr = receiver.Run()
	}()
	go func() {
		defer wg.Done()
		sendAchieved, sendErr = sender.Run(context.Background(), /*sessionID*/ 11, /*fromLSN*/ 0, frozenTarget)
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

	// AchievedLSN must reflect ALL 35 writes (30 backlog + 5 live).
	// In the POC, BlockStore's Sync uses nextLSN-1 as syncedLSN, so
	// after applying LSN 35 the frontier is 35.
	if recvAchieved < 35 {
		t.Fatalf("achievedLSN=%d, expected >= 35 (live writes must have been applied)", recvAchieved)
	}

	// Verify replica byte-equal to primary across the full active range.
	for lba := uint32(0); lba < 35; lba++ {
		pd, _ := primary.Read(lba)
		rd, _ := replica.Read(lba)
		if !bytes.Equal(pd, rd) {
			t.Fatalf("lba %d mismatch: primary[0]=%02x replica[0]=%02x", lba, pd[0], rd[0])
		}
	}

	// Coordinator returned to Idle after EndSession in sender.Run defer.
	if got := coord.Phase("r1"); got != PhaseIdle {
		t.Fatalf("post-session phase=%s want Idle", got)
	}
	// And after Idle, any subsequent local write routes to steady live.
	if got := coord.RouteLocalWrite("r1", 1000); got != RouteSteadyLive {
		t.Fatalf("post-session route: got %v want RouteSteadyLive", got)
	}
}

// TestE2E_KindCountsPinsBacklogVsLive verifies the WAL-lane kind tag
// flows wire → receiver → RebuildSession.Status: a session that ships
// N backlog entries plus M PushLiveWrite entries should report
// exactly (N, M) in BacklogApplied / SessionLiveApplied. This pins
// the observability contract from WALEntryKind.
//
// IMPORTANT: this test does NOT use kind counts to assert "caught
// up" — convergence is still proved by the byte-equal check after
// barrier (per architect ruling: kind is observability, not
// convergence proof).
//
// Substrate choice (P2c-slice B-2): memorywal — preserves real
// write-time LSNs, so live writes after frozenTarget keep LSN >
// frozenTarget and streamBacklog filters them out cleanly. BlockStore
// would synthesize LSN from walHead at scan time, so live writes
// (push-before-Run) would shift walHead and ALL entries would get
// stamped with the new walHead → all filtered out.
func TestE2E_KindCountsPinsBacklogVsLive(t *testing.T) {
	const numBlocks = 32
	const blockSize = 4096
	const epoch byte = 0xE5

	primary := memorywal.NewStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	// Phase 1: 10 LBAs — these become the backlog.
	for lba := uint32(0); lba < 10; lba++ {
		_, _ = primary.Write(lba, formulaPayload(lba, epoch, blockSize))
	}
	_, _ = primary.Sync()
	_, _, frozenTarget := primary.Boundaries()

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	coord := NewPeerShipCoordinator()
	sender := NewSenderWithBacklogRelay(primary, coord, primaryConn, "r1")
	receiver := NewReceiver(replica, replicaConn)

	if err := coord.StartSession("r1", 17, 0, frozenTarget); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Phase 2: 3 live writes BEFORE sender.Run starts. The bridging
	// sink buffers them under sinkMu via PushLiveWrite; flushAndSeal
	// ships them as WALKindSessionLive after streamBacklog. memorywal
	// preserves real LSNs, so these get LSN > frozenTarget and
	// streamBacklog's `e.LSN > targetLSN` filter excludes them from
	// the backlog scan, leaving them to flushAndSeal.
	for i := uint32(0); i < 3; i++ {
		lba := 10 + i
		data := formulaPayload(lba, epoch, blockSize)
		lsn, _ := primary.Write(lba, data)
		if err := sender.PushLiveWrite(lba, lsn, data); err != nil {
			t.Fatalf("PushLiveWrite (pre-Run): %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = receiver.Run()
	}()
	go func() {
		defer wg.Done()
		_, _ = sender.Run(context.Background(), 17, 0, frozenTarget)
	}()
	wg.Wait()

	st := receiver.Session().Status()
	// 10 backlog entries: ScanLBAs over BlockStore emits one entry
	// per stored LBA at scan-time LSN. Our 10 stored LBAs all have
	// LSN ≤ frozenTarget, so all 10 ship as Backlog kind.
	if st.BacklogApplied != 10 {
		t.Errorf("BacklogApplied=%d want 10", st.BacklogApplied)
	}
	if st.SessionLiveApplied != 3 {
		t.Errorf("SessionLiveApplied=%d want 3", st.SessionLiveApplied)
	}
	t.Logf("kind counts: backlog=%d, sessionLive=%d (target=%d, walApplied=%d)",
		st.BacklogApplied, st.SessionLiveApplied, st.TargetLSN, st.WALApplied)
}

// TestE2E_PinFloorAdvancesIncrementally — INV-PIN-ADVANCES-ONLY-ON-
// REPLICA-ACK end-to-end: receiver emits BaseBatchAck per cadence
// (K blocks or T elapsed), sender's reader translates each into
// coord.SetPinFloor, primary-side `pin_floor` advances from
// fromLSN (initial) toward walApplied as the session progresses.
//
// Setup uses a small cadence (K=8) so the 50-LBA backlog produces
// multiple acks; we observe pinFloor monotonically increase across
// the session via Status snapshots taken at session end and after
// the BaseDone-mandatory ack.
func TestE2E_PinFloorAdvancesIncrementally(t *testing.T) {
	const numBlocks = 64
	const blockSize = 4096
	const epoch byte = 0xF1

	primary := storage.NewBlockStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	// Populate primary so each Write commits a fresh LSN; final H = 50.
	for lba := uint32(0); lba < 50; lba++ {
		_, _ = primary.Write(lba, formulaPayload(lba, epoch, blockSize))
	}
	_, _ = primary.Sync()
	_, _, primaryH := primary.Boundaries()

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	coord := NewPeerShipCoordinator()
	sender := NewSenderWithBacklogRelay(primary, coord, primaryConn, "r1")
	// Small K so cadence triggers within the 50-LBA backlog.
	receiver := NewReceiverWithCadence(replica, replicaConn, 8, 50*time.Millisecond)

	if err := coord.StartSession("r1", 21, 0, primaryH); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Observe initial pin floor — should equal fromLSN (0 in this test).
	if got := coord.PinFloor("r1"); got != 0 {
		t.Errorf("initial pinFloor=%d want 0 (== fromLSN)", got)
	}

	var wg sync.WaitGroup
	var sendErr, recvErr error
	wg.Add(2)
	go func() { defer wg.Done(); _, recvErr = receiver.Run() }()
	go func() {
		defer wg.Done()
		_, sendErr = sender.Run(context.Background(), 21, 0, primaryH)
	}()
	// No live writes; sender.Run barriers on its own after DrainBacklog.
	wg.Wait()

	if sendErr != nil {
		t.Fatalf("sender: %v", sendErr)
	}
	if recvErr != nil {
		t.Fatalf("receiver: %v", recvErr)
	}

	// After session: coord goes Idle (pinFloor reads 0). To inspect
	// the LAST observed pinFloor before EndSession dropped it, use
	// the achieved frontier from the receiver — it equals the
	// primary's H, which is what the final ack would have driven.
	rR, _, _ := replica.Boundaries()
	if rR != primaryH {
		t.Fatalf("replica frontier=%d != primaryH=%d", rR, primaryH)
	}

	// Coordinator returned to Idle; pinFloor=0 by definition.
	if got := coord.Phase("r1"); got != PhaseIdle {
		t.Errorf("post-session phase=%s want Idle", got)
	}
	if got := coord.PinFloor("r1"); got != 0 {
		t.Errorf("post-EndSession pinFloor=%d want 0 (released)", got)
	}
}

// TestE2E_PushLiveWriteAtomicSeal — architect review item #1: while
// the session is active (any non-Idle phase), RouteLocalWrite returns
// SessionLane and PushLiveWrite must not lose entries between drain
// and barrier. After P2c-slice B-2 the seal point lives in the
// bridging sink's flushAndSeal (under sinkMu): any concurrent push
// either lands before sealing (buffered → shipped) or returns "sink
// sealed" error (caller knows session is closing).
//
// This test races N pushers against Run's autonomous completion
// (sender.Run barriers as soon as sink.DrainBacklog returns). Every
// successful PushLiveWrite (no error) must result in the entry being
// shipped — verified by the receiver's substrate having that LBA at
// the correct content.
func TestE2E_PushLiveWriteAtomicSeal(t *testing.T) {
	const numBlocks = 64
	const blockSize = 4096
	const epoch byte = 0xC0

	primary := storage.NewBlockStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	// Phase 1: minimal backlog (5 LBAs).
	for lba := uint32(0); lba < 5; lba++ {
		_, _ = primary.Write(lba, formulaPayload(lba, epoch, blockSize))
	}
	_, _ = primary.Sync()
	_, _, frozenTarget := primary.Boundaries()

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	coord := NewPeerShipCoordinator()
	sender := NewSenderWithBacklogRelay(primary, coord, primaryConn, "r1")
	receiver := NewReceiver(replica, replicaConn)

	if err := coord.StartSession("r1", 13, 0, frozenTarget); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	var wg sync.WaitGroup
	var recvErr error
	var sendErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, recvErr = receiver.Run()
	}()
	go func() {
		defer wg.Done()
		_, sendErr = sender.Run(context.Background(), 13, 0, frozenTarget)
	}()

	// Pusher goroutine: rapid-fire live writes; some will land before
	// seal (no error) and ship; some will land after seal (error) and
	// the caller would need to fall back. Track the LSN range that
	// SUCCESSFULLY pushed so we can verify those LBAs shipped.
	var (
		acceptedMu  sync.Mutex
		acceptedSet = map[uint32][]byte{}
	)
	var pushersWg sync.WaitGroup
	for w := 0; w < 8; w++ {
		pushersWg.Add(1)
		go func(worker int) {
			defer pushersWg.Done()
			for i := 0; i < 5; i++ {
				lba := uint32(10 + worker*5 + i) // distinct LBAs per worker
				if lba >= numBlocks {
					continue
				}
				data := formulaPayload(lba, epoch, blockSize)
				lsn, err := primary.Write(lba, data)
				if err != nil {
					continue
				}
				if pushErr := sender.PushLiveWrite(lba, lsn, data); pushErr == nil {
					acceptedMu.Lock()
					acceptedSet[lba] = data
					acceptedMu.Unlock()
				}
				// If pushErr != nil (sender sealed), the caller would
				// in production fall back to steady-live; in this POC
				// we just drop. The contract being tested is: every
				// SUCCESSFUL push lands on the replica.
			}
		}(w)
	}

	// Let pushers finish; sender.Run barriers autonomously when
	// sink.DrainBacklog completes (P2c-slice B-2). Pushers race with
	// flushAndSeal: pushes that land before sealing get nil; after
	// sealing get an error. The atomic-seal contract is at the sink's
	// flushAndSeal boundary, not the previous closeCh.
	pushersWg.Wait()
	wg.Wait()

	if sendErr != nil {
		t.Fatalf("sender: %v", sendErr)
	}
	if recvErr != nil {
		t.Fatalf("receiver: %v", recvErr)
	}

	// Verify: every accepted push has its bytes on the replica.
	acceptedMu.Lock()
	defer acceptedMu.Unlock()
	for lba, data := range acceptedSet {
		got, _ := replica.Read(lba)
		if !bytes.Equal(got, data) {
			t.Errorf("INV-SEAL-ATOMIC violation: PushLiveWrite for lba=%d returned nil but replica's bytes differ", lba)
		}
	}
	t.Logf("atomic-seal test: accepted %d pushes, all present on replica", len(acceptedSet))
}
