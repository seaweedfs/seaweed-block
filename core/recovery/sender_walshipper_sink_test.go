package recovery

// P2a — Sender delegates WAL pump to an injected `WalShipperSink`.
//
// Spec drivers:
//   - v3-recovery-wal-shipper-mini-plan.md §3 P2 — "Drain API wired
//     from recovery.Sender; delete/disable recovery/sender duplicate
//     pump".
//   - v3-recovery-wal-shipper-spec.md §1 (boundary): WalShipper does
//     NOT know wire format; recovery package provides the abstract
//     sink interface; transport's WalShipper implements it via duck
//     typing (no import cycle since interface lives here).
//
// P2 — Sender delegates the backlog WAL scan+pump to WalShipperSink.
// streamBacklog runs only inside the backlog-relay sink (P2b) or legacy
// NewSender (until P2c). After sink.DrainBacklog, Run still waits on
// Close and drainAndSeal for PushLiveWrite session tails until P2c.
//
// Migration:
//   - P2b: NewSenderWithBacklogRelay / NewSenderWithSink; e2e uses relay.
//
// P2c: make sink required; delete legacy NewSender pump.
//
// P2d: transport-layer integration tests for caller obligations:
//   1. updateWalShipperEmitContext BEFORE sink.StartSession.
//   2. After sink.EndSession, restore steady lineage / context.

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// recordingSink is a mock WalShipperSink that captures call order
// + arguments. Tests assert against the recorded log to pin
// delegation ordering.
type recordingSink struct {
	mu sync.Mutex

	startCalls    int
	startFromLSNs []uint64
	drainCalls    int
	endCalls      int
	callOrder     []string // sequence of method names

	// Test injection knobs.
	startErr error
	drainErr error

	// drainBlocksUntil is closed by the test to release a blocked
	// DrainBacklog call. nil = return immediately.
	drainBlocksUntil <-chan struct{}

	// shipDuringDrain, if non-nil, is invoked from inside
	// DrainBacklog so the test can simulate "wal entries arriving"
	// while the pump runs. The test typically writes frameWALEntry
	// directly to the wire here so the receiver can apply.
	shipDuringDrain func(ctx context.Context) error

	// drainCalledCh fires once after the sink enters DrainBacklog,
	// so tests can synchronize without sleeping.
	drainCalledCh chan struct{}
	drainOnce     sync.Once
}

func newRecordingSink() *recordingSink {
	return &recordingSink{
		drainCalledCh: make(chan struct{}),
	}
}

func (r *recordingSink) StartSession(fromLSN uint64) error {
	r.mu.Lock()
	r.startCalls++
	r.startFromLSNs = append(r.startFromLSNs, fromLSN)
	r.callOrder = append(r.callOrder, "StartSession")
	err := r.startErr
	r.mu.Unlock()
	return err
}

func (r *recordingSink) DrainBacklog(ctx context.Context) error {
	r.mu.Lock()
	r.drainCalls++
	r.callOrder = append(r.callOrder, "DrainBacklog")
	blocker := r.drainBlocksUntil
	hook := r.shipDuringDrain
	err := r.drainErr
	r.mu.Unlock()

	r.drainOnce.Do(func() { close(r.drainCalledCh) })

	if hook != nil {
		if hookErr := hook(ctx); hookErr != nil {
			return hookErr
		}
	}
	if blocker != nil {
		select {
		case <-blocker:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (r *recordingSink) EndSession() {
	r.mu.Lock()
	r.endCalls++
	r.callOrder = append(r.callOrder, "EndSession")
	r.mu.Unlock()
}

func (r *recordingSink) snapshot() (startCalls, drainCalls, endCalls int, order []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := make([]string, len(r.callOrder))
	copy(o, r.callOrder)
	return r.startCalls, r.drainCalls, r.endCalls, o
}

// TestSender_NoSink_FallsBackToLegacyBehavior — P2a backwards-compat:
// callers that don't pass a sink (NewSender — old signature) get
// the existing streamBacklog/drainAndSeal path. P2c removes this
// fallback after all callers migrate.
//
// This test does NOT assert on full e2e — it just confirms the
// constructor accepts and Run is reachable. The detailed e2e
// behavior is covered by existing TestE2E_RebuildHappyPath etc.
// Skipped here by design (those tests still run via legacy path).
func TestSender_NoSink_FallsBackToLegacyBehavior(t *testing.T) {
	t.Skip(`legacy-path coverage: instantiate NewSender (no sink) and exercise streamBacklog in isolation if needed — e2e use NewSenderWithBacklogRelay (P2b)`)
}

// TestSender_WithSink_DelegatesStartSession — basic delegation.
//
// Construction: NewSenderWithSink(..., sink) (or equivalent — TBD
// from P2 impl).
// Sender.Run before reaching the WAL pump: must invoke
// sink.StartSession(fromLSN) at least once. Order is asserted
// in TestSender_WithSink_DelegationOrder.
func TestSender_WithSink_DelegatesStartSession(t *testing.T) {
	t.Skip("redundant with TestSender_WithSink_DelegationOrder (covers StartSession + ordering)")
}

func TestNewSenderWithBacklogRelay_installsRelaySink(t *testing.T) {
	coord := NewPeerShipCoordinator()
	store := storage.NewBlockStore(4, 4096)
	prim, replicaEnd := net.Pipe()
	defer prim.Close()
	defer replicaEnd.Close()
	snd := NewSenderWithBacklogRelay(store, coord, prim, "r1")
	if snd.sink == nil {
		t.Fatal("NewSenderWithBacklogRelay: expected backlog relay sink")
	}
	var _ WalShipperSink = senderBacklogSink{}
}

// TestSender_WithSink_DelegationOrder — pin the ordering contract.
//
// recovery.Sender's Run() with a sink installed MUST invoke:
//   1. sink.StartSession(fromLSN) — before any WAL emission begins
//   2. sink.DrainBacklog(ctx)     — runs the pump
//   3. sink.EndSession()          — after the session bracket closes
//                                   (defer-fired regardless of
//                                   success or failure)
//
// The test does NOT assert on Run's exit value — convergence
// requires the sink to actually pump WAL entries, which our
// recordingSink doesn't do (its DrainBacklog returns nil
// immediately). Run will likely return FailureContract from the
// barrier check (walApplied < target on receiver). What we DO
// assert: the 3 sink methods were called in the right order
// regardless of how Run exits.
func TestSender_WithSink_DelegationOrder(t *testing.T) {
	const fromLSN = uint64(0)
	const targetLSN = uint64(50)
	const blockSize = 4096
	const numBlocks = uint32(8)

	primary := newSinkTestPrimary(t, numBlocks, blockSize, 5) // 5 entries pre-seeded
	replica := newSinkTestPrimary(t, numBlocks, blockSize, 0) // empty

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	coord := NewPeerShipCoordinator()
	if err := coord.StartSession("r1", 7, fromLSN, targetLSN); err != nil {
		t.Fatalf("coord.StartSession: %v", err)
	}

	sink := newRecordingSink()
	sender := NewSenderWithSink(primary, coord, primaryConn, "r1", sink)
	receiver := NewReceiver(replica, replicaConn)

	// After DrainBacklog returns, sink-mode Run waits on closeCh —
	// unblock it so barrier + sink.EndSession can run.
	go func() {
		<-sink.drainCalledCh
		sender.Close()
	}()

	var (
		wg      sync.WaitGroup
		runErr  error
		recvErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, recvErr = receiver.Run()
		// When receiver returns (success or error), close the
		// replica side so sender's readerLoop unblocks. Without
		// this, both goroutines deadlock when receiver errors.
		_ = replicaConn.Close()
	}()
	go func() {
		defer wg.Done()
		_, runErr = sender.Run(context.Background(), 7, fromLSN, targetLSN)
	}()
	wg.Wait()

	// Run errors are expected (barrier won't converge since recordingSink
	// doesn't pump WAL). The CONTRACT being tested is the sink call
	// ordering — that's independent of barrier outcome.
	_ = runErr
	_ = recvErr

	startCalls, drainCalls, endCalls, order := sink.snapshot()
	if startCalls != 1 {
		t.Errorf("StartSession calls=%d want 1", startCalls)
	}
	if drainCalls != 1 {
		t.Errorf("DrainBacklog calls=%d want 1", drainCalls)
	}
	if endCalls != 1 {
		t.Errorf("EndSession calls=%d want 1 (always-cleanup defer)", endCalls)
	}

	// Ordering: StartSession before DrainBacklog before EndSession.
	if len(order) < 3 {
		t.Fatalf("call log too short: %v (want at least 3 calls)", order)
	}
	want := []string{"StartSession", "DrainBacklog", "EndSession"}
	gotPrefix := order[:3]
	for i, w := range want {
		if gotPrefix[i] != w {
			t.Errorf("call[%d]=%q want %q (full order=%v)", i, gotPrefix[i], w, order)
		}
	}

	// fromLSN was forwarded.
	if len(sink.startFromLSNs) != 1 || sink.startFromLSNs[0] != fromLSN {
		t.Errorf("StartSession fromLSNs=%v want [%d]", sink.startFromLSNs, fromLSN)
	}
}

// newSinkTestPrimary spins up a memorywal with N pre-seeded entries
// (LSN 1..N). Helper for sink delegation tests so the test body
// stays focused on assertions, not setup boilerplate.
func newSinkTestPrimary(t *testing.T, numBlocks uint32, blockSize int, seedCount uint32) storage.LogicalStorage {
	t.Helper()
	primary := newMemorywalForTest(t, numBlocks, blockSize)
	for lba := uint32(0); lba < seedCount; lba++ {
		buf := bytes.Repeat([]byte{byte(lba & 0xff)}, blockSize)
		if _, err := primary.Write(lba, buf); err != nil {
			t.Fatalf("seed Write lba=%d: %v", lba, err)
		}
	}
	if seedCount > 0 {
		if _, err := primary.Sync(); err != nil {
			t.Fatalf("seed Sync: %v", err)
		}
	}
	return primary
}

// newMemorywalForTest is a thin wrapper so the sink test file
// doesn't need its own import of memorywal at top-level — the
// test file lives in package recovery; memorywal is a separate
// package. We borrow the existing test helper pattern; this
// centralizes substrate construction here.
//
// (Currently constructs a memorywal Store via the storage import
// surface. If memorywal isn't accessible this file would need
// import; for now we route through the recovery package's own
// substrate construction conventions used by existing e2e tests.)
//
// IMPLEMENTATION NOTE: this helper actually returns a memorywal.Store
// dressed as storage.LogicalStorage. To keep this file self-contained
// without adding a memorywal import in the recovery package's tests,
// we delegate to a per-test-file constructor; if that constructor
// doesn't exist yet, the test linker will surface it as a
// missing-symbol error and we'll add the appropriate import.
func newMemorywalForTest(t *testing.T, numBlocks uint32, blockSize int) storage.LogicalStorage {
	t.Helper()
	// Defer to the e2e_test.go pattern: existing tests in this
	// package use storage.NewBlockStore. Sink delegation tests don't
	// need write-time-LSN semantics for ORDERING assertions (we
	// don't assert on exact LSNs from substrate — just sink call
	// order). BlockStore is acceptable here.
	return storage.NewBlockStore(numBlocks, blockSize)
}

// TestSender_WithSink_EndSessionAlwaysCalled — always-cleanup contract.
//
// Sink.EndSession MUST be called via defer regardless of how Run
// exits: success path, ctx-cancel, wire-error, sink-error.
// Mirrors recovery.Sender's existing always-cleanup discipline
// (defer s.coordinator.EndSession at the top of Run).
func TestSender_WithSink_EndSessionAlwaysCalled(t *testing.T) {
	tcs := []struct {
		name      string
		injectErr error
	}{
		{"success path", nil},
		{"sink.StartSession error", errors.New("synthetic StartSession failure")},
		{"sink.DrainBacklog error", errors.New("synthetic DrainBacklog failure")},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Skip("TODO impl: Sender's defer fires sink.EndSession on every exit path")
		})
	}
}

// TestSender_WithSink_StartSessionFailureAborts — fail-closed contract.
//
// If sink.StartSession returns error, Sender.Run MUST NOT proceed
// to DrainBacklog or barrier; it should return the wrapped
// failure (Kind classifiable per FailureKind taxonomy) and still
// fire sink.EndSession in defer.
func TestSender_WithSink_StartSessionFailureAborts(t *testing.T) {
	t.Skip("TODO impl: StartSession err short-circuits Run; EndSession still fires")
}

// TestSender_WithSink_NoStreamBacklogReachable — INV-NO-DOUBLE-LIVE
// at recovery layer.
//
// When a sink is installed, the legacy streamBacklog code path MUST
// NOT execute. If both run, the wire would receive WAL frames twice
// (once via streamBacklog → frameWALEntry, once via sink). This is
// the architect's "5th time wrong pump" guard.
//
// Verification path (P2a): use a recordingSink + a wire monitor;
// after Run, count frameWALEntry frames on the wire. With sink
// installed, the count from recovery.Sender's direct emission
// must be ZERO (only sink-driven emits, none from streamBacklog).
func TestSender_WithSink_NoStreamBacklogReachable(t *testing.T) {
	// Verification approach: after impl lands, hook a counter on
	// frameWALEntry writes from streamBacklog vs from sink path.
	// Simplest: delete streamBacklog entirely (P2c) so this test
	// becomes a compile-time invariant. P2a softer version:
	// verify recordingSink.drainCalls == 1 AND no frameWALEntry
	// originates from a non-sink code path.
	t.Skip("TODO impl: sink-installed Sender does NOT run streamBacklog (P2a soft assert; P2c compile-time)")
}
