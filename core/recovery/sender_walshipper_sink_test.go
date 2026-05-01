package recovery

// Completion oracle: recover(a,b) band — NOT recover(a) closure.
// See sw-block/design/recover-semantics-adjustment-plan.md §8.1.

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

// notifyAppendCall captures one NotifyAppend invocation for assertions.
type notifyAppendCall struct {
	lba  uint32
	lsn  uint64
	data []byte
}

// recordingSink is a mock WalShipperSink that captures call order
// + arguments. Tests assert against the recorded log to pin
// delegation ordering.
type recordingSink struct {
	mu sync.Mutex

	startCalls        int
	startFromLSNs     []uint64
	drainCalls        int
	endCalls          int
	notifyAppendCalls []notifyAppendCall
	callOrder         []string // sequence of method names

	// Test injection knobs.
	startErr        error
	drainErr        error
	notifyAppendErr error

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

func (r *recordingSink) NotifyAppend(lba uint32, lsn uint64, data []byte) error {
	r.mu.Lock()
	cp := make([]byte, len(data))
	copy(cp, data)
	r.notifyAppendCalls = append(r.notifyAppendCalls, notifyAppendCall{lba: lba, lsn: lsn, data: cp})
	r.callOrder = append(r.callOrder, "NotifyAppend")
	err := r.notifyAppendErr
	r.mu.Unlock()
	return err
}

func (r *recordingSink) snapshot() (startCalls, drainCalls, endCalls int, order []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := make([]string, len(r.callOrder))
	copy(o, r.callOrder)
	return r.startCalls, r.drainCalls, r.endCalls, o
}

func (r *recordingSink) snapshotNotifyAppend() []notifyAppendCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]notifyAppendCall, len(r.notifyAppendCalls))
	copy(out, r.notifyAppendCalls)
	return out
}

// TestNewSenderWithSink_NilSinkPanics — P2c-slice A: sink is mandatory.
//
// Before P2c, NewSenderWithSink(..., nil) silently promoted to the
// legacy NewSender path. After P2c-slice A there is no legacy path,
// so a nil sink is a programmer error. Constructor panics — clear
// blast radius, fails at construction not at first WAL emit.
func TestNewSenderWithSink_NilSinkPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewSenderWithSink(nil) did not panic; sink must be mandatory after P2c-slice A")
		}
	}()
	coord := NewPeerShipCoordinator()
	store := storage.NewBlockStore(4, 4096)
	prim, replicaEnd := net.Pipe()
	defer prim.Close()
	defer replicaEnd.Close()
	_ = NewSenderWithSink(store, coord, prim, "r1", nil)
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
	var _ WalShipperSink = &senderBacklogSink{}
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

	// P2c-slice B-2: sender.Run barriers as soon as DrainBacklog
	// returns; no closeCh wait. recordingSink.DrainBacklog returns
	// nil immediately, so Run proceeds to the barrier round-trip
	// directly. EndSession fires via defer regardless.
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

// TestSender_PushLiveWrite_RoutesViaSinkNotifyAppend — P2c-slice B-1:
// the single live path. recovery.Sender.PushLiveWrite is the recovery-
// layer entry point for RouteSessionLane writes; after slice B-1 it
// MUST delegate to s.sink.NotifyAppend instead of touching liveQueue
// directly. The sink — production transport WalShipper or bridging
// senderBacklogSink — owns the buffering / ordering discipline.
//
// What this test pins:
//   - PushLiveWrite invokes sink.NotifyAppend exactly once per call.
//   - lba/lsn/data are forwarded verbatim (caller's bytes; sink
//     decides whether to copy).
//   - When sink.NotifyAppend returns error, PushLiveWrite returns it
//     (caller surfaces the failure to the engine).
//
// What this test does NOT pin (slice B-2 / P2d concerns):
//   - Whether liveQueue / drainAndSeal scaffolding is removed.
//   - Wire format of session-live frames (frameWALEntry vs MsgShipEntry).
func TestSender_PushLiveWrite_RoutesViaSinkNotifyAppend(t *testing.T) {
	coord := NewPeerShipCoordinator()
	store := storage.NewBlockStore(4, 4096)
	prim, replicaEnd := net.Pipe()
	defer prim.Close()
	defer replicaEnd.Close()

	sink := newRecordingSink()
	sender := NewSenderWithSink(store, coord, prim, "r1", sink)

	// Three live writes during a hypothetical session window.
	calls := []notifyAppendCall{
		{lba: 0, lsn: 100, data: []byte{0xAA, 0xBB}},
		{lba: 1, lsn: 101, data: []byte{0xCC}},
		{lba: 2, lsn: 102, data: []byte{0xDD, 0xEE, 0xFF}},
	}
	for _, c := range calls {
		if err := sender.PushLiveWrite(c.lba, c.lsn, c.data); err != nil {
			t.Fatalf("PushLiveWrite(lba=%d lsn=%d): %v", c.lba, c.lsn, err)
		}
	}

	got := sink.snapshotNotifyAppend()
	if len(got) != len(calls) {
		t.Fatalf("sink.NotifyAppend called %d times; want %d (PushLiveWrite must route via sink)", len(got), len(calls))
	}
	for i, want := range calls {
		if got[i].lba != want.lba || got[i].lsn != want.lsn {
			t.Errorf("call %d: got lba=%d lsn=%d, want lba=%d lsn=%d",
				i, got[i].lba, got[i].lsn, want.lba, want.lsn)
		}
		if !bytes.Equal(got[i].data, want.data) {
			t.Errorf("call %d: data mismatch: got=%x want=%x", i, got[i].data, want.data)
		}
	}

	// Error propagation: when sink.NotifyAppend errors, PushLiveWrite
	// surfaces the error to the caller.
	sink.mu.Lock()
	sink.notifyAppendErr = errors.New("synthetic NotifyAppend failure")
	sink.mu.Unlock()
	if err := sender.PushLiveWrite(3, 103, []byte{0x42}); err == nil {
		t.Errorf("PushLiveWrite did not surface sink.NotifyAppend error")
	}
}

// TestSenderBacklogSink_NotifyAppend_BuffersForFlushAndSeal — P2c-slice B-2:
// bridging-path discipline (buffer now owned by senderBacklogSink, not Sender).
//
// Until P2d aligns the wire format (dual-lane frameWALEntry vs legacy
// MsgShipEntry), senderBacklogSink cannot ship live writes directly to
// the wire — it would interleave with streamBacklog frames and break
// the receiver's monotonic-LSN check (architect rule 2: "production
// discipline / queue under one mutex").
//
// senderBacklogSink.NotifyAppend therefore buffers into the sink's
// internal queue (under sinkMu) so flushAndSeal — called at the tail
// of DrainBacklog — can ship them in LSN order after streamBacklog
// completes. This test pins that contract: after NotifyAppend, the
// sink's queue holds the entries; flushAndSeal would flush them on
// the wire path.
//
// When P2d swaps in a real transport.WalShipper sink, NotifyAppend
// will route to WalShipper.NotifyAppend (Backlog mode lag-tracking;
// Realtime direct ship under shipMu) — at which point the buffer
// scaffolding here goes too.
func TestSenderBacklogSink_NotifyAppend_BuffersForFlushAndSeal(t *testing.T) {
	coord := NewPeerShipCoordinator()
	store := storage.NewBlockStore(4, 4096)
	prim, replicaEnd := net.Pipe()
	defer prim.Close()
	defer replicaEnd.Close()

	sender := NewSenderWithBacklogRelay(store, coord, prim, "r1")

	// Push two live writes via the public PushLiveWrite path; this
	// goes Sender.PushLiveWrite → sink.NotifyAppend → buffer.
	if err := sender.PushLiveWrite(7, 200, []byte{0x11, 0x22}); err != nil {
		t.Fatalf("PushLiveWrite: %v", err)
	}
	if err := sender.PushLiveWrite(8, 201, []byte{0x33}); err != nil {
		t.Fatalf("PushLiveWrite: %v", err)
	}

	// Buffer accessor: read the sink's internal queue under sinkMu.
	// White-box check — the buffer's location is the bridging-sink's
	// concern; when a real transport.WalShipper sink replaces it (P2d),
	// this test goes too.
	bs, ok := sender.sink.(*senderBacklogSink)
	if !ok {
		t.Fatalf("sink type=%T, want *senderBacklogSink", sender.sink)
	}
	bs.sinkMu.Lock()
	bufLen := len(bs.queue)
	var lsns []uint64
	for _, w := range bs.queue {
		lsns = append(lsns, w.lsn)
	}
	bs.sinkMu.Unlock()

	if bufLen != 2 {
		t.Errorf("sink queue len=%d, want 2 (NotifyAppend must buffer in bridging path)", bufLen)
	}
	if len(lsns) != 2 || lsns[0] != 200 || lsns[1] != 201 {
		t.Errorf("sink queue LSNs=%v, want [200 201]", lsns)
	}
}
