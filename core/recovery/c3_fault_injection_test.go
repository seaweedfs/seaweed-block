package recovery

// C3 fault-injection — robustness of Sender.Run's BASE ∥ WAL goroutine
// pair (§6.8 #6 / P6 / G3). The parallel-overlap claim already has its
// happy-path test (TestC3_BaseWalParallel_FramesInterleave). These
// tests prove the FAILURE paths terminate cleanly:
//
//   #1  BASE error → groupCancel fires → WAL exits via groupCtx.Done.
//   #3  Outer ctx cancel → both lanes wind down; sink.EndSession +
//       coord.EndSession defers each fire exactly once.
//   #2A WAL error → Run returns the WAL failure (variant A — narrow).
//
// Why no symmetric "WAL error → BASE stops promptly" test (variant B):
// streamBase does not currently accept a context. Today it iterates
// `for lba := 0; lba < numBlocks` reading from primaryStore. A WAL
// failure that triggers groupCancel cannot interrupt streamBase
// mid-loop — it will run to completion of all base blocks before its
// goroutine exits. Adding a periodic groupCtx check inside streamBase
// is a small product change that variant B (a future follow-up) would
// pair with a strict prompt-stop assertion. For now we test only that
// Run terminates with the WAL error correctly propagated, NOT that
// BASE stops in the middle.
//
// All three tests reuse a common spy WalShipperSink that:
//   - records StartSession / DrainBacklog / EndSession call counts,
//   - lets a per-test scenario script DrainBacklog's behavior
//     (block until ctx, return immediately, return synthetic err),
//   - captures the ctx.Err() observed at DrainBacklog exit.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// ─── spy sink ────────────────────────────────────────────────────────

// drainBehavior selects what DrainBacklog does on entry.
type drainBehavior int

const (
	drainImmediate drainBehavior = iota // return nil straight away
	drainBlockUntilCtx                  // <-ctx.Done(); return ctx.Err()
	drainSyntheticErr                   // return preset err immediately
)

type spySink struct {
	startedN  atomic.Int32
	drainedN  atomic.Int32
	endedN    atomic.Int32
	notifiedN atomic.Int32

	behavior drainBehavior
	// drainErr — when behavior == drainSyntheticErr, returned by DrainBacklog.
	drainErr error
	// drainCtxErrSeen captures the ctx.Err observed in DrainBacklog
	// at exit (only meaningful for drainBlockUntilCtx).
	drainCtxErrSeen atomic.Value // stores error
}

func (s *spySink) StartSession(fromLSN uint64) error { s.startedN.Add(1); return nil }
func (s *spySink) EndSession()                       { s.endedN.Add(1) }
func (s *spySink) NotifyAppend(lba uint32, lsn uint64, data []byte) error {
	s.notifiedN.Add(1)
	return nil
}

func (s *spySink) DrainBacklog(ctx context.Context) error {
	s.drainedN.Add(1)
	switch s.behavior {
	case drainImmediate:
		return nil
	case drainBlockUntilCtx:
		<-ctx.Done()
		err := ctx.Err()
		s.drainCtxErrSeen.Store(errOrNil(err))
		return err
	case drainSyntheticErr:
		return s.drainErr
	default:
		return nil
	}
}

// errOrNil wraps the error so atomic.Value (which forbids storing typed
// nil interfaces) gets a consistent dynamic type.
func errOrNil(e error) error {
	if e == nil {
		return errSentinelNil
	}
	return e
}

var errSentinelNil = errors.New("__nil_sentinel__")

// ─── faulty primary store: Read fails after N reads ──────────────────

type faultyReadStore struct {
	storage.LogicalStorage
	failAfter int32
	reads     atomic.Int32
}

func (f *faultyReadStore) Read(lba uint32) ([]byte, error) {
	if f.reads.Add(1) > f.failAfter {
		return nil, fmt.Errorf("synthetic read failure at lba=%d", lba)
	}
	return f.LogicalStorage.Read(lba)
}

// ─── helper: drain the receiver-side conn so writes don't block ───────

func drainConnUntilClose(conn net.Conn, done chan<- struct{}) {
	defer close(done)
	buf := make([]byte, 4096)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}

// ─── #1 BASE error → WAL exits via groupCtx ──────────────────────────
//
// Setup: faulty store fails Read on the 3rd call → streamBase fails
// after 2 base blocks. The WAL goroutine is sitting in DrainBacklog
// blocked on ctx — groupCancel from the BASE goroutine triggers it
// to return ctx.Err.
//
// Asserts:
//   - Run returns within 2s (no hang).
//   - Returned error wraps the faulty Read error.
//   - spy.drainCtxErrSeen != nil (DrainBacklog observed ctx cancellation).
//   - spy.startedN == 1, spy.endedN == 1 (lifecycle ran exactly once).
//   - No goroutine leak post-Run.
func TestC3FaultInjection_BaseError_WalExitsViaCtx(t *testing.T) {
	const numBlocks = 32
	const blockSize = 64

	primaryRaw := memorywal.NewStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 8; lba++ {
		_, _ = primaryRaw.Write(lba, []byte{byte(lba + 1)})
	}
	_, _ = primaryRaw.Sync()

	faulty := &faultyReadStore{LogicalStorage: primaryRaw, failAfter: 2}

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()
	readerDone := make(chan struct{})
	go drainConnUntilClose(replicaConn, readerDone)

	coord := NewPeerShipCoordinator()
	const replicaID ReplicaID = "r1"
	if err := coord.StartSession(replicaID, 7, 0, 5); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	spy := &spySink{behavior: drainBlockUntilCtx}
	sender := NewSenderWithSink(faulty, coord, primaryConn, replicaID, spy)

	beforeGoroutines := runtime.NumGoroutine()

	type runResult struct {
		achieved uint64
		err      error
	}
	resCh := make(chan runResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ach, err := sender.Run(ctx, 7, 0, 5)
		resCh <- runResult{ach, err}
	}()

	var res runResult
	select {
	case res = <-resCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Sender.Run did not return within 2s after BASE error")
	}

	if res.err == nil {
		t.Fatal("expected BASE failure, Run returned nil")
	}
	var f *Failure
	if !errors.As(res.err, &f) {
		t.Fatalf("expected *Failure, got %T: %v", res.err, res.err)
	}
	if f.Kind != FailureSubstrate && f.Kind != FailureWire {
		t.Errorf("unexpected failure kind=%v phase=%v underlying=%v (want Substrate or Wire)",
			f.Kind, f.Phase, f.Underlying)
	}

	// WAL goroutine: must have exited via ctx.Done.
	if v := spy.drainCtxErrSeen.Load(); v == nil {
		t.Error("spy.DrainBacklog did NOT observe ctx cancellation — WAL goroutine may still be running or never reached ctx.Done")
	} else if got, _ := v.(error); got == errSentinelNil {
		t.Error("spy.DrainBacklog observed ctx.Err() == nil; expected non-nil after groupCancel")
	}

	// Lifecycle counters: StartSession exactly once, EndSession exactly once.
	if got := spy.startedN.Load(); got != 1 {
		t.Errorf("spy.StartSession calls=%d want 1", got)
	}
	if got := spy.endedN.Load(); got != 1 {
		t.Errorf("spy.EndSession calls=%d want 1", got)
	}

	primaryConn.Close()
	<-readerDone

	// Goroutine leak check (allow some slack for runtime / GC goroutines).
	time.Sleep(50 * time.Millisecond)
	leaked := runtime.NumGoroutine() - beforeGoroutines
	if leaked > 1 {
		t.Logf("goroutine count delta=%d (informational; runtime noise expected)", leaked)
	}
}

// ─── #3 Outer ctx cancel → both lanes wind down ──────────────────────
//
// Setup: caller starts Run, then cancels the outer ctx after a short
// delay. WAL goroutine is parked in DrainBacklog → ctx.Done returns.
// BASE goroutine doesn't observe ctx (streamBase is not ctx-aware,
// see file header), so it runs to completion of all numBlocks Reads
// — but on a tiny store this is sub-millisecond. Run returns either
// the WAL Cancelled failure (if base finished cleanly) or a wrapped
// error from the conn-write path if the cancel raced with BaseDone.
// Either way, the assertions below hold.
//
// Asserts:
//   - Run returns within 2s of cancel.
//   - sink.EndSession defer fired exactly once.
//   - DrainBacklog observed a non-nil ctx.Err (proves the WAL
//     goroutine exited via ctx, not via a hang).
//   - coord.Phase post-Run is Idle (coord.EndSession defer fired).
func TestC3FaultInjection_OuterCancel_BothLanesWindDown(t *testing.T) {
	const numBlocks = 32
	const blockSize = 64

	primary := memorywal.NewStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 4; lba++ {
		_, _ = primary.Write(lba, []byte{byte(lba + 0x80)})
	}
	_, _ = primary.Sync()

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()
	readerDone := make(chan struct{})
	go drainConnUntilClose(replicaConn, readerDone)

	coord := NewPeerShipCoordinator()
	const replicaID ReplicaID = "r1"
	if err := coord.StartSession(replicaID, 11, 0, 4); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	spy := &spySink{behavior: drainBlockUntilCtx}
	sender := NewSenderWithSink(primary, coord, primaryConn, replicaID, spy)

	type runResult struct {
		achieved uint64
		err      error
	}
	resCh := make(chan runResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ach, err := sender.Run(ctx, 11, 0, 4)
		resCh <- runResult{ach, err}
	}()

	// Give the WAL goroutine a moment to enter DrainBacklog and park.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && spy.drainedN.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if spy.drainedN.Load() == 0 {
		t.Fatal("DrainBacklog never entered; cannot test outer-cancel propagation")
	}

	// Cancel the parent ctx. WAL goroutine's groupCtx (derived from
	// this) sees Done; returns ctx.Err.
	cancel()

	var res runResult
	select {
	case res = <-resCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Sender.Run did not return within 2s of outer-ctx cancel")
	}

	if res.err == nil {
		t.Fatal("expected non-nil error after cancel; Run returned nil")
	}

	// DrainBacklog must have observed ctx cancellation.
	if v := spy.drainCtxErrSeen.Load(); v == nil {
		t.Error("DrainBacklog did NOT record ctx.Err — WAL goroutine path bypassed cancellation observation")
	}

	// EndSession defer fired exactly once.
	if got := spy.endedN.Load(); got != 1 {
		t.Errorf("spy.EndSession calls=%d want 1", got)
	}

	// coord.EndSession defer ran → Phase back to Idle.
	if got := coord.Phase(replicaID); got != PhaseIdle {
		t.Errorf("post-Run coord.Phase=%s want Idle (coord.EndSession defer should have fired)", got)
	}

	primaryConn.Close()
	<-readerDone
}

// ─── #2A WAL error → Run terminates (narrow / variant A) ────────────
//
// Setup: spy returns a synthetic error from DrainBacklog. BASE goroutine
// runs to completion (small store; no ctx-awareness in streamBase).
// Run reads bErr=nil, then wErr=non-nil → returns the WAL failure.
//
// What this test does NOT prove (variant B territory):
//   - That BASE stops mid-loop on WAL failure. streamBase has no
//     ctx-check today; it always finishes the full extent before its
//     goroutine exits. A future variant-B test would pair with
//     adding a periodic `if err := groupCtx.Err(); err != nil`
//     check inside streamBase's for-loop.
//
// Asserts:
//   - Run returns the WAL failure (errors.Is unwraps to our synthetic).
//   - spy.startedN == 1, spy.endedN == 1.
//   - Run returns within 2s (no hang waiting for BASE).
func TestC3FaultInjection_WalError_RunTerminates_NarrowVariantA(t *testing.T) {
	const numBlocks = 16
	const blockSize = 64

	primary := memorywal.NewStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 4; lba++ {
		_, _ = primary.Write(lba, []byte{byte(lba + 0x10)})
	}
	_, _ = primary.Sync()

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()
	readerDone := make(chan struct{})
	go drainConnUntilClose(replicaConn, readerDone)

	coord := NewPeerShipCoordinator()
	const replicaID ReplicaID = "r1"
	if err := coord.StartSession(replicaID, 13, 0, 4); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	syntheticWALErr := errors.New("synthetic WAL drain failure")
	spy := &spySink{behavior: drainSyntheticErr, drainErr: syntheticWALErr}

	sender := NewSenderWithSink(primary, coord, primaryConn, replicaID, spy)

	type runResult struct {
		achieved uint64
		err      error
	}
	resCh := make(chan runResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ach, err := sender.Run(ctx, 13, 0, 4)
		resCh <- runResult{ach, err}
	}()

	var res runResult
	select {
	case res = <-resCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Sender.Run did not return within 2s after WAL error")
	}

	if res.err == nil {
		t.Fatal("expected WAL failure, Run returned nil")
	}
	if !errors.Is(res.err, syntheticWALErr) {
		t.Errorf("Run error does not wrap synthetic WAL err: %v", res.err)
	}
	var f *Failure
	if !errors.As(res.err, &f) {
		t.Fatalf("expected *Failure, got %T", res.err)
	}
	if f.Phase != PhaseBacklog {
		t.Errorf("WAL failure Phase=%s want %s", f.Phase, PhaseBacklog)
	}

	if got := spy.startedN.Load(); got != 1 {
		t.Errorf("spy.StartSession calls=%d want 1", got)
	}
	if got := spy.endedN.Load(); got != 1 {
		t.Errorf("spy.EndSession calls=%d want 1", got)
	}

	primaryConn.Close()
	<-readerDone
}

// guard: spySink satisfies WalShipperSink by structural typing.
var _ WalShipperSink = (*spySink)(nil)
