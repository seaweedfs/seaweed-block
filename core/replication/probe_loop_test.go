package replication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// G5-5C §2 #2 lifecycle tests:
//  - Start before run (no panic; no goroutine leak)
//  - Stop while running (clean exit; goroutine returns)
//  - Stop-before-Start (idempotent; no deadlock — CP4B-2 lesson 1)
//  - Zero-interval guard (rejected at construction — CP4B-2 lesson 2)
//  - Callback panic isolation (CP4B-2 lesson 3)
//
// These tests exercise the loop machinery without engaging real
// peers, executors, or adapters — peersFn / probeFn / cooldownFn are
// stubs that record interactions.

func newProbeLoopForTest(
	t *testing.T,
	interval time.Duration,
	peersFn PeerSourceFn,
	probeFn ProbeFn,
	cooldownFn CooldownFn,
) *ProbeLoop {
	t.Helper()
	return newProbeLoopWithResult(t, interval, peersFn, probeFn, cooldownFn, func(_ *ReplicaPeer, _ error) {})
}

func newProbeLoopWithResult(
	t *testing.T,
	interval time.Duration,
	peersFn PeerSourceFn,
	probeFn ProbeFn,
	cooldownFn CooldownFn,
	resultFn ResultFn,
) *ProbeLoop {
	t.Helper()
	cfg := DefaultProbeLoopConfig()
	cfg.Interval = interval
	cfg.CooldownBase = 100 * time.Millisecond
	cfg.CooldownCap = 400 * time.Millisecond
	loop, err := NewProbeLoop(cfg, peersFn, probeFn, cooldownFn, resultFn)
	if err != nil {
		t.Fatalf("NewProbeLoop: %v", err)
	}
	return loop
}

// TestProbeLoop_Lifecycle_StartStop verifies a clean start → tick → stop.
// Goal: no panic, goroutine exits, the probeFn is reachable from the
// loop iteration.
func TestProbeLoop_Lifecycle_StartStop(t *testing.T) {
	var probeCalls atomic.Int64
	peersFn := func() []*ReplicaPeer {
		// One synthetic peer; State is not relevant because cooldownFn
		// always returns true here.
		return []*ReplicaPeer{{target: ReplicaTarget{ReplicaID: "r1"}}}
	}
	probeFn := func(_ context.Context, _ *ReplicaPeer) error {
		probeCalls.Add(1)
		return nil
	}
	cooldownFn := func(_ *ReplicaPeer) bool { return true }

	loop := newProbeLoopForTest(t, 20*time.Millisecond, peersFn, probeFn, cooldownFn)

	if err := loop.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for at least one tick to fire.
	deadline := time.Now().Add(2 * time.Second)
	for probeCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if probeCalls.Load() == 0 {
		t.Fatal("probeFn was never called within 2s")
	}

	loop.Stop()

	// After Stop returns, the loop goroutine must be gone. Verify by
	// observing no further calls.
	beforeStop := probeCalls.Load()
	time.Sleep(80 * time.Millisecond)
	if got := probeCalls.Load(); got != beforeStop {
		t.Fatalf("probeFn called after Stop: before=%d after=%d", beforeStop, got)
	}
}

// TestProbeLoop_Lifecycle_StopBeforeStart verifies CP4B-2 lesson 1:
// Stop called before Start must NOT deadlock. Once stopped before
// start, Start is a no-op (terminal state).
func TestProbeLoop_Lifecycle_StopBeforeStart(t *testing.T) {
	peersFn := func() []*ReplicaPeer { return nil }
	probeFn := func(_ context.Context, _ *ReplicaPeer) error { return nil }
	cooldownFn := func(_ *ReplicaPeer) bool { return true }

	loop := newProbeLoopForTest(t, 50*time.Millisecond, peersFn, probeFn, cooldownFn)

	done := make(chan struct{})
	go func() {
		loop.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop before Start deadlocked")
	}

	// Calling Start after Stop returns nil but does not spawn a goroutine.
	if err := loop.Start(); err != nil {
		t.Fatalf("Start after Stop: %v", err)
	}
	// Calling Stop again is a no-op.
	loop.Stop()
}

// TestProbeLoop_Lifecycle_StopIdempotent verifies multiple Stop calls
// from concurrent goroutines all return cleanly.
func TestProbeLoop_Lifecycle_StopIdempotent(t *testing.T) {
	peersFn := func() []*ReplicaPeer { return nil }
	probeFn := func(_ context.Context, _ *ReplicaPeer) error { return nil }
	cooldownFn := func(_ *ReplicaPeer) bool { return true }

	loop := newProbeLoopForTest(t, 50*time.Millisecond, peersFn, probeFn, cooldownFn)
	if err := loop.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const N = 8
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			loop.Stop()
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent Stop calls deadlocked")
	}
}

// TestProbeLoop_ZeroInterval_Rejected verifies CP4B-2 lesson 2:
// zero or negative interval must be rejected at construction so the
// loop never spins at top speed.
func TestProbeLoop_ZeroInterval_Rejected(t *testing.T) {
	peersFn := func() []*ReplicaPeer { return nil }
	probeFn := func(_ context.Context, _ *ReplicaPeer) error { return nil }
	cooldownFn := func(_ *ReplicaPeer) bool { return true }
	resultFn := func(_ *ReplicaPeer, _ error) {}

	cfg := DefaultProbeLoopConfig()
	cfg.Interval = 0
	if _, err := NewProbeLoop(cfg, peersFn, probeFn, cooldownFn, resultFn); err == nil {
		t.Fatal("zero interval should be rejected at construction")
	}

	cfg.Interval = -1 * time.Second
	if _, err := NewProbeLoop(cfg, peersFn, probeFn, cooldownFn, resultFn); err == nil {
		t.Fatal("negative interval should be rejected at construction")
	}
}

// TestProbeLoop_MaxConcurrentGreaterThanOne_Rejected verifies that
// a config with MaxConcurrent > 1 is rejected at construction
// (G5-5C v0.5 architect-bound: MaxConcurrent=1 only). Avoids leaving
// a knob the runtime silently ignores.
func TestProbeLoop_MaxConcurrentGreaterThanOne_Rejected(t *testing.T) {
	peersFn := func() []*ReplicaPeer { return nil }
	probeFn := func(_ context.Context, _ *ReplicaPeer) error { return nil }
	cooldownFn := func(_ *ReplicaPeer) bool { return true }
	resultFn := func(_ *ReplicaPeer, _ error) {}

	cfg := DefaultProbeLoopConfig()
	cfg.MaxConcurrent = 4
	if _, err := NewProbeLoop(cfg, peersFn, probeFn, cooldownFn, resultFn); err == nil {
		t.Fatal("MaxConcurrent=4 should be rejected at construction in G5-5C v0.5")
	}

	// MaxConcurrent <= 0 normalizes to 1 (silent default), and 1 is
	// the only accepted explicit value.
	cfg.MaxConcurrent = 0
	if _, err := NewProbeLoop(cfg, peersFn, probeFn, cooldownFn, resultFn); err != nil {
		t.Fatalf("MaxConcurrent=0 should normalize to 1: %v", err)
	}
	cfg.MaxConcurrent = 1
	if _, err := NewProbeLoop(cfg, peersFn, probeFn, cooldownFn, resultFn); err != nil {
		t.Fatalf("MaxConcurrent=1 should be accepted: %v", err)
	}
}

// TestProbeLoop_NilResultFn_Rejected verifies the resultFn seam is
// required at construction.
func TestProbeLoop_NilResultFn_Rejected(t *testing.T) {
	peersFn := func() []*ReplicaPeer { return nil }
	probeFn := func(_ context.Context, _ *ReplicaPeer) error { return nil }
	cooldownFn := func(_ *ReplicaPeer) bool { return true }

	cfg := DefaultProbeLoopConfig()
	if _, err := NewProbeLoop(cfg, peersFn, probeFn, cooldownFn, nil); err == nil {
		t.Fatal("nil resultFn should be rejected at construction")
	}
}

// TestProbeLoop_CallbackPanic_Isolated verifies CP4B-2 lesson 3:
// a panic inside probeFn does NOT crash the loop goroutine. The loop
// keeps ticking, just logging the panic.
func TestProbeLoop_CallbackPanic_Isolated(t *testing.T) {
	var calls atomic.Int64
	peersFn := func() []*ReplicaPeer {
		return []*ReplicaPeer{{target: ReplicaTarget{ReplicaID: "r1"}}}
	}
	probeFn := func(_ context.Context, _ *ReplicaPeer) error {
		c := calls.Add(1)
		if c <= 2 {
			panic("simulated probeFn panic")
		}
		return nil
	}
	cooldownFn := func(_ *ReplicaPeer) bool { return true }

	loop := newProbeLoopForTest(t, 20*time.Millisecond, peersFn, probeFn, cooldownFn)
	if err := loop.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer loop.Stop()

	// Wait until the loop has called probeFn at least 3 times. If the
	// panic crashed the goroutine we would never get past 1.
	deadline := time.Now().Add(3 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() < 3 {
		t.Fatalf("probeFn called only %d times — panic likely crashed loop", calls.Load())
	}
}

// TestProbeLoop_OnlyEligiblePeers_AreProbed verifies the cooldownFn
// gate: peers for which cooldownFn returns false are skipped without
// any probeFn call. Pins INV-G5-5C-RECOVERY-BACKOFF (gating happens
// before dispatch) and INV-G5-5C-PROBE-LOOP-001 (loop only acts on
// eligible peers).
func TestProbeLoop_OnlyEligiblePeers_AreProbed(t *testing.T) {
	probed := make(map[string]int)
	var probedMu sync.Mutex

	r1 := &ReplicaPeer{target: ReplicaTarget{ReplicaID: "r1"}}
	r2 := &ReplicaPeer{target: ReplicaTarget{ReplicaID: "r2"}}
	r3 := &ReplicaPeer{target: ReplicaTarget{ReplicaID: "r3"}}

	peersFn := func() []*ReplicaPeer { return []*ReplicaPeer{r1, r2, r3} }
	probeFn := func(_ context.Context, p *ReplicaPeer) error {
		probedMu.Lock()
		probed[p.target.ReplicaID]++
		probedMu.Unlock()
		return nil
	}
	// Only r2 is eligible; r1 and r3 are gated out by cooldownFn.
	cooldownFn := func(p *ReplicaPeer) bool {
		return p.target.ReplicaID == "r2"
	}

	loop := newProbeLoopForTest(t, 20*time.Millisecond, peersFn, probeFn, cooldownFn)
	if err := loop.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer loop.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for {
		probedMu.Lock()
		r2Count := probed["r2"]
		probedMu.Unlock()
		if r2Count >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("r2 was not probed at least twice within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	probedMu.Lock()
	if probed["r1"] != 0 {
		t.Errorf("r1 was probed %d times; expected 0 (cooldownFn=false)", probed["r1"])
	}
	if probed["r3"] != 0 {
		t.Errorf("r3 was probed %d times; expected 0 (cooldownFn=false)", probed["r3"])
	}
	probedMu.Unlock()
}

// TestProbeLoop_NilPeersOK verifies an empty peer set causes no probe
// calls and no panic.
func TestProbeLoop_NilPeersOK(t *testing.T) {
	var calls atomic.Int64
	peersFn := func() []*ReplicaPeer { return nil }
	probeFn := func(_ context.Context, _ *ReplicaPeer) error {
		calls.Add(1)
		return nil
	}
	cooldownFn := func(_ *ReplicaPeer) bool { return true }

	loop := newProbeLoopForTest(t, 20*time.Millisecond, peersFn, probeFn, cooldownFn)
	if err := loop.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	loop.Stop()

	if calls.Load() != 0 {
		t.Fatalf("probeFn called %d times despite empty peer set", calls.Load())
	}
}

// TestProbeLoop_StopCancelsInflightProbe verifies that Stop() unblocks
// a probeFn that is currently waiting on its context. The loop's
// dispatchProbe wires the per-call ctx to stopCh; without that, Stop
// would hang waiting for an in-flight probe.
func TestProbeLoop_StopCancelsInflightProbe(t *testing.T) {
	probeStarted := make(chan struct{}, 1)
	peersFn := func() []*ReplicaPeer {
		return []*ReplicaPeer{{target: ReplicaTarget{ReplicaID: "r1"}}}
	}
	probeFn := func(ctx context.Context, _ *ReplicaPeer) error {
		select {
		case probeStarted <- struct{}{}:
		default:
		}
		// Block until the loop cancels us via Stop.
		<-ctx.Done()
		return ctx.Err()
	}
	cooldownFn := func(_ *ReplicaPeer) bool { return true }

	loop := newProbeLoopForTest(t, 20*time.Millisecond, peersFn, probeFn, cooldownFn)
	if err := loop.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for probeFn to start.
	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("probeFn never started")
	}

	// Stop must return promptly — within ~200 ms — even though the
	// in-flight probe is still blocked on ctx.Done().
	stopReturned := make(chan struct{})
	go func() {
		loop.Stop()
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return — in-flight probe was not cancelled")
	}
}

// TestProbeLoop_ResultFn_ReceivesProbeOutcome verifies the new
// ResultFn seam: every probeFn call produces exactly one resultFn
// invocation, and the err passed to resultFn matches what probeFn
// returned (success path, error path, panic path).
func TestProbeLoop_ResultFn_ReceivesProbeOutcome(t *testing.T) {
	type result struct {
		peerID string
		errMsg string
	}
	results := make(chan result, 16)

	var probeCount atomic.Int64
	peersFn := func() []*ReplicaPeer {
		return []*ReplicaPeer{{target: ReplicaTarget{ReplicaID: "r1"}}}
	}
	probeFn := func(_ context.Context, _ *ReplicaPeer) error {
		switch probeCount.Add(1) {
		case 1:
			return nil // success
		case 2:
			return errors.New("simulated failure")
		default:
			panic("simulated probeFn panic")
		}
	}
	cooldownFn := func(_ *ReplicaPeer) bool { return true }
	resultFn := func(p *ReplicaPeer, err error) {
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		results <- result{peerID: p.target.ReplicaID, errMsg: msg}
	}

	loop := newProbeLoopWithResult(t, 20*time.Millisecond, peersFn, probeFn, cooldownFn, resultFn)
	if err := loop.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer loop.Stop()

	collected := make([]result, 0, 3)
	deadline := time.After(3 * time.Second)
	for len(collected) < 3 {
		select {
		case r := <-results:
			collected = append(collected, r)
		case <-deadline:
			t.Fatalf("collected only %d/3 results: %+v", len(collected), collected)
		}
	}

	// Iteration 1: success → empty errMsg.
	if collected[0].errMsg != "" {
		t.Errorf("iter 1: expected nil err, got %q", collected[0].errMsg)
	}
	// Iteration 2: explicit error.
	if collected[1].errMsg != "simulated failure" {
		t.Errorf("iter 2: expected 'simulated failure', got %q", collected[1].errMsg)
	}
	// Iteration 3: panic must surface as a non-nil err to ResultFn so
	// cooldown progression treats it as a failure.
	if collected[2].errMsg == "" {
		t.Errorf("iter 3: expected panic to surface as non-nil err to resultFn, got nil")
	}
}

// TestProbeLoop_ResultFn_PanicIsolated verifies that a panic inside
// resultFn does NOT crash the loop (CP4B-2 lesson 3 — defensive on
// every callback, not just probeFn).
func TestProbeLoop_ResultFn_PanicIsolated(t *testing.T) {
	var probeCalls atomic.Int64
	peersFn := func() []*ReplicaPeer {
		return []*ReplicaPeer{{target: ReplicaTarget{ReplicaID: "r1"}}}
	}
	probeFn := func(_ context.Context, _ *ReplicaPeer) error {
		probeCalls.Add(1)
		return nil
	}
	cooldownFn := func(_ *ReplicaPeer) bool { return true }
	var resultCalls atomic.Int64
	resultFn := func(_ *ReplicaPeer, _ error) {
		c := resultCalls.Add(1)
		if c <= 2 {
			panic("simulated resultFn panic")
		}
	}

	loop := newProbeLoopWithResult(t, 20*time.Millisecond, peersFn, probeFn, cooldownFn, resultFn)
	if err := loop.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer loop.Stop()

	deadline := time.Now().Add(3 * time.Second)
	for probeCalls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if probeCalls.Load() < 3 {
		t.Fatalf("probeCalls=%d — resultFn panic likely halted loop", probeCalls.Load())
	}
}

// TestProbeLoop_ProbeFnError_DoesNotStop verifies the loop continues
// after a probeFn error (errors are forwarded to cooldownFn for
// backoff progression, not to the loop's lifecycle).
func TestProbeLoop_ProbeFnError_DoesNotStop(t *testing.T) {
	var calls atomic.Int64
	peersFn := func() []*ReplicaPeer {
		return []*ReplicaPeer{{target: ReplicaTarget{ReplicaID: "r1"}}}
	}
	probeFn := func(_ context.Context, _ *ReplicaPeer) error {
		calls.Add(1)
		return errors.New("simulated probe failure")
	}
	cooldownFn := func(_ *ReplicaPeer) bool { return true }

	loop := newProbeLoopForTest(t, 20*time.Millisecond, peersFn, probeFn, cooldownFn)
	if err := loop.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer loop.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() < 3 {
		t.Fatalf("probeFn called only %d times — error likely halted loop", calls.Load())
	}
}
