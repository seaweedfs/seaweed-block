package replication

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// G5-5C #2 tests for ReplicaPeer.ProbeIfDegraded + OnProbeAttempt:
//   - Backoff progression (5s → 10s → 20s → 40s → 60s cap)
//   - Reset to base on success
//   - Non-Degraded peers skipped
//   - In-flight guard (single in-flight per peer)
//   - Close() during in-flight: teardown semantics (§1.E (c))
//   - TOCTOU: state change between gate and dispatch
//
// Tests construct ReplicaPeer directly via the struct literal so we
// don't need a real BlockExecutor — the probe state machine is
// orthogonal to ship/barrier/fence transport. SetState is exercised
// through the public API.

// newPeerForProbeTest builds a peer with a fixed ID and the given
// initial state, using compressed cooldown durations so backoff
// progression takes ms not seconds.
func newPeerForProbeTest(t *testing.T, id string, state ReplicaState, base, cap time.Duration) *ReplicaPeer {
	t.Helper()
	p := &ReplicaPeer{
		target: ReplicaTarget{ReplicaID: id, Epoch: 1, EndpointVersion: 1},
		state:  state,
		cooldownCfg: PeerProbeCooldown{
			Base: base,
			Cap:  cap,
		},
	}
	return p
}

// TestPeer_ProbeIfDegraded_OnlyDegradedPasses verifies the §1.A
// "only-on-degraded" discipline: Healthy / CatchingUp / NeedsRebuild
// peers are not eligible for probe dispatch, regardless of cooldown.
func TestPeer_ProbeIfDegraded_OnlyDegradedPasses(t *testing.T) {
	cases := []struct {
		state ReplicaState
		want  bool
	}{
		{ReplicaUnknown, false},
		{ReplicaHealthy, false},
		{ReplicaDegraded, true},
		{ReplicaCatchingUp, false},
		{ReplicaNeedsRebuild, false},
	}
	for _, c := range cases {
		p := newPeerForProbeTest(t, "r1", c.state, 5*time.Millisecond, 50*time.Millisecond)
		got := p.ProbeIfDegraded(time.Now())
		if got != c.want {
			t.Errorf("state=%s: ProbeIfDegraded returned %v, want %v", c.state, got, c.want)
		}
		// Cleanup if we claimed it (test hygiene; a real loop would
		// always pair with OnProbeAttempt).
		if got {
			p.OnProbeAttempt(time.Now(), false)
		}
	}
}

// TestPeer_ProbeIfDegraded_ClosedSkipped verifies that a Closed peer
// is never eligible for probe (§1.E (c) lineage teardown).
func TestPeer_ProbeIfDegraded_ClosedSkipped(t *testing.T) {
	p := newPeerForProbeTest(t, "r1", ReplicaDegraded, 5*time.Millisecond, 50*time.Millisecond)
	// Mark closed via the field directly — Close() requires an
	// executor, which we don't have in this test. The closed flag
	// alone is what gates ProbeIfDegraded.
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()

	if got := p.ProbeIfDegraded(time.Now()); got {
		t.Fatal("closed peer should never be probe-eligible")
	}
}

// TestPeer_ProbeIfDegraded_InFlightGuard verifies that a single
// peer cannot have two probes in flight (INV-G5-5C-SINGLE-INFLIGHT-PER-PEER).
func TestPeer_ProbeIfDegraded_InFlightGuard(t *testing.T) {
	p := newPeerForProbeTest(t, "r1", ReplicaDegraded, 5*time.Millisecond, 50*time.Millisecond)

	now := time.Now()
	if !p.ProbeIfDegraded(now) {
		t.Fatal("first ProbeIfDegraded should claim")
	}
	if !p.IsProbeInFlight() {
		t.Fatal("expected probeInFlight=true after first claim")
	}

	// Second call must be rejected because previous probe is still
	// in flight.
	if p.ProbeIfDegraded(now) {
		t.Fatal("second ProbeIfDegraded should be rejected (in-flight)")
	}

	// Releasing via OnProbeAttempt allows future probes after
	// cooldown.
	p.OnProbeAttempt(now, false)
	if p.IsProbeInFlight() {
		t.Fatal("expected probeInFlight=false after OnProbeAttempt")
	}
}

// TestPeer_OnProbeAttempt_BackoffProgression verifies the
// 5s → 10s → 20s → 40s → 60s (cap) → 60s sequence.
// INV-G5-5C-RECOVERY-BACKOFF.
func TestPeer_OnProbeAttempt_BackoffProgression(t *testing.T) {
	base := 5 * time.Millisecond
	cap := 40 * time.Millisecond // 5 → 10 → 20 → 40 (cap) → 40
	p := newPeerForProbeTest(t, "r1", ReplicaDegraded, base, cap)

	// Use a fixed reference time so deadlines are deterministic.
	t0 := time.Unix(1_700_000_000, 0)

	type step struct {
		failureNum int
		wantDelay  time.Duration
	}
	steps := []step{
		{1, 5 * time.Millisecond},  // base
		{2, 10 * time.Millisecond}, // base * 2
		{3, 20 * time.Millisecond}, // base * 4
		{4, 40 * time.Millisecond}, // base * 8 (would be 40 = cap)
		{5, 40 * time.Millisecond}, // capped
		{6, 40 * time.Millisecond}, // still capped
	}
	for _, s := range steps {
		// claim + fail
		if !p.ProbeIfDegraded(t0) {
			t.Fatalf("step %d: ProbeIfDegraded should claim", s.failureNum)
		}
		p.OnProbeAttempt(t0, false)
		gotEligible := p.ProbeNextEligibleAt()
		gotDelay := gotEligible.Sub(t0)
		if gotDelay != s.wantDelay {
			t.Errorf("step %d (failure #%d): nextEligibleAt-now = %v, want %v",
				s.failureNum, s.failureNum, gotDelay, s.wantDelay)
		}
		// Advance virtual clock past cooldown so next claim succeeds.
		t0 = gotEligible
	}
}

// TestPeer_OnProbeAttempt_SuccessResetsBackoff verifies that a
// successful probe resets the cooldown to Base and clears the
// failure counter (INV-G5-5C-RECOVERY-BACKOFF reset rule).
func TestPeer_OnProbeAttempt_SuccessResetsBackoff(t *testing.T) {
	base := 5 * time.Millisecond
	cap := 80 * time.Millisecond
	p := newPeerForProbeTest(t, "r1", ReplicaDegraded, base, cap)
	t0 := time.Unix(1_700_000_000, 0)

	// Three consecutive failures: 5 → 10 → 20.
	for i := 0; i < 3; i++ {
		if !p.ProbeIfDegraded(t0) {
			t.Fatalf("failure %d: ProbeIfDegraded should claim", i+1)
		}
		p.OnProbeAttempt(t0, false)
		t0 = p.ProbeNextEligibleAt()
	}
	// Verify we reached 20ms backoff before success.
	if !p.ProbeIfDegraded(t0) {
		t.Fatal("ProbeIfDegraded should claim after cooldown elapsed")
	}
	// Now succeed.
	p.OnProbeAttempt(t0, true)

	// After success: nextEligible = t0 + Base, regardless of prior
	// failure count.
	wantEligible := t0.Add(base)
	if got := p.ProbeNextEligibleAt(); !got.Equal(wantEligible) {
		t.Errorf("after success: nextEligibleAt = %v, want %v (delta = %v)",
			got, wantEligible, got.Sub(wantEligible))
	}

	// Next failure should be Base again (counter reset to 0,
	// then ++ to 1, → Base).
	t0 = p.ProbeNextEligibleAt()
	if !p.ProbeIfDegraded(t0) {
		t.Fatal("post-success ProbeIfDegraded should claim")
	}
	p.OnProbeAttempt(t0, false)
	wantNextDelay := base
	gotNextDelay := p.ProbeNextEligibleAt().Sub(t0)
	if gotNextDelay != wantNextDelay {
		t.Errorf("post-success first failure: delay = %v, want %v (counter not reset)",
			gotNextDelay, wantNextDelay)
	}
}

// TestPeer_OnProbeAttempt_ClosedDoesNotUpdateCooldown verifies §1.E (c):
// a peer Close()'d during the in-flight window has its in-flight
// flag cleared by OnProbeAttempt but no cooldown update applied.
// Without this, a delayed OnProbeAttempt could leak state across a
// torn-down peer's lifetime.
func TestPeer_OnProbeAttempt_ClosedDoesNotUpdateCooldown(t *testing.T) {
	base := 5 * time.Millisecond
	cap := 40 * time.Millisecond
	p := newPeerForProbeTest(t, "r1", ReplicaDegraded, base, cap)
	t0 := time.Unix(1_700_000_000, 0)

	if !p.ProbeIfDegraded(t0) {
		t.Fatal("ProbeIfDegraded should claim")
	}
	if !p.IsProbeInFlight() {
		t.Fatal("expected probeInFlight=true")
	}

	// Mark closed — simulates ReplicationVolume.UpdateReplicaSet
	// tearing down this peer mid-probe (§1.E (c) lineage bump).
	p.mu.Lock()
	p.closed = true
	prevEligible := p.probeNextEligible
	p.mu.Unlock()

	// Late OnProbeAttempt — should clear in-flight and return without
	// updating cooldown deadline.
	p.OnProbeAttempt(t0, false)

	if p.IsProbeInFlight() {
		t.Error("expected probeInFlight=false after OnProbeAttempt on closed peer")
	}
	if got := p.ProbeNextEligibleAt(); !got.Equal(prevEligible) {
		t.Errorf("closed peer cooldown was updated: prev=%v got=%v", prevEligible, got)
	}
}

// TestPeer_StateChange_InFlightSafe verifies the TOCTOU acceptance
// criterion (architect G5-5C #3): if a peer transitions out of
// ReplicaDegraded (e.g., engine drives it to CatchingUp on probe
// success) WHILE a probe is in flight, the eventual OnProbeAttempt
// must clear in-flight without disturbing the new state.
func TestPeer_StateChange_InFlightSafe(t *testing.T) {
	base := 5 * time.Millisecond
	cap := 40 * time.Millisecond
	p := newPeerForProbeTest(t, "r1", ReplicaDegraded, base, cap)
	t0 := time.Unix(1_700_000_000, 0)

	if !p.ProbeIfDegraded(t0) {
		t.Fatal("ProbeIfDegraded should claim")
	}

	// Engine drives state to CatchingUp mid-probe (e.g., probe result
	// classified the gap as catch-up; engine emitted StartCatchUp;
	// runtime SetState'd the peer to CatchingUp).
	p.SetState(ReplicaCatchingUp)
	if got := p.State(); got != ReplicaCatchingUp {
		t.Fatalf("expected state=CatchingUp, got %v", got)
	}

	// Late OnProbeAttempt (success or failure — engine owns the
	// state advance, this just clears in-flight).
	p.OnProbeAttempt(t0, true)
	if p.IsProbeInFlight() {
		t.Error("expected probeInFlight=false after OnProbeAttempt")
	}
	// State must NOT have been mutated by OnProbeAttempt.
	if got := p.State(); got != ReplicaCatchingUp {
		t.Errorf("state changed by OnProbeAttempt: got %v, want CatchingUp", got)
	}

	// A fresh ProbeIfDegraded must skip — peer is no longer Degraded.
	if p.ProbeIfDegraded(t0.Add(time.Hour)) {
		t.Error("ProbeIfDegraded on CatchingUp peer should return false")
	}
}

// TestPeer_ProbeIfDegraded_ConcurrentClaims verifies the in-flight
// guard under concurrent contention: only one of N goroutines can
// claim the probe at a time (TOCTOU pinning).
func TestPeer_ProbeIfDegraded_ConcurrentClaims(t *testing.T) {
	base := 5 * time.Millisecond
	cap := 40 * time.Millisecond
	p := newPeerForProbeTest(t, "r1", ReplicaDegraded, base, cap)

	const N = 32
	var claims atomic.Int64
	var wg sync.WaitGroup
	wg.Add(N)
	now := time.Now()
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-start
			if p.ProbeIfDegraded(now) {
				claims.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := claims.Load(); got != 1 {
		t.Fatalf("expected exactly 1 concurrent claim, got %d", got)
	}
}

// TestDefaultProbeCooldownFn_Wires verifies that the default
// CooldownFn wrapper delegates to ProbeIfDegraded on the supplied
// peer with the supplied clock.
func TestDefaultProbeCooldownFn_Wires(t *testing.T) {
	base := 5 * time.Millisecond
	cap := 40 * time.Millisecond
	p := newPeerForProbeTest(t, "r1", ReplicaDegraded, base, cap)
	t0 := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return t0 }

	fn := DefaultProbeCooldownFn(clock)
	if !fn(p) {
		t.Fatal("DefaultProbeCooldownFn should return true for fresh degraded peer")
	}
	if !p.IsProbeInFlight() {
		t.Fatal("expected DefaultProbeCooldownFn to claim in-flight")
	}
}

// TestDefaultProbeResultFn_Wires verifies the default ResultFn
// wrapper delegates to OnProbeAttempt with success = (err == nil).
func TestDefaultProbeResultFn_Wires(t *testing.T) {
	base := 5 * time.Millisecond
	cap := 40 * time.Millisecond
	p := newPeerForProbeTest(t, "r1", ReplicaDegraded, base, cap)
	t0 := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return t0 }

	// Claim first.
	if !p.ProbeIfDegraded(t0) {
		t.Fatal("setup: ProbeIfDegraded should claim")
	}

	fn := DefaultProbeResultFn(clock)
	fn(p, nil) // success

	if p.IsProbeInFlight() {
		t.Error("expected DefaultProbeResultFn to clear in-flight on success")
	}
	want := t0.Add(base)
	if got := p.ProbeNextEligibleAt(); !got.Equal(want) {
		t.Errorf("nextEligibleAt = %v, want %v", got, want)
	}
}
