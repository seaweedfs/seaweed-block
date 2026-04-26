package engine

import (
	"testing"
	"time"
)

// T4c-pre-B engine command model upgrade tests.
//
// Per design memo §7a (architect rounds 29-30): unified `StartRecovery`
// + per-content-kind `RecoveryRuntimePolicy`. These tests pin the
// envelope defaults and the command shape; they are the engine-side
// receipt that T4c-pre-B has landed.

func TestStartRecovery_CommandKind(t *testing.T) {
	cmd := StartRecovery{
		ReplicaID:       "r1",
		Epoch:           7,
		EndpointVersion: 2,
		TargetLSN:       1000,
		ContentKind:     RecoveryContentWALDelta,
		RuntimePolicy:   DefaultRuntimePolicyFor(RecoveryContentWALDelta),
	}
	if got := CommandKind(cmd); got != "StartRecovery" {
		t.Fatalf("CommandKind: want StartRecovery, got %q", got)
	}
}

func TestDefaultRuntimePolicyFor_WALDelta(t *testing.T) {
	got := DefaultRuntimePolicyFor(RecoveryContentWALDelta)
	if got.ExpectedDurationClass != DurationShort {
		t.Errorf("WALDelta DurationClass = %q, want %q", got.ExpectedDurationClass, DurationShort)
	}
	if got.CancellationMode != CancelOnTimeout {
		t.Errorf("WALDelta CancellationMode = %q, want %q", got.CancellationMode, CancelOnTimeout)
	}
	if got.Timeout <= 0 {
		t.Errorf("WALDelta Timeout = %v, want > 0 (CancelOnTimeout requires positive timeout)", got.Timeout)
	}
	if got.ProgressCadence <= 0 || got.ProgressCadence > got.Timeout {
		t.Errorf("WALDelta ProgressCadence = %v, want 0 < cadence <= timeout=%v",
			got.ProgressCadence, got.Timeout)
	}
}

func TestDefaultRuntimePolicyFor_FullExtent(t *testing.T) {
	got := DefaultRuntimePolicyFor(RecoveryContentFullExtent)
	if got.ExpectedDurationClass != DurationLong {
		t.Errorf("FullExtent DurationClass = %q, want %q", got.ExpectedDurationClass, DurationLong)
	}
	if got.CancellationMode != CancelOnProgressStall {
		t.Errorf("FullExtent CancellationMode = %q, want %q", got.CancellationMode, CancelOnProgressStall)
	}
	// Memo §7a.1a: rebuilds may legitimately run 30min+; no fixed
	// timeout. Stall-driven cancellation handles unbounded runtime.
	if got.Timeout != 0 {
		t.Errorf("FullExtent Timeout = %v, want 0 (stall-driven, not timeout-driven)", got.Timeout)
	}
	if got.ProgressCadence <= 0 {
		t.Errorf("FullExtent ProgressCadence = %v, want > 0", got.ProgressCadence)
	}
}

func TestDefaultRuntimePolicyFor_PartialLBA(t *testing.T) {
	got := DefaultRuntimePolicyFor(RecoveryContentPartialLBA)
	if got.ExpectedDurationClass != DurationArchiveBound {
		t.Errorf("PartialLBA DurationClass = %q, want %q", got.ExpectedDurationClass, DurationArchiveBound)
	}
	if got.CancellationMode != CancelOnProgressStall {
		t.Errorf("PartialLBA CancellationMode = %q, want %q", got.CancellationMode, CancelOnProgressStall)
	}
}

func TestDefaultRuntimePolicyFor_UnknownKind(t *testing.T) {
	got := DefaultRuntimePolicyFor(RecoveryContentKind("does-not-exist"))
	zero := RecoveryRuntimePolicy{}
	if got != zero {
		t.Errorf("unknown kind: want zero policy, got %+v", got)
	}
}

// Memo §7a.1a "duration class is observable" — once labeled, the
// labels must be the documented set so observability tooling can rely
// on them. This pins the public string values.
func TestDurationClass_Labels(t *testing.T) {
	cases := []struct{ got, want string }{
		{string(DurationShort), "short"},
		{string(DurationLong), "long"},
		{string(DurationArchiveBound), "archive_bound"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("duration class label = %q, want %q", c.got, c.want)
		}
	}
}

func TestCancellationMode_Labels(t *testing.T) {
	cases := []struct{ got, want string }{
		{string(CancelOnTimeout), "timeout"},
		{string(CancelOnExplicit), "explicit"},
		{string(CancelOnProgressStall), "progress_stall"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("cancellation mode label = %q, want %q", c.got, c.want)
		}
	}
}

func TestRecoveryContentKind_Labels(t *testing.T) {
	cases := []struct{ got, want string }{
		{string(RecoveryContentWALDelta), "wal_delta"},
		{string(RecoveryContentFullExtent), "full_extent"},
		{string(RecoveryContentPartialLBA), "partial_lba"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("content kind label = %q, want %q", c.got, c.want)
		}
	}
}

// Pin: per-content-kind defaults are independent — changing one MUST
// NOT silently change another (memo §7a.1a explicitly requires
// per-kind separation, not a shared default).
func TestDefaultRuntimePolicyFor_PerKindIndependence(t *testing.T) {
	wal := DefaultRuntimePolicyFor(RecoveryContentWALDelta)
	full := DefaultRuntimePolicyFor(RecoveryContentFullExtent)

	if wal.CancellationMode == full.CancellationMode {
		t.Errorf("wal_delta and full_extent share cancellation mode (%q) — memo §7a.1a requires distinct envelopes",
			wal.CancellationMode)
	}
	if wal.ExpectedDurationClass == full.ExpectedDurationClass {
		t.Errorf("wal_delta and full_extent share duration class (%q) — memo §7a.1a requires distinct envelopes",
			wal.ExpectedDurationClass)
	}
}

// Pin: ProgressCadence must be a sub-multiple of any positive
// Timeout. If Timeout is 30s and Cadence is 60s, the timeout fires
// before the first cadence — silent envelope contradiction.
func TestDefaultRuntimePolicyFor_TimeoutCadenceConsistency(t *testing.T) {
	for _, kind := range []RecoveryContentKind{
		RecoveryContentWALDelta,
		RecoveryContentFullExtent,
		RecoveryContentPartialLBA,
	} {
		p := DefaultRuntimePolicyFor(kind)
		if p.Timeout > 0 && p.ProgressCadence > p.Timeout {
			t.Errorf("kind=%q cadence=%v > timeout=%v: cadence would never fire before timeout",
				kind, p.ProgressCadence, p.Timeout)
		}
	}
}

// TestSessionFailed_WALRecycled_EscalatesToRebuild — T4d-1 (architect
// HIGH v0.1 #1 + v0.3 boundary fix): engine branches on TYPED
// FailureKind, NOT on Reason substring. When a SessionClosedFailed
// event's `FailureKind == RecoveryFailureWALRecycled`, engine MUST
// set recovery.Decision = DecisionRebuild so the next decide() pass
// emits StartRebuild instead of looping on StartCatchUp.
//
// `INV-REPL-CATCHUP-RECYCLE-ESCALATES` (pinning method updated at
// T4d-1: substring match → typed kind).
func TestSessionFailed_WALRecycled_EscalatesToRebuild(t *testing.T) {
	st := &ReplicaState{
		Identity: IdentityTruth{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1},
		Session: SessionTruth{
			SessionID: 7,
			Phase:     PhaseRunning,
		},
		Recovery: RecoveryTruth{Decision: DecisionCatchUp},
	}
	ev := SessionClosedFailed{
		ReplicaID:   "r1",
		SessionID:   7,
		FailureKind: RecoveryFailureWALRecycled, // T4d-1: typed branch
		Reason:      "catch-up: WAL recycled (diagnostic text only — engine MUST NOT parse)",
	}
	r := Apply(st, ev)
	_ = r
	if st.Recovery.Decision != DecisionRebuild {
		t.Errorf("Recovery.Decision = %s, want %s after WAL-recycled session failure",
			st.Recovery.Decision, DecisionRebuild)
	}
	if st.Recovery.DecisionReason != "wal_recycled" {
		t.Errorf("Recovery.DecisionReason = %q, want %q",
			st.Recovery.DecisionReason, "wal_recycled")
	}
}

// TestSessionFailed_WALRecycled_NoSubstringMatchUsed — T4d-1
// negative pin: even if FailureKind is wrong (Unknown), Reason text
// containing "WAL recycled" must NOT trigger escalation. This proves
// engine does NOT fall back to substring matching.
func TestSessionFailed_WALRecycled_NoSubstringMatchUsed(t *testing.T) {
	st := &ReplicaState{
		Identity: IdentityTruth{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1},
		Session:  SessionTruth{SessionID: 7, Phase: PhaseRunning},
		Recovery: RecoveryTruth{Decision: DecisionCatchUp, H: 100},
	}
	// FailureKind=Unknown (zero value); Reason has the old substring.
	// If engine still parses Reason, this would escalate to Rebuild.
	// Post-T4d-1: must NOT escalate. Falls through to retry path.
	ev := SessionClosedFailed{
		ReplicaID: "r1",
		SessionID: 7,
		Reason:    "catch-up: WAL recycled past requested LSN (substring trap)",
	}
	Apply(st, ev)
	if st.Recovery.Decision == DecisionRebuild {
		t.Error("FAIL: engine fell back to substring match — must branch on FailureKind only")
	}
	if st.Recovery.DecisionReason == "wal_recycled" {
		t.Error("FAIL: DecisionReason=wal_recycled set despite FailureKind=Unknown — substring path leaked")
	}
}

// TestSessionFailed_NonRecycled_RetriesUntilBudget — T4c-3 §4.1
// retry-loop wiring (round-38). Engine increments Attempts on
// non-recycled failure; while Attempts <= MaxRetries, re-emits the
// matching Start* command. Once Attempts > MaxRetries, escalates
// (via PublishDegraded + Decision reset).
func TestSessionFailed_NonRecycled_RetriesUntilBudget(t *testing.T) {
	st := &ReplicaState{
		Identity: IdentityTruth{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1},
		Session:  SessionTruth{SessionID: 7, Phase: PhaseRunning},
		Recovery: RecoveryTruth{Decision: DecisionCatchUp, H: 100},
	}
	budget := DefaultRuntimePolicyFor(RecoveryContentWALDelta).MaxRetries // 3
	if budget != 3 {
		t.Fatalf("test premise: wal_delta MaxRetries=%d, want 3", budget)
	}

	for attempt := 1; attempt <= budget; attempt++ {
		st.Session = SessionTruth{SessionID: uint64(attempt + 6), Phase: PhaseRunning}
		ev := SessionClosedFailed{
			ReplicaID: "r1",
			SessionID: uint64(attempt + 6),
			Reason:    "transient: connection reset",
		}
		r := Apply(st, ev)
		if st.Recovery.Attempts != attempt {
			t.Errorf("attempt=%d: Attempts=%d, want %d", attempt, st.Recovery.Attempts, attempt)
		}
		// Within budget: must re-emit StartCatchUp.
		hasRetry := false
		for _, cmd := range r.Commands {
			if _, ok := cmd.(StartCatchUp); ok {
				hasRetry = true
			}
		}
		if !hasRetry {
			t.Errorf("attempt=%d: expected StartCatchUp re-emit (within budget); got %v",
				attempt, r.Commands)
		}
		if st.Recovery.Decision != DecisionCatchUp {
			t.Errorf("attempt=%d: Decision flipped from CatchUp to %s before budget exhausted",
				attempt, st.Recovery.Decision)
		}
	}

	// Attempt #4 (over budget): must escalate, NOT re-emit StartCatchUp.
	st.Session = SessionTruth{SessionID: 11, Phase: PhaseRunning}
	ev := SessionClosedFailed{ReplicaID: "r1", SessionID: 11, Reason: "transient again"}
	r := Apply(st, ev)
	for _, cmd := range r.Commands {
		if _, ok := cmd.(StartCatchUp); ok {
			t.Error("budget exhausted: must NOT re-emit StartCatchUp")
		}
	}
	if st.Recovery.Attempts != 0 {
		t.Errorf("budget exhausted: Attempts=%d, want 0 (reset)", st.Recovery.Attempts)
	}
	if st.Recovery.Decision != DecisionUnknown {
		t.Errorf("budget exhausted: Decision=%s, want Unknown (reset)", st.Recovery.Decision)
	}
	if st.Recovery.DecisionReason != "retry_budget_exhausted" {
		t.Errorf("budget exhausted: DecisionReason=%q, want retry_budget_exhausted",
			st.Recovery.DecisionReason)
	}
	hasDegraded := false
	for _, cmd := range r.Commands {
		if _, ok := cmd.(PublishDegraded); ok {
			hasDegraded = true
		}
	}
	if !hasDegraded {
		t.Error("budget exhausted: expected PublishDegraded command")
	}
}

// TestSessionCompleted_ClearsAttemptCounter — pin: a successful
// recovery clears Attempts so a future independent recovery starts
// fresh.
func TestSessionCompleted_ClearsAttemptCounter(t *testing.T) {
	st := &ReplicaState{
		Identity: IdentityTruth{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1},
		Session:  SessionTruth{SessionID: 7, Phase: PhaseRunning},
		Recovery: RecoveryTruth{Decision: DecisionCatchUp, H: 100, Attempts: 2},
	}
	ev := SessionClosedCompleted{ReplicaID: "r1", SessionID: 7, AchievedLSN: 100}
	Apply(st, ev)
	if st.Recovery.Attempts != 0 {
		t.Errorf("Attempts=%d after successful completion, want 0", st.Recovery.Attempts)
	}
}

// TestSessionFailed_NonRecycled_DoesNotEscalate — counter-pin: a
// non-recycle failure (e.g. transient network error, target-not-
// reached) MUST NOT silently flip Decision to Rebuild. The retry
// path (G-1 §4.1) handles those — escalation is reserved for the
// tier-class-change sentinel.
func TestSessionFailed_NonRecycled_DoesNotEscalate(t *testing.T) {
	st := &ReplicaState{
		Identity: IdentityTruth{ReplicaID: "r1", Epoch: 1, EndpointVersion: 1},
		Session: SessionTruth{
			SessionID: 7,
			Phase:     PhaseRunning,
		},
		Recovery: RecoveryTruth{Decision: DecisionCatchUp},
	}
	ev := SessionClosedFailed{
		ReplicaID:  "r1",
		SessionID:  7,
		Reason: "catch-up: target 100 not reached (achieved=50)",
	}
	Apply(st, ev)
	if st.Recovery.Decision != DecisionCatchUp {
		t.Errorf("Recovery.Decision = %s, want %s — non-recycle failure must not escalate",
			st.Recovery.Decision, DecisionCatchUp)
	}
}

// TestDefaultRuntimePolicy_MaxRetries — T4c-2 G-1 §4.1 architect
// Option B: per-content-kind retry budget lives on RuntimePolicy.
// wal_delta gets V2's `maxCatchupRetries=3`; full_extent gets 0
// (rebuild has no retry — restart is a tier-class action);
// partial_lba gets 1 (archive-bound; one retry tolerates a transient
// fetch hiccup).
func TestDefaultRuntimePolicy_MaxRetries(t *testing.T) {
	cases := []struct {
		kind RecoveryContentKind
		want int
	}{
		{RecoveryContentWALDelta, 3},
		{RecoveryContentFullExtent, 0},
		{RecoveryContentPartialLBA, 1},
	}
	for _, tc := range cases {
		got := DefaultRuntimePolicyFor(tc.kind).MaxRetries
		if got != tc.want {
			t.Errorf("kind=%q MaxRetries = %d, want %d", tc.kind, got, tc.want)
		}
	}
}

// Sanity: the policy struct is comparable (all comparable fields).
// If a future change adds a non-comparable field this catches it at
// compile time rather than at usage site.
var _ = func() bool {
	a := RecoveryRuntimePolicy{}
	b := RecoveryRuntimePolicy{}
	return a == b
}

// time import is referenced; suppress unused if a future cleanup
// removes the only Duration check path.
var _ = time.Second
