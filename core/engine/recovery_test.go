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
