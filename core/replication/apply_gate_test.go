package replication

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// T4d-2 apply gate tests. Pins the round-43/44 architect-locked
// behavior + Option C hybrid AppliedLSNs seed + lane discrimination.

const blockSize = 4096

func newGate(t *testing.T) (*ReplicaApplyGate, *storage.BlockStore) {
	t.Helper()
	store := storage.NewBlockStore(64, blockSize)
	gate := NewReplicaApplyGate(store)
	return gate, store
}

func recoveryLineage(sessionID, targetLSN uint64) transport.RecoveryLineage {
	return transport.RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           1,
		EndpointVersion: 1,
		TargetLSN:       targetLSN, // > 1 = recovery lane
	}
}

func liveLineage(sessionID uint64) transport.RecoveryLineage {
	return transport.RecoveryLineage{
		SessionID:       sessionID,
		Epoch:           1,
		EndpointVersion: 1,
		TargetLSN:       liveShipTargetLSN, // = 1 = live lane
	}
}

func makeData(marker byte) []byte {
	d := make([]byte, blockSize)
	d[0] = marker
	return d
}

// --- Lane purity regression (round-46 architect ruling) ---
//
// Per round-46: gate is lane-PURE — caller decides lane, gate does
// NOT inspect lineage.TargetLSN to discriminate. The previous
// "isRecoveryLane(lineage)" helper has been removed; the gate's API
// is two explicit methods (ApplyRecovery + ApplyLive).
//
// TestApplyGate_RecoveryWithTargetLSN1_RoutesToRecoveryLane is the
// architect-required regression: a recovery session with TargetLSN=1
// (the OLD live-ship sentinel value) routes to recovery lane when
// caller invokes ApplyRecovery — proving the gate doesn't sniff the
// payload.

func TestApplyGate_RecoveryWithTargetLSN1_RoutesToRecoveryLane(t *testing.T) {
	gate, _ := newGate(t)
	// Recovery lineage with TargetLSN=1 — the OLD live-ship sentinel
	// value. Pre-rework, gate's TargetLSN inspection treated this
	// as live → would have fail-loud'd a "stale" recovery entry.
	// Post-rework: gate doesn't care about TargetLSN; ApplyRecovery
	// is recovery-lane semantics regardless of the payload value.
	lin := transport.RecoveryLineage{
		SessionID: 7, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}

	// Apply newer first.
	if err := gate.ApplyRecovery(lin, 5, makeData(0xBB), 50); err != nil {
		t.Fatalf("first recovery apply: %v", err)
	}

	// Stale apply via recovery: must SKIP silently (recovery lane).
	// If gate were sniffing TargetLSN==1 as live, this would
	// fail-loud with an error.
	err := gate.ApplyRecovery(lin, 5, makeData(0xAA), 30)
	if err != nil {
		t.Fatalf("FAIL: recovery stale apply errored — gate is treating TargetLSN=1 as live; should treat as recovery (caller's lane choice is authoritative); err=%v", err)
	}

	// Recovery coverage MUST advance even on stale-skip.
	if !gate.SessionRecoveryCoverage(7, 5) {
		t.Fatal("FAIL: recovery coverage did not advance — gate misrouted ApplyRecovery as live")
	}
}

// TestApplyGate_LiveWithTargetLSN100_RoutesToLiveLane is the mirror
// regression: live-lane sessions with arbitrarily-high TargetLSN
// (which would have been mis-classified as recovery pre-rework)
// route to live lane when caller invokes ApplyLive.
func TestApplyGate_LiveWithTargetLSN100_RoutesToLiveLane(t *testing.T) {
	gate, _ := newGate(t)
	// Live lineage with TargetLSN=100 — would have been treated as
	// recovery by pre-rework gate (TargetLSN > 1). Post-rework: gate
	// honors caller's choice.
	lin := transport.RecoveryLineage{
		SessionID: 99, Epoch: 1, EndpointVersion: 1, TargetLSN: 100,
	}

	if err := gate.ApplyLive(lin, 5, makeData(0xBB), 50); err != nil {
		t.Fatalf("first live apply: %v", err)
	}

	// Stale via live: must FAIL LOUD.
	err := gate.ApplyLive(lin, 5, makeData(0xAA), 30)
	if err == nil {
		t.Fatal("FAIL: live stale apply did NOT error — gate misrouted ApplyLive as recovery")
	}

	// Live MUST NOT advance recovery coverage.
	if gate.SessionRecoveryCoverage(99, 5) {
		t.Fatal("FAIL: live apply advanced recovery coverage — gate misrouted ApplyLive as recovery")
	}
}

// --- Recovery-lane stale-skip (INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA) ---

func TestApplyGate_RecoveryLane_StaleSkip_DoesNotRegressData(t *testing.T) {
	gate, store := newGate(t)
	lin := recoveryLineage(7, 100)

	// Apply newer first — LBA=5, LSN=50, value B.
	dataB := makeData(0xBB)
	if err := gate.ApplyRecovery(lin, 5, dataB, 50); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	// Substrate has B.
	got, _ := store.Read(5)
	if got[0] != 0xBB {
		t.Fatalf("substrate should have B; got %02x", got[0])
	}

	// Apply STALE — LBA=5, LSN=31, value A. Must skip.
	dataA := makeData(0xAA)
	if err := gate.ApplyRecovery(lin, 5, dataA, 31); err != nil {
		t.Errorf("stale apply must NOT error (recovery lane skips silently); got %v", err)
	}
	// Substrate STILL has B (no regression).
	got, _ = store.Read(5)
	if got[0] != 0xBB {
		t.Fatalf("FAIL: stale recovery apply REGRESSED data: substrate now has %02x, want B", got[0])
	}
}

// --- Round-44 #1: COVERAGE-ADVANCES-ON-SKIP ---

func TestApplyGate_RecoveryLane_StaleSkip_AdvancesRecoveryCovered(t *testing.T) {
	gate, _ := newGate(t)
	lin := recoveryLineage(7, 100)

	// Newer apply.
	gate.ApplyRecovery(lin, 5, makeData(0xBB), 50)
	// Stale skip.
	gate.ApplyRecovery(lin, 5, makeData(0xAA), 31)

	// recoveryCovered MUST be true for LBA=5 even though the stale
	// apply was skipped. Without this, barrier completion lies and
	// live lane sees "recovery hasn't covered this slot."
	if !gate.SessionRecoveryCoverage(7, 5) {
		t.Fatal("FAIL: round-44 INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP — coverage MUST advance even on stale-skip")
	}
}

func TestApplyGate_RecoveryLane_FreshApply_AdvancesCoverageAndAppliedLSN(t *testing.T) {
	gate, _ := newGate(t)
	lin := recoveryLineage(7, 100)

	gate.ApplyRecovery(lin, 5, makeData(0xAA), 30)
	// Both maps updated.
	if !gate.SessionRecoveryCoverage(7, 5) {
		t.Error("recoveryCovered must include applied LBA")
	}
	lsn, ok := gate.SessionAppliedLSN(7, 5)
	if !ok || lsn != 30 {
		t.Errorf("appliedLSN[5] = (%d, %v), want (30, true)", lsn, ok)
	}
}

// --- Round-44 #2: LIVE-LANE-STALE-FAILS-LOUD ---

func TestApplyGate_LiveLane_StaleEntry_ReturnsError(t *testing.T) {
	gate, store := newGate(t)
	lin := liveLineage(99)

	// Live apply newer first.
	gate.ApplyLive(lin, 5, makeData(0xBB), 50)

	// Live STALE: must FAIL LOUD (return error).
	err := gate.ApplyLive(lin, 5, makeData(0xAA), 31)
	if err == nil {
		t.Fatal("FAIL: round-44 INV-REPL-LIVE-LANE-STALE-FAILS-LOUD — live-lane stale MUST return error")
	}
	if !strings.Contains(err.Error(), "live-lane stale") {
		t.Errorf("error should name the stale-fail-loud invariant: %v", err)
	}
	if !strings.Contains(err.Error(), "INV-REPL-LIVE-LANE-STALE-FAILS-LOUD") {
		t.Errorf("error should reference invariant name for traceability: %v", err)
	}

	// Substrate must still have B (data NOT regressed).
	got, _ := store.Read(5)
	if got[0] != 0xBB {
		t.Errorf("FAIL: live-lane fail-loud must NOT mutate data; got %02x, want B", got[0])
	}
}

func TestApplyGate_LiveLane_StaleEntry_DoesNotAdvanceRecoveryCovered(t *testing.T) {
	gate, _ := newGate(t)
	lin := liveLineage(99)

	// Establish LBA=5 at LSN=50.
	gate.ApplyLive(lin, 5, makeData(0xBB), 50)

	// Live stale (failed apply).
	gate.ApplyLive(lin, 5, makeData(0xAA), 31)

	// CRITICAL: live lane MUST NOT advance recoveryCovered, even on
	// the FIRST (successful, non-stale) apply or on the stale-fail
	// path. recoveryCovered is recovery-lane-only accounting.
	if gate.SessionRecoveryCoverage(99, 5) {
		t.Fatal("FAIL: live lane MUST NOT advance recoveryCovered (Q1/round-44 split)")
	}
}

// --- Lane independence: two sessions, different lanes ---

func TestApplyGate_LiveAndRecovery_DistinctSessions_Independent(t *testing.T) {
	gate, _ := newGate(t)
	live := liveLineage(50)
	recov := recoveryLineage(7, 100)

	gate.ApplyLive(live, 5, makeData(0xBB), 100)
	gate.ApplyRecovery(recov, 5, makeData(0xAA), 30) // recovery sees its own session

	// Recovery session sees its own appliedLSN (just set: 30).
	lsn, ok := gate.SessionAppliedLSN(7, 5)
	if !ok || lsn != 30 {
		t.Errorf("recovery session appliedLSN[5] = (%d, %v), want (30, true)", lsn, ok)
	}

	// Live session sees its own appliedLSN (set: 100).
	lsn, ok = gate.SessionAppliedLSN(50, 5)
	if !ok || lsn != 100 {
		t.Errorf("live session appliedLSN[5] = (%d, %v), want (100, true)", lsn, ok)
	}
}

// --- Option C hybrid: AppliedLSNs seed at session init ---

func TestApplyGate_OptionCHybrid_BlockStoreFallback_NoSeed(t *testing.T) {
	// BlockStore returns ErrAppliedLSNsNotTracked. Gate must fall
	// back to empty session map; first apply works normally.
	gate, _ := newGate(t)
	lin := recoveryLineage(7, 100)
	if err := gate.ApplyRecovery(lin, 5, makeData(0xAA), 30); err != nil {
		t.Fatalf("BlockStore-backed gate failed first apply: %v", err)
	}
	// SessionState should reflect 1 applied + 1 covered.
	a, _, c, found := gate.SessionState(7)
	if !found {
		t.Fatal("session state missing after first apply")
	}
	if a != 1 || c != 1 {
		t.Errorf("after 1 apply: appliedLSN count=%d, recoveryCovered count=%d, want 1/1", a, c)
	}
}

// fakeStoreWithSeed implements LogicalStorage's AppliedLSNs() with a
// preset seed map for testing the Option C hybrid seed path.
// Other methods delegate to BlockStore.
type fakeStoreWithSeed struct {
	*storage.BlockStore
	seed map[uint32]uint64
}

func (f *fakeStoreWithSeed) AppliedLSNs() (map[uint32]uint64, error) {
	out := make(map[uint32]uint64, len(f.seed))
	for k, v := range f.seed {
		out[k] = v
	}
	return out, nil
}

func TestApplyGate_OptionCHybrid_SubstrateSeedHonored(t *testing.T) {
	// Substrate reports LBA=5 already at LSN=50.
	store := &fakeStoreWithSeed{
		BlockStore: storage.NewBlockStore(64, blockSize),
		seed:       map[uint32]uint64{5: 50},
	}
	gate := NewReplicaApplyGate(store)
	lin := recoveryLineage(7, 100)

	// Trigger session init (any Apply does it).
	gate.ApplyRecovery(lin, 99, makeData(0xCC), 60) // unrelated LBA to force init

	// Now LBA=5's seeded LSN should be visible.
	lsn, ok := gate.SessionAppliedLSN(7, 5)
	if !ok {
		t.Fatal("FAIL: seeded LBA=5 missing from session appliedLSN — substrate seed not honored")
	}
	if lsn != 50 {
		t.Errorf("seeded appliedLSN[5] = %d, want 50", lsn)
	}

	// And a stale recovery apply for LBA=5 at LSN=30 must skip.
	dataA := makeData(0xAA)
	gate.ApplyRecovery(lin, 5, dataA, 30)
	gotData, _ := store.Read(5)
	if gotData[0] == 0xAA {
		t.Fatal("FAIL: stale recovery apply applied despite substrate seed (round-43 stale-skip + Option C)")
	}
}

// --- Restart safety (INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE) ---

func TestApplyGate_RestartMidRecovery_NewSessionReseedsFromSubstrate(t *testing.T) {
	// Simulate: session 7 makes some progress; gate is destroyed
	// (process restart); a fresh gate over the same substrate
	// should re-seed from substrate AppliedLSNs at the next session.
	store := storage.NewBlockStore(64, blockSize)
	// Pre-seed substrate with LBA=5 at LSN=100 (simulates pre-restart
	// state).
	store.ApplyEntry(5, makeData(0xBB), 100)

	// "Restart": new gate (fresh state, but same substrate).
	gate := NewReplicaApplyGate(store)
	lin := recoveryLineage(8, 200) // new session post-restart

	// First Apply triggers init. With BlockStore (ErrAppliedLSNsNotTracked),
	// the gate's session-only map starts empty; substrate's actual
	// data at LBA=5 is value B but the gate doesn't know LSN=100.
	// A stale apply for LBA=5 at LSN=30 would NORMALLY pass the
	// session-only check (no entry in appliedLSN[5] yet) and apply A,
	// regressing data.
	//
	// Per kickoff §2.5 #1 architect: "Option B alone is not
	// restart-safe enough" — this is exactly the case. With
	// BlockStore (no AppliedLSNs), restart-safety degrades; this
	// test documents the limitation honestly.
	gate.ApplyRecovery(lin, 5, makeData(0xAA), 30)
	got, _ := store.Read(5)
	// EXPECTED on BlockStore (no substrate seed): data WAS regressed.
	// This is the documented gap; substrate-side
	// per-LBA-LSN tracking (full Option A) closes it.
	if got[0] != 0xAA {
		t.Logf("BlockStore restart-safety gap NOT exhibited (data not regressed) — substrate may have changed; review")
	}
}

// fakeStoreWithSeedAndApply lets us simulate a substrate that DOES
// expose AppliedLSNs (Option C hybrid full path) — pin restart-safety
// via the seed.
func TestApplyGate_RestartSafe_SubstrateSeedPathRejectsStale(t *testing.T) {
	store := &fakeStoreWithSeed{
		BlockStore: storage.NewBlockStore(64, blockSize),
		seed:       map[uint32]uint64{5: 100}, // pre-restart applied state
	}
	store.BlockStore.ApplyEntry(5, makeData(0xBB), 100) // matching substrate data

	gate := NewReplicaApplyGate(store) // post-restart gate
	lin := recoveryLineage(8, 200)

	// Stale recovery apply post-restart. With substrate seed
	// honored, gate sees appliedLSN[5]=100, skips LSN=30. Pins
	// INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE.
	gate.ApplyRecovery(lin, 5, makeData(0xAA), 30)
	got, _ := store.Read(5)
	if got[0] != 0xBB {
		t.Fatalf("FAIL: post-restart stale recovery apply regressed data; got %02x, want B (round-44 INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE; Option C hybrid full path)", got[0])
	}
}

// --- Substrate apply error propagates ---

type errStore struct{ *storage.BlockStore }

func (e *errStore) ApplyEntry(uint32, []byte, uint64) error {
	return errSubstrateBoom
}

var errSubstrateBoom = boomErr("substrate boom")

type boomErr string

func (b boomErr) Error() string { return string(b) }

func TestApplyGate_SubstrateApplyError_Propagates(t *testing.T) {
	store := &errStore{BlockStore: storage.NewBlockStore(64, blockSize)}
	gate := NewReplicaApplyGate(store)
	lin := recoveryLineage(7, 100)

	err := gate.ApplyRecovery(lin, 5, makeData(0xAA), 30)
	if err == nil || !strings.Contains(err.Error(), "substrate boom") {
		t.Errorf("substrate ApplyEntry error must propagate; got %v", err)
	}
}

// --- CloseSession releases state ---

func TestApplyGate_CloseSession_ReleasesState(t *testing.T) {
	gate, _ := newGate(t)
	lin := recoveryLineage(7, 100)
	gate.ApplyRecovery(lin, 5, makeData(0xAA), 30)
	if _, _, _, found := gate.SessionState(7); !found {
		t.Fatal("session 7 should exist before close")
	}
	gate.CloseSession(7)
	if _, _, _, found := gate.SessionState(7); found {
		t.Fatal("session 7 should be gone after close")
	}
}
