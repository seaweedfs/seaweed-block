package authority

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// ============================================================
// P14 S3 — Policy Owner Test Matrix
//
// Mirrors docs/sw-block/design/v3-phase-14-s3-sketch.md §13.
// Four categories:
//   1. authoring tests               (8)
//   2. stale-resistance tests        (3)
//   3. boundary tests                (4)
//   4. end-to-end closure tests      (3)
// ============================================================

// fakeBasisReader lets unit tests inject an AuthorityBasis result
// without spinning a real Publisher.
type fakeBasisReader struct {
	mu   sync.Mutex
	data map[string]AuthorityBasis
}

func newFakeBasisReader() *fakeBasisReader {
	return &fakeBasisReader{data: make(map[string]AuthorityBasis)}
}

func (f *fakeBasisReader) LastAuthorityBasis(volumeID, replicaID string) (AuthorityBasis, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.data[volumeID+"/"+replicaID]
	return b, ok
}

// VolumeAuthorityLine satisfies the new AuthorityBasisReader
// method. Iterates stored entries whose key starts with the
// VolumeID prefix and returns the highest-Epoch entry.
func (f *fakeBasisReader) VolumeAuthorityLine(volumeID string) (AuthorityBasis, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := volumeID + "/"
	var best AuthorityBasis
	any := false
	for k, b := range f.data {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		if !any || b.Epoch > best.Epoch {
			best = b
			any = true
		}
	}
	return best, any
}

func (f *fakeBasisReader) Set(volumeID, replicaID string, b AuthorityBasis) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[volumeID+"/"+replicaID] = b
}

func (f *fakeBasisReader) Clear(volumeID, replicaID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, volumeID+"/"+replicaID)
}

// acceptableSlot builds an SlotObservation whose acceptable()
// predicate returns true — all four fields correct and not
// withdrawn. Tests then knock specific fields down to make it
// unacceptable.
func acceptableSlot(replicaID, server, dataAddr, ctrlAddr string) SlotObservation {
	return SlotObservation{
		ReplicaID:       replicaID,
		ServerID:        server,
		DataAddr:        dataAddr,
		CtrlAddr:        ctrlAddr,
		Reachable:       true,
		ReadyForPrimary: true,
		Eligible:        true,
		Withdrawn:       false,
	}
}

// runNextOnce drives the owner on one submitted snapshot with a
// short deadline. Returns the emitted ask or an error, which for
// the blocking-on-no-ask path is context.DeadlineExceeded.
func runNextOnce(t *testing.T, o *PolicyOwner, snap PolicySnapshot) (AssignmentAsk, error) {
	t.Helper()
	if err := o.SubmitSnapshot(snap); err != nil {
		t.Fatalf("SubmitSnapshot: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	return o.Next(ctx)
}

// expectNoEmit asserts that Next blocks (deadline-expires) on the
// given snapshot — i.e. the decision was no-ask or unsupported.
func expectNoEmit(t *testing.T, o *PolicyOwner, snap PolicySnapshot) {
	t.Helper()
	ask, err := runNextOnce(t, o, snap)
	if err == nil {
		t.Fatalf("expected no emit, got ask=%+v", ask)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

// expectEmit asserts Next emits the expected ask (intent +
// replicaID) on the given snapshot.
func expectEmit(t *testing.T, o *PolicyOwner, snap PolicySnapshot, wantIntent AskIntent, wantReplicaID string) AssignmentAsk {
	t.Helper()
	ask, err := runNextOnce(t, o, snap)
	if err != nil {
		t.Fatalf("expected emit, got err=%v", err)
	}
	if ask.Intent != wantIntent {
		t.Fatalf("intent: got %s want %s", ask.Intent, wantIntent)
	}
	if ask.ReplicaID != wantReplicaID {
		t.Fatalf("replicaID: got %q want %q", ask.ReplicaID, wantReplicaID)
	}
	return ask
}

// ============================================================
// §13.1 — Policy authoring tests
// ============================================================

func TestPolicy_NoAuth_OneAcceptable_EmitsBind(t *testing.T) {
	reader := newFakeBasisReader()
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	snap := PolicySnapshot{
		VolumeID:          "v1",
		CollectedRevision: 1,
		Authority:         AuthorityBasis{Assigned: false},
		PrimarySlot:       acceptableSlot("r1", "s1", "d1", "c1"),
		SecondarySlot:     acceptableSlot("r2", "s2", "d2", "c2"),
	}
	// Make secondary unacceptable so only primary qualifies.
	snap.SecondarySlot.Reachable = false

	expectEmit(t, owner, snap, IntentBind, "r1")
}

func TestPolicy_NoAuth_BothAcceptable_EmitsBindForPreferred(t *testing.T) {
	reader := newFakeBasisReader()
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	snap := PolicySnapshot{
		VolumeID:          "v1",
		CollectedRevision: 1,
		Authority:         AuthorityBasis{Assigned: false},
		PrimarySlot:       acceptableSlot("r1", "s1", "d1", "c1"),
		SecondarySlot:     acceptableSlot("r2", "s2", "d2", "c2"),
	}
	// Both acceptable: tie-break on preferred (PrimarySlot).
	expectEmit(t, owner, snap, IntentBind, "r1")
}

func TestPolicy_NoAuth_NeitherAcceptable_NoAsk(t *testing.T) {
	reader := newFakeBasisReader()
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	primary := acceptableSlot("r1", "s1", "d1", "c1")
	primary.Reachable = false
	secondary := acceptableSlot("r2", "s2", "d2", "c2")
	secondary.Withdrawn = true
	snap := PolicySnapshot{
		VolumeID:      "v1",
		Authority:     AuthorityBasis{Assigned: false},
		PrimarySlot:   primary,
		SecondarySlot: secondary,
	}
	expectNoEmit(t, owner, snap)
}

func TestPolicy_CurrentHealthy_Unchanged_NoAsk(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	snap := PolicySnapshot{
		VolumeID: "v1",
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			DataAddr: "d1", CtrlAddr: "c1",
		},
		PrimarySlot:   acceptableSlot("r1", "s1", "d1", "c1"),
		SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
	}
	expectNoEmit(t, owner, snap)
}

func TestPolicy_CurrentHealthy_DataAddrChanged_EmitsRefresh(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	snap := PolicySnapshot{
		VolumeID: "v1",
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			DataAddr: "d1", CtrlAddr: "c1",
		},
		// Same identity, new DataAddr.
		PrimarySlot:   acceptableSlot("r1", "s1", "d1-new", "c1"),
		SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
	}
	ask := expectEmit(t, owner, snap, IntentRefreshEndpoint, "r1")
	if ask.DataAddr != "d1-new" {
		t.Fatalf("refresh DataAddr: got %q want d1-new", ask.DataAddr)
	}
}

func TestPolicy_CurrentHealthy_CtrlAddrChanged_EmitsRefresh(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	snap := PolicySnapshot{
		VolumeID: "v1",
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			DataAddr: "d1", CtrlAddr: "c1",
		},
		// Same identity + data addr, new CtrlAddr.
		PrimarySlot:   acceptableSlot("r1", "s1", "d1", "c1-new"),
		SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
	}
	ask := expectEmit(t, owner, snap, IntentRefreshEndpoint, "r1")
	if ask.CtrlAddr != "c1-new" {
		t.Fatalf("refresh CtrlAddr: got %q want c1-new", ask.CtrlAddr)
	}
}

func TestPolicy_CurrentUnacceptable_OtherAcceptable_EmitsReassign(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	primary := acceptableSlot("r1", "s1", "d1", "c1")
	primary.Reachable = false // current is unacceptable
	snap := PolicySnapshot{
		VolumeID: "v1",
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			DataAddr: "d1", CtrlAddr: "c1",
		},
		PrimarySlot:   primary,
		SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
	}
	expectEmit(t, owner, snap, IntentReassign, "r2")
}

func TestPolicy_CurrentUnacceptable_OtherUnacceptable_NoAsk(t *testing.T) {
	reader := newFakeBasisReader()
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	primary := acceptableSlot("r1", "s1", "d1", "c1")
	primary.Reachable = false
	secondary := acceptableSlot("r2", "s2", "d2", "c2")
	secondary.ReadyForPrimary = false
	snap := PolicySnapshot{
		VolumeID: "v1",
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			DataAddr: "d1", CtrlAddr: "c1",
		},
		PrimarySlot:   primary,
		SecondarySlot: secondary,
	}
	expectNoEmit(t, owner, snap)
}

// TestPolicy_OutOfTopologyPublisherAuthority_NoAskPlusUnsupportedEvidence
// is the regression for finding 1: the publisher already holds
// authority on a replica outside the two accepted slots (e.g. r99
// from a prior out-of-band Bind), and the snapshot's own
// Authority says Assigned=false. Before this fix, stale-
// resistance only looked at the snapshot's two slot IDs, missed
// the r99 line entirely, and let the policy emit Bind(r1) — which
// Publisher.apply would accept because IntentBind is rejected
// only per (vid, rid), not per volume. Result: two authority
// lines on the same volume.
//
// With per-volume stale-resistance, the r99 line is detected,
// and the snapshot is rejected with explicit unsupported evidence.
func TestPolicy_OutOfTopologyPublisherAuthority_NoAskPlusUnsupportedEvidence(t *testing.T) {
	reader := newFakeBasisReader()
	// Publisher already holds authority on r99 — outside the
	// snapshot's accepted topology (r1, r2).
	reader.Set("v1", "r99", AuthorityBasis{
		Assigned: true, ReplicaID: "r99", Epoch: 5, EndpointVersion: 1,
		DataAddr: "dx", CtrlAddr: "cx",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)

	// Snapshot sees no authority in the accepted slots; without
	// the per-volume check, policy would emit Bind(r1).
	snap := PolicySnapshot{
		VolumeID:          "v1",
		CollectedRevision: 11,
		Authority:         AuthorityBasis{Assigned: false},
		PrimarySlot:       acceptableSlot("r1", "s1", "d1", "c1"),
		SecondarySlot:     acceptableSlot("r2", "s2", "d2", "c2"),
	}
	expectNoEmit(t, owner, snap)

	ev, ok := owner.LastUnsupported()
	if !ok {
		t.Fatal("expected unsupported evidence for out-of-topology publisher authority")
	}
	if ev.SnapshotRevision != 11 {
		t.Fatalf("evidence revision: got %d want 11", ev.SnapshotRevision)
	}
	// Structured-artifact assertions (not just the reason string):
	// the offending Basis must carry the publisher's current line
	// r99@5, NOT the snapshot's unassigned Authority view.
	if !ev.Basis.Assigned {
		t.Fatal("evidence Basis: must carry the offending publisher line, got unassigned (snapshot's view)")
	}
	if ev.Basis.ReplicaID != "r99" {
		t.Fatalf("evidence Basis.ReplicaID: got %q want r99", ev.Basis.ReplicaID)
	}
	if ev.Basis.Epoch != 5 {
		t.Fatalf("evidence Basis.Epoch: got %d want 5", ev.Basis.Epoch)
	}
}

// TestPolicy_EmptyServerID_NoAskPlusUnsupportedEvidence covers
// the tightened distinct-servers invariant: missing server
// identity on either slot is also unsupported. "Two replica slots
// on distinct servers" cannot be proven if the identities are
// absent; accepting empty ServerID would let the slice claim a
// topology invariant it cannot check.
func TestPolicy_EmptyServerID_NoAskPlusUnsupportedEvidence(t *testing.T) {
	reader := newFakeBasisReader()
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)

	// Secondary slot has empty ServerID — invariant violated.
	secondary := acceptableSlot("r2", "", "d2", "c2")
	snap := PolicySnapshot{
		VolumeID:          "v1",
		CollectedRevision: 17,
		Authority:         AuthorityBasis{Assigned: false},
		PrimarySlot:       acceptableSlot("r1", "s1", "d1", "c1"),
		SecondarySlot:     secondary,
	}
	expectNoEmit(t, owner, snap)

	ev, ok := owner.LastUnsupported()
	if !ok {
		t.Fatal("empty ServerID must record unsupported evidence")
	}
	if ev.SnapshotRevision != 17 {
		t.Fatalf("evidence revision: got %d want 17", ev.SnapshotRevision)
	}
	if !strings.Contains(ev.Reason, "ServerID") {
		t.Fatalf("expected evidence reason to mention ServerID, got %q", ev.Reason)
	}
}

// TestPolicy_BothSlotsSameServer_NoAskPlusUnsupportedEvidence is
// the regression for finding 3: the accepted topology set
// requires "two replica slots on distinct servers". Two slots
// sharing a ServerID violate that invariant. Policy records
// unsupported evidence and emits no ask, even when the slots are
// otherwise fully acceptable.
func TestPolicy_BothSlotsSameServer_NoAskPlusUnsupportedEvidence(t *testing.T) {
	reader := newFakeBasisReader()
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)

	// Both slots on the same ServerID — topology violation.
	snap := PolicySnapshot{
		VolumeID:          "v1",
		CollectedRevision: 13,
		Authority:         AuthorityBasis{Assigned: false},
		PrimarySlot:       acceptableSlot("r1", "sA", "d1", "c1"),
		SecondarySlot:     acceptableSlot("r2", "sA", "d2", "c2"),
	}
	expectNoEmit(t, owner, snap)

	ev, ok := owner.LastUnsupported()
	if !ok {
		t.Fatal("same-server topology violation must record evidence")
	}
	if ev.SnapshotRevision != 13 {
		t.Fatalf("evidence revision: got %d want 13", ev.SnapshotRevision)
	}
	if !strings.Contains(ev.Reason, "ServerID") {
		t.Fatalf("expected evidence reason to mention ServerID, got %q", ev.Reason)
	}
}

func TestPolicy_AuthOutsideTopology_NoAskPlusUnsupportedEvidence(t *testing.T) {
	reader := newFakeBasisReader()
	// Authority says r99 is published, which is NOT in either of our
	// two accepted slots (r1 / r2).
	reader.Set("v1", "r99", AuthorityBasis{
		Assigned: true, ReplicaID: "r99", Epoch: 3, EndpointVersion: 1,
		DataAddr: "dx", CtrlAddr: "cx",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	snap := PolicySnapshot{
		VolumeID:          "v1",
		CollectedRevision: 42,
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r99", Epoch: 3, EndpointVersion: 1,
			DataAddr: "dx", CtrlAddr: "cx",
		},
		PrimarySlot:   acceptableSlot("r1", "s1", "d1", "c1"),
		SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
	}
	expectNoEmit(t, owner, snap)

	// Unsupported evidence must have been recorded — silent idle
	// would be a violation of sketch §8.
	ev, ok := owner.LastUnsupported()
	if !ok {
		t.Fatal("expected unsupported evidence, got none")
	}
	if ev.SnapshotRevision != 42 {
		t.Fatalf("evidence revision: got %d want 42", ev.SnapshotRevision)
	}
	if ev.Basis.ReplicaID != "r99" {
		t.Fatalf("evidence basis ReplicaID: got %q want r99", ev.Basis.ReplicaID)
	}
	if ev.Reason == "" {
		t.Fatal("expected non-empty evidence Reason")
	}
}

// ============================================================
// Reassign semantic regressions (from
// sw-block/design/v3-phase-14-s3-reassign-semantic.md §5)
//
// These tests pin the widened IntentReassign semantic directly
// against the Publisher. They are deliberately placed alongside
// the policy authoring tests because they prove the authority
// contract the policy relies on.
// ============================================================

// TestReassign_CrossSlotMintsEpochPlusOne — Publisher.apply with
// IntentReassign targeting a previously-unbound ReplicaID of the
// same VolumeID must mint (targetReplicaID, maxEpoch+1, EV=1).
// This is the first-class cross-slot failover proof.
func TestReassign_CrossSlotMintsEpochPlusOne(t *testing.T) {
	pub := NewPublisher(nil)
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind r1: %v", err)
	}
	// Cross-slot reassign targeting a previously-unbound r2.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r2",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("cross-slot Reassign r2: %v", err)
	}
	got, ok := pub.LastPublished("v1", "r2")
	if !ok {
		t.Fatal("no publication for (v1, r2) after cross-slot Reassign")
	}
	if got.Epoch != 2 {
		t.Fatalf("epoch: got %d want 2 (max across volume + 1)", got.Epoch)
	}
	if got.EndpointVersion != 1 {
		t.Fatalf("EV: got %d want 1 (Reassign resets EV)", got.EndpointVersion)
	}
	if got.ReplicaID != "r2" {
		t.Fatalf("replicaID: got %q want r2", got.ReplicaID)
	}
}

// TestReassign_NoPriorVolumePublish_StillRejects — IntentReassign
// with no prior publication of any kind for this VolumeID must
// still be rejected. First-time establishment stays on
// IntentBind; this prevents confusion about the per-volume
// monotonic line.
func TestReassign_NoPriorVolumePublish_StillRejects(t *testing.T) {
	pub := NewPublisher(nil)
	err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentReassign,
	})
	if !errors.Is(err, ErrReassignNotBound) {
		t.Fatalf("want ErrReassignNotBound, got %v", err)
	}
}

// TestReassign_PerSlotOpsOnOldSlotDoNotAdvanceVolumeLine — after
// a cross-slot Reassign moves per-volume authority from r1@1 to
// r2@2, the old slot state for (v1, r1) is intentionally
// preserved so per-slot operations (like RefreshEndpoint on r1)
// still work locally. The tightened claim this test proves is:
//
//   A RefreshEndpoint on the OLD slot bumps only its own EV;
//   it does NOT retroactively advance the Epoch of the old slot
//   above its frozen value, and it does NOT change the current
//   per-volume line (still r2@2).
//
// This test does NOT claim that the old slot is invisible to any
// subscriber that might still be listening on it. Subscribers to
// (v1, r1) see per-slot updates for r1 as usual — that is a
// property of the per-subscription fan-out model from S2 and is
// out of scope for this per-volume-line regression.
func TestReassign_PerSlotOpsOnOldSlotDoNotAdvanceVolumeLine(t *testing.T) {
	pub := NewPublisher(nil)
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind r1: %v", err)
	}
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r2",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("cross-slot Reassign r2: %v", err)
	}

	// Old slot state: last published (v1, r1, epoch=1, ev=1) —
	// frozen by design so per-slot Refresh on r1 still works.
	oldBasis, ok := pub.LastAuthorityBasis("v1", "r1")
	if !ok {
		t.Fatal("(v1, r1) state unexpectedly missing after cross-slot Reassign")
	}
	if oldBasis.Epoch != 1 {
		t.Fatalf("old slot Epoch: got %d want 1 (frozen)", oldBasis.Epoch)
	}

	// Per-slot Refresh on r1. This is intentionally still allowed.
	// The tight claim: it bumps r1's own EV but does not touch r1's
	// Epoch and does not change the current per-volume line.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1-changed", CtrlAddr: "c1",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("RefreshEndpoint on old slot: %v", err)
	}
	r1After, _ := pub.LastAuthorityBasis("v1", "r1")
	if r1After.Epoch != 1 {
		t.Fatalf("old-slot Refresh must not retroactively advance its Epoch, got %d",
			r1After.Epoch)
	}
	if r1After.EndpointVersion != 2 {
		t.Fatalf("old-slot Refresh must advance its own EV: got %d want 2",
			r1After.EndpointVersion)
	}

	// The per-volume current line remains r2@2.
	r2, ok := pub.LastAuthorityBasis("v1", "r2")
	if !ok {
		t.Fatal("(v1, r2) state missing after cross-slot Reassign")
	}
	if r2.Epoch != 2 {
		t.Fatalf("new slot Epoch: got %d want 2", r2.Epoch)
	}
}

// TestReassign_PerSlotEndpointVersionMonotonicityPreserved — after
// a cross-slot Reassign puts the new slot at EV=1, a subsequent
// RefreshEndpoint on that slot advances its EV from 1 to 2, not
// from the OLD slot's EV or from some synthetic starting point.
func TestReassign_PerSlotEndpointVersionMonotonicityPreserved(t *testing.T) {
	pub := NewPublisher(nil)
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind r1: %v", err)
	}
	// Inflate r1's EV so we can tell the two slots apart if they
	// were accidentally merged.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1-v2", CtrlAddr: "c1",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("Refresh r1: %v", err)
	}
	// Cross-slot reassign: r2 must land at EV=1, not EV=3 or any
	// value derived from r1's history.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r2",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("cross-slot Reassign r2: %v", err)
	}
	first, _ := pub.LastAuthorityBasis("v1", "r2")
	if first.EndpointVersion != 1 {
		t.Fatalf("post-Reassign r2 EV: got %d want 1", first.EndpointVersion)
	}
	// A Refresh on r2 advances ITS own EV from 1 → 2.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r2",
		DataAddr: "d2-v2", CtrlAddr: "c2",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("Refresh r2: %v", err)
	}
	second, _ := pub.LastAuthorityBasis("v1", "r2")
	if second.EndpointVersion != 2 {
		t.Fatalf("post-Refresh r2 EV: got %d want 2 (per-slot monotonic from 1)", second.EndpointVersion)
	}
	if second.Epoch != 2 {
		t.Fatalf("post-Refresh r2 Epoch: got %d want 2 (unchanged by Refresh)", second.Epoch)
	}
}

// ============================================================
// §13.2 — Stale-resistance tests
// ============================================================

func TestPolicy_StaleDecision_DroppedWhenAuthorityAdvances(t *testing.T) {
	reader := newFakeBasisReader()
	// Snapshot basis: epoch=1. Reader reports epoch=2 already —
	// the candidate decision is stale.
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 2, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	primary := acceptableSlot("r1", "s1", "d1", "c1")
	primary.Reachable = false // would normally trigger Reassign
	staleSnap := PolicySnapshot{
		VolumeID: "v1",
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			DataAddr: "d1", CtrlAddr: "c1",
		},
		PrimarySlot:   primary,
		SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
	}
	expectNoEmit(t, owner, staleSnap)
}

func TestPolicy_StaleRefresh_DroppedWhenNewerEndpointVersion(t *testing.T) {
	reader := newFakeBasisReader()
	// Snapshot basis: EV=1. Reader reports EV=2 already.
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 2,
		DataAddr: "d1-newer", CtrlAddr: "c1",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	staleSnap := PolicySnapshot{
		VolumeID: "v1",
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			DataAddr: "d1", CtrlAddr: "c1",
		},
		PrimarySlot:   acceptableSlot("r1", "s1", "d1-new", "c1"),
		SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
	}
	expectNoEmit(t, owner, staleSnap)
}

// TestPolicy_StaleSnapshotAfterCrossSlotFailover_Dropped is the
// regression for the architect finding that the earlier per-slot
// stale check was wrong under the widened S3 IntentReassign
// semantic. After cross-slot failover (r1@1 -> r2@2), the
// preserved (v1, r1)=epoch=1 state made a stale pre-failover
// snapshot still look "current" to the per-slot compare, so the
// policy would emit another Reassign producing r2@3 spuriously.
//
// With per-volume-line stale-resistance, the snapshot is compared
// against the highest-epoch basis across both slots (= r2@2) and
// dropped cleanly.
func TestPolicy_StaleSnapshotAfterCrossSlotFailover_Dropped(t *testing.T) {
	reader := newFakeBasisReader()
	// Post-failover state: (v1, r1) preserved at epoch=1, current
	// per-volume line at r2@2. This is exactly what Publisher.apply
	// produces after IntentBind(r1) + IntentReassign(r2).
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	reader.Set("v1", "r2", AuthorityBasis{
		Assigned: true, ReplicaID: "r2", Epoch: 2, EndpointVersion: 1,
		DataAddr: "d2", CtrlAddr: "c2",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)

	// Stale snapshot: still thinks r1@1 is the current authority
	// AND observes r1 as unreachable, which (without the fix)
	// would drive another Reassign to r2 — spuriously producing
	// r2@3 on top of the already-moved authority line.
	staleSnap := PolicySnapshot{
		VolumeID: "v1",
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
			DataAddr: "d1", CtrlAddr: "c1",
		},
		PrimarySlot: func() SlotObservation {
			s := acceptableSlot("r1", "s1", "d1", "c1")
			s.Reachable = false
			return s
		}(),
		SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
	}
	expectNoEmit(t, owner, staleSnap)
}

func TestPolicy_NewerSnapshotOverwritesOlder_OnlyLatestValidEmits(t *testing.T) {
	reader := newFakeBasisReader()
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)

	// Submit three snapshots WITHOUT calling Next in between; the
	// overwrite-latest slot discards older unread snapshots.
	olderBoth := PolicySnapshot{
		VolumeID:          "v1",
		CollectedRevision: 1,
		Authority:         AuthorityBasis{Assigned: false},
		PrimarySlot:       acceptableSlot("r1", "s1", "d1-OLD", "c1-OLD"),
		SecondarySlot:     acceptableSlot("r2", "s2", "d2", "c2"),
	}
	intermediate := olderBoth
	intermediate.CollectedRevision = 2
	intermediate.PrimarySlot.DataAddr = "d1-MID"
	newest := olderBoth
	newest.CollectedRevision = 3
	newest.PrimarySlot.DataAddr = "d1-NEW"
	newest.PrimarySlot.CtrlAddr = "c1-NEW"

	if err := owner.SubmitSnapshot(olderBoth); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	if err := owner.SubmitSnapshot(intermediate); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if err := owner.SubmitSnapshot(newest); err != nil {
		t.Fatalf("submit 3: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ask, err := owner.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ask.Intent != IntentBind {
		t.Fatalf("intent: got %s want Bind", ask.Intent)
	}
	if ask.DataAddr != "d1-NEW" || ask.CtrlAddr != "c1-NEW" {
		t.Fatalf("expected latest snapshot addrs, got data=%q ctrl=%q",
			ask.DataAddr, ask.CtrlAddr)
	}
}

// ============================================================
// §13.3 — Boundary tests
// ============================================================

// The non-forgeability test already walks the repo and enforces
// "policy code cannot construct AssignmentInfo directly" — policy.go
// lives in core/authority/, so it is allowed to reference types
// from adapter for consumption, but the test flags any
// AssignmentInfo construction. This test file proves NO construction
// happens in the policy implementation by grepping at the source
// level the decision paths must only produce AssignmentAsk values.
func TestPolicy_NoDirectAssignmentInfoConstruction(t *testing.T) {
	// The real proof lives in
	// TestNonForgeability_NoAssignmentInfoMintingOutsideAuthority —
	// which audits the whole repo, including core/authority/. That
	// test would fail if policy.go minted AssignmentInfo. This
	// stub asserts the audit runs cleanly on the current tree.
	if _, err := auditNonForgeability("policy.go"); err != nil {
		t.Fatalf("audit: %v", err)
	}
	// Deeper coverage is the whole-repo audit:
	t.Run("delegates to repo-wide audit", TestNonForgeability_NoAssignmentInfoMintingOutsideAuthority)
}

// TestPolicy_DoesNotAddAdapterIngress — the only verb the policy
// exposes to callers is Next (Directive) + SubmitSnapshot +
// LastUnsupported. None of them writes to the adapter directly.
// We assert the Directive interface is satisfied (so Publisher can
// consume it) and that PolicyOwner has no OnAssignment-style
// method accidentally added.
func TestPolicy_DoesNotAddAdapterIngress(t *testing.T) {
	var _ Directive = (*PolicyOwner)(nil) // compile-time check

	// Reflectively guard against an accidentally-added
	// OnAssignment method on PolicyOwner. This catches a refactor
	// that routes decisions through a direct adapter call.
	// Using a type assertion against a narrow interface is
	// simplest.
	type hiddenAdapterIngress interface {
		OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog
	}
	var po any = (*PolicyOwner)(nil)
	if _, ok := po.(hiddenAdapterIngress); ok {
		t.Fatal("PolicyOwner must not expose OnAssignment — adapter ingress stays on VolumeReplicaAdapter only")
	}
}

func TestPolicy_OnlyThreeIntentsProduced(t *testing.T) {
	// Run the decision table over a spread of inputs. Every ask
	// produced must be one of Bind / RefreshEndpoint / Reassign —
	// Remove / retirement intents do not appear in this slice.
	reader := newFakeBasisReader()
	reader.Set("v1", "r1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)

	cases := []struct {
		name string
		snap PolicySnapshot
	}{
		{"bind-only-primary", PolicySnapshot{VolumeID: "v1",
			PrimarySlot: acceptableSlot("r1", "s1", "d1", "c1"),
		}},
		{"refresh-data", PolicySnapshot{VolumeID: "v1",
			Authority: AuthorityBasis{
				Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
				DataAddr: "d1", CtrlAddr: "c1",
			},
			PrimarySlot:   acceptableSlot("r1", "s1", "d1-new", "c1"),
			SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
		}},
		{"reassign", PolicySnapshot{VolumeID: "v1",
			Authority: AuthorityBasis{
				Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
				DataAddr: "d1", CtrlAddr: "c1",
			},
			PrimarySlot: func() SlotObservation {
				s := acceptableSlot("r1", "s1", "d1", "c1")
				s.Reachable = false
				return s
			}(),
			SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
		}},
	}
	for _, c := range cases {
		ask, kind := owner.decide(c.snap)
		if kind != decisionEmit {
			continue
		}
		switch ask.Intent {
		case IntentBind, IntentRefreshEndpoint, IntentReassign:
			// ok
		default:
			t.Fatalf("%s: illegal intent produced: %s", c.name, ask.Intent)
		}
	}
}

func TestPolicy_UnsupportedTopology_AlwaysEmitsEvidence(t *testing.T) {
	reader := newFakeBasisReader()
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, reader)
	// Authority points to a replica not in either slot.
	snap := PolicySnapshot{
		VolumeID:          "v1",
		CollectedRevision: 7,
		Authority: AuthorityBasis{
			Assigned: true, ReplicaID: "r-unknown", Epoch: 1, EndpointVersion: 1,
		},
		PrimarySlot:   acceptableSlot("r1", "s1", "d1", "c1"),
		SecondarySlot: acceptableSlot("r2", "s2", "d2", "c2"),
	}
	expectNoEmit(t, owner, snap)

	ev, ok := owner.LastUnsupported()
	if !ok {
		t.Fatal("unsupported topology must not be silent idle — evidence required")
	}
	if ev.SnapshotRevision != 7 {
		t.Fatalf("evidence revision: got %d want 7", ev.SnapshotRevision)
	}
}

// ============================================================
// §13.4 — End-to-end closure tests
// ============================================================

// policyClosureExecutor is a minimal CommandExecutor that drives
// the adapter to Healthy on a caught-up replica.
//
// Fence behavior is configurable:
//   - default: fires onFenceComplete inline (deterministic for
//     tests that do not need to observe the ack-gated window).
//   - after ArmHoldNextFence: the NEXT Fence call stashes its
//     result in heldResult instead of firing. Tests release it
//     via ReleaseHeldFence() once they have observed the
//     transitional state (adapter at new Epoch but not yet
//     Healthy). One-shot per arm.
type policyClosureExecutor struct {
	mu              sync.Mutex
	onFenceComplete adapter.OnFenceComplete
	nextSession     atomic.Uint64

	holdNext   bool
	heldResult *adapter.FenceResult
}

func newPolicyClosureExecutor() *policyClosureExecutor {
	pe := &policyClosureExecutor{}
	pe.nextSession.Store(2000)
	return pe
}

func (e *policyClosureExecutor) SetOnSessionStart(fn adapter.OnSessionStart) {}
func (e *policyClosureExecutor) SetOnSessionClose(fn adapter.OnSessionClose) {}
func (e *policyClosureExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete) {
	e.mu.Lock()
	e.onFenceComplete = fn
	e.mu.Unlock()
}

func (e *policyClosureExecutor) Probe(replicaID, dataAddr, ctrlAddr string, sessionID, epoch, endpointVersion uint64) adapter.ProbeResult {
	return adapter.ProbeResult{
		ReplicaID: replicaID, Success: true,
		EndpointVersion: endpointVersion, TransportEpoch: epoch,
		ReplicaFlushedLSN: 100, PrimaryTailLSN: 10, PrimaryHeadLSN: 100,
	}
}

func (e *policyClosureExecutor) StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, fromLSN, targetLSN uint64) error {
	return nil
}
func (e *policyClosureExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	return nil
}
func (e *policyClosureExecutor) StartRecoverySession(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64, contentKind engine.RecoveryContentKind, policy engine.RecoveryRuntimePolicy) error {
	return nil
}
func (e *policyClosureExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {}
func (e *policyClosureExecutor) PublishHealthy(replicaID string)                                     {}
func (e *policyClosureExecutor) PublishDegraded(replicaID string, reason string)                     {}

func (e *policyClosureExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	result := adapter.FenceResult{
		ReplicaID: replicaID, SessionID: sessionID,
		Epoch: epoch, EndpointVersion: endpointVersion,
		Success: true,
	}
	e.mu.Lock()
	if e.holdNext {
		e.heldResult = &result
		e.holdNext = false
		e.mu.Unlock()
		return nil
	}
	cb := e.onFenceComplete
	e.mu.Unlock()
	if cb != nil {
		cb(result)
	}
	return nil
}

// ArmHoldNextFence causes the next Fence() call to stash its
// FenceResult instead of firing onFenceComplete. Used by the
// publication-honesty-during-transition test to observe the
// ack-gated window deterministically.
func (e *policyClosureExecutor) ArmHoldNextFence() {
	e.mu.Lock()
	e.holdNext = true
	e.mu.Unlock()
}

// HasHeldFence reports whether a held FenceResult is waiting to
// be released.
func (e *policyClosureExecutor) HasHeldFence() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.heldResult != nil
}

// ReleaseHeldFence fires the previously-held FenceResult (if any)
// to onFenceComplete. Returns true if a release happened.
func (e *policyClosureExecutor) ReleaseHeldFence() bool {
	e.mu.Lock()
	held := e.heldResult
	e.heldResult = nil
	cb := e.onFenceComplete
	e.mu.Unlock()
	if held == nil || cb == nil {
		return false
	}
	cb(*held)
	return true
}

func waitHealthy(t *testing.T, a *adapter.VolumeReplicaAdapter, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if a.Projection().Mode == engine.ModeHealthy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("adapter did not reach Healthy; mode=%s", a.Projection().Mode)
}

// waitHealthyAtEpoch waits for the adapter to report Healthy AT or
// above the given epoch. Distinct from waitHealthy because an
// adapter that was already Healthy from a prior bind satisfies
// waitHealthy trivially — failover/refresh tests need to confirm
// the identity actually advanced to the expected epoch.
func waitHealthyAtEpoch(t *testing.T, a *adapter.VolumeReplicaAdapter, minEpoch uint64, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		p := a.Projection()
		if p.Mode == engine.ModeHealthy && p.Epoch >= minEpoch {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	p := a.Projection()
	t.Fatalf("adapter did not reach Healthy at epoch>=%d; mode=%s epoch=%d",
		minEpoch, p.Mode, p.Epoch)
}

// TestPolicyToAdapter_E2E_Bind proves the full S3→S2 route for a
// first-time bind:
//   policy snapshot -> PolicyOwner.Next -> Publisher -> Bridge ->
//   adapter.OnAssignment -> engine Healthy.
func TestPolicyToAdapter_E2E_Bind(t *testing.T) {
	exec := newPolicyClosureExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)

	pub := NewPublisher(nil) // Directive wired below
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, pub)
	pub.dir = owner // the publisher drives the policy owner as its directive

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)
	go Bridge(ctx, pub, a, "v1", "r1")

	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     AuthorityBasis{Assigned: false},
		PrimarySlot:   acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334"),
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitHealthy(t, a, 2*time.Second)
}

// TestPolicyToAdapter_E2E_Failover — initial Bind on primary, then
// primary becomes unreachable, policy emits Reassign, adapter
// re-anchors and reaches Healthy on the new epoch.
//
// This test uses ONE VolumeBridge for the accepted two-slot
// topology, started at the top and never torn down. The adapter
// retarget from r1 to r2 happens through the authority route —
// no test-side bridge teardown-and-retarget choreography. That is
// the honest closure claim of "failover is system-driven".
func TestPolicyToAdapter_E2E_Failover(t *testing.T) {
	exec := newPolicyClosureExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)

	pub := NewPublisher(nil)
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, pub)
	pub.dir = owner

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)

	// One VolumeBridge covers both accepted slots. It spawns
	// internal per-slot Bridges and forwards each delivery to the
	// same adapter. The adapter's monotonic applyAssignment
	// handles the r1→r2 identity advance on cross-slot Reassign.
	go VolumeBridge(ctx, pub, a, "v1", "r1", "r2")

	// Initial bind.
	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     AuthorityBasis{Assigned: false},
		PrimarySlot:   acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334"),
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	waitHealthy(t, a, 2*time.Second)

	// Failover snapshot: primary unreachable, secondary healthy.
	base, _ := pub.LastAuthorityBasis("v1", "r1")
	primary := acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334")
	primary.Reachable = false
	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     base,
		PrimarySlot:   primary,
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	// Wait for the adapter to move to the new epoch via the same
	// VolumeBridge — no test-side retargeting.
	waitHealthyAtEpoch(t, a, 2, 2*time.Second)

	// Verify the epoch bumped (Reassign bumps epoch, resets EV=1)
	// and the new replica is r2 — cross-slot failover worked.
	final, ok := pub.LastAuthorityBasis("v1", "r2")
	if !ok {
		t.Fatal("post-failover: no basis published for r2")
	}
	if final.Epoch != 2 {
		t.Fatalf("post-failover epoch: got %d want 2", final.Epoch)
	}
	if final.EndpointVersion != 1 {
		t.Fatalf("post-failover EV: got %d want 1 (Reassign resets EV)", final.EndpointVersion)
	}
}

// TestVolumeBridge_FollowsFailoverWithoutTestChoreography is the
// direct closure regression for finding 2: the S3 failover path
// must be system-owned. One VolumeBridge is started at the top of
// the test; no bridge cancel/restart happens during the test.
// After cross-slot failover, the adapter reports Healthy at the
// new epoch without any test orchestration.
func TestVolumeBridge_FollowsFailoverWithoutTestChoreography(t *testing.T) {
	exec := newPolicyClosureExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)

	pub := NewPublisher(nil)
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, pub)
	pub.dir = owner

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ONE VolumeBridge for the accepted two-slot topology; no
	// per-slot Bridge calls in this test.
	go pub.Run(ctx)
	go VolumeBridge(ctx, pub, a, "v1", "r1", "r2")

	// Initial bind.
	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     AuthorityBasis{Assigned: false},
		PrimarySlot:   acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334"),
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	waitHealthyAtEpoch(t, a, 1, 2*time.Second)

	// Cross-slot failover: primary unreachable.
	base, _ := pub.LastAuthorityBasis("v1", "r1")
	primary := acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334")
	primary.Reachable = false
	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     base,
		PrimarySlot:   primary,
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}

	// Without any test-side retarget, the adapter must reach
	// Healthy at epoch=2 on the new replica.
	waitHealthyAtEpoch(t, a, 2, 2*time.Second)
}

// TestPolicyToAdapter_E2E_EndpointRefresh — initial bind, then the
// primary's data address changes, policy emits RefreshEndpoint,
// publisher bumps EndpointVersion, adapter re-anchors on same
// epoch with new EV.
func TestPolicyToAdapter_E2E_EndpointRefresh(t *testing.T) {
	exec := newPolicyClosureExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)

	pub := NewPublisher(nil)
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, pub)
	pub.dir = owner

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)
	go Bridge(ctx, pub, a, "v1", "r1")

	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     AuthorityBasis{Assigned: false},
		PrimarySlot:   acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334"),
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	waitHealthy(t, a, 2*time.Second)

	// DataAddr changes on the primary.
	base, _ := pub.LastAuthorityBasis("v1", "r1")
	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     base,
		PrimarySlot:   acceptableSlot("r1", "s1", "10.0.0.2:9335", "10.0.0.2:9334"),
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}

	// Wait for EV to advance on r1.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b, ok := pub.LastAuthorityBasis("v1", "r1"); ok && b.EndpointVersion == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	b, _ := pub.LastAuthorityBasis("v1", "r1")
	t.Fatalf("EndpointVersion did not advance to 2; basis=%+v", b)
}

// TestVolumeBridge_PostFailoverOldSlotRefreshRejectedByAdapter
// closes the 14A gap "preserved old-slot state must not perturb
// the new authoritative state". After cross-slot failover moves
// authority from r1@1 to r2@2 and the adapter is Healthy at
// epoch=2, an out-of-band RefreshEndpoint on the OLD slot r1
// (which bumps publisher state to r1@epoch=1,EV=2) is delivered
// through VolumeBridge. The adapter must reject it on the
// engine-side monotonic-epoch guard (e.Epoch=1 < current=2) and
// remain Healthy at epoch=2, unperturbed.
//
// The test uses pub.apply() directly to inject the old-slot
// refresh because the policy decision table would never emit such
// a refresh once authority has moved. The point here is adapter-
// level rejection of stale-identity input delivered through the
// real VolumeBridge route.
func TestVolumeBridge_PostFailoverOldSlotRefreshRejectedByAdapter(t *testing.T) {
	exec := newPolicyClosureExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)

	pub := NewPublisher(nil)
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, pub)
	pub.dir = owner

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)
	go VolumeBridge(ctx, pub, a, "v1", "r1", "r2")

	// Step 1: Bind on r1 + wait for epoch=1 Healthy.
	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     AuthorityBasis{Assigned: false},
		PrimarySlot:   acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334"),
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit bind: %v", err)
	}
	waitHealthyAtEpoch(t, a, 1, 2*time.Second)

	// Step 2: cross-slot failover to r2 + wait for epoch=2 Healthy.
	base, _ := pub.LastAuthorityBasis("v1", "r1")
	primary := acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334")
	primary.Reachable = false
	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     base,
		PrimarySlot:   primary,
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit failover: %v", err)
	}
	waitHealthyAtEpoch(t, a, 2, 2*time.Second)
	preEpoch := a.Projection().Epoch
	preMode := a.Projection().Mode

	// Step 3: inject an out-of-band RefreshEndpoint on the old slot
	// r1. This bumps publisher state to (r1, epoch=1, EV=2) and
	// the VolumeBridge-internal Bridge for r1 delivers it to the
	// adapter. The adapter's engine is at (r2, epoch=2); the new
	// assignment's Epoch=1 is strictly less, so the engine drops
	// it as stale.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "10.0.0.2:CHANGED", CtrlAddr: "10.0.0.2:9334",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("out-of-band Refresh on r1: %v", err)
	}

	// Give the bridge time to forward and the engine time to reject.
	time.Sleep(150 * time.Millisecond)

	post := a.Projection()
	if post.Mode != preMode {
		t.Fatalf("adapter Mode perturbed by old-slot refresh: pre=%s post=%s",
			preMode, post.Mode)
	}
	if post.Epoch != preEpoch {
		t.Fatalf("adapter Epoch perturbed by old-slot refresh: pre=%d post=%d",
			preEpoch, post.Epoch)
	}
	if post.Epoch != 2 {
		t.Fatalf("adapter Epoch: got %d want 2 (unperturbed)", post.Epoch)
	}
	if post.Mode != engine.ModeHealthy {
		t.Fatalf("adapter Mode: got %s want Healthy (unperturbed)", post.Mode)
	}
}

// TestPolicyToAdapter_E2E_Failover_PublicationHonestyDuringTransition
// closes the 14A "publication honesty during authority transition"
// proof gap. When policy moves authority r1@1 -> r2@2, the adapter
// MUST NOT remain Healthy throughout the transition: after the new
// assignment lands, Publication must be held non-Healthy until the
// fence ack gates it (P14 S1's ack-gated Healthy contract). Only
// after FenceCompleted does Mode flip back to Healthy at the new
// epoch.
//
// The test uses the fence-hold hook on the executor to observe the
// transitional state deterministically rather than racing the
// inline fence callback.
func TestPolicyToAdapter_E2E_Failover_PublicationHonestyDuringTransition(t *testing.T) {
	exec := newPolicyClosureExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)

	pub := NewPublisher(nil)
	owner := NewPolicyOwner(PolicyConfig{VolumeID: "v1"}, pub)
	pub.dir = owner

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)
	go VolumeBridge(ctx, pub, a, "v1", "r1", "r2")

	// Initial Bind at epoch=1 with inline fence (default).
	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     AuthorityBasis{Assigned: false},
		PrimarySlot:   acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334"),
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit bind: %v", err)
	}
	waitHealthyAtEpoch(t, a, 1, 2*time.Second)

	// Hold the NEXT fence — this will be the fence for the r2@2
	// assignment that the failover Reassign will produce.
	exec.ArmHoldNextFence()

	// Failover.
	base, _ := pub.LastAuthorityBasis("v1", "r1")
	primary := acceptableSlot("r1", "s1", "10.0.0.2:9333", "10.0.0.2:9334")
	primary.Reachable = false
	if err := owner.SubmitSnapshot(PolicySnapshot{
		VolumeID:      "v1",
		Authority:     base,
		PrimarySlot:   primary,
		SecondarySlot: acceptableSlot("r2", "s2", "10.0.0.3:9333", "10.0.0.3:9334"),
	}); err != nil {
		t.Fatalf("submit failover: %v", err)
	}

	// Wait until the adapter has taken on the new identity (epoch=2)
	// AND the fence is in flight (held). During this window the
	// engine's ack-gated Publication must hold Mode non-Healthy.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a.Projection().Epoch == 2 && exec.HasHeldFence() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	proj := a.Projection()
	if proj.Epoch != 2 {
		t.Fatalf("transition window: adapter did not advance to Epoch=2 (got %d)", proj.Epoch)
	}
	if !exec.HasHeldFence() {
		t.Fatal("transition window: fence was not held as expected")
	}

	// Publication honesty: the adapter MUST NOT claim Healthy
	// while the fence is still in flight under the new epoch.
	if proj.Mode == engine.ModeHealthy {
		t.Fatalf("publication honesty violated: adapter Healthy at Epoch=2 before fence ack")
	}

	// Release the held fence; adapter should flip to Healthy@2.
	if !exec.ReleaseHeldFence() {
		t.Fatal("ReleaseHeldFence returned false")
	}
	waitHealthyAtEpoch(t, a, 2, 2*time.Second)
}
