package authority

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// #183: Bind is genesis for the volume. These cover the branches around
// the per-volume check; the Run-path reproducer is
// TestPublisher_RunRejectsSecondSlotGenesisBind.

func bindAsk(volumeID, replicaID string) AssignmentAsk {
	return AssignmentAsk{VolumeID: volumeID, ReplicaID: replicaID, DataAddr: "d-" + replicaID, CtrlAddr: "c-" + replicaID, Intent: IntentBind}
}

func reassignAsk(volumeID, replicaID string) AssignmentAsk {
	ask := bindAsk(volumeID, replicaID)
	ask.Intent = IntentReassign
	ask.Evidence = testPromotion
	return ask
}

func TestPublisher_BindSecondSlotOfBoundVolumeRejected(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(bindAsk("v1", "r1")); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := pub.apply(bindAsk("v1", "r2")); !errors.Is(err, ErrBindAlreadyBound) {
		t.Fatalf("second-slot Bind: want ErrBindAlreadyBound, got %v", err)
	}
	if got, ok := pub.LastPublished("v1", "r2"); ok {
		t.Fatalf("second slot was published: %+v", got)
	}
	if got, ok := pub.LastPublished("v1", "r1"); !ok || got.Epoch != 1 || got.EndpointVersion != 1 {
		t.Fatalf("first slot line changed: %+v ok=%t", got, ok)
	}
}

func TestPublisher_BindOtherVolumeUnaffected(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(bindAsk("v1", "r1")); err != nil {
		t.Fatalf("Bind v1: %v", err)
	}
	if err := pub.apply(bindAsk("v2", "r2")); err != nil {
		t.Fatalf("Bind of a fresh volume rejected: %v", err)
	}
	if got, ok := pub.LastPublished("v2", "r2"); !ok || got.Epoch != 1 {
		t.Fatalf("fresh volume line=%+v ok=%t want epoch 1", got, ok)
	}
}

func TestPublisher_ReassignToOtherSlotStillAllowedThenBindRejected(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(bindAsk("v1", "r1")); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := pub.apply(reassignAsk("v1", "r2")); err != nil {
		t.Fatalf("Reassign to another slot: %v", err)
	}
	if got, ok := pub.LastPublished("v1", "r2"); !ok || got.Epoch != 2 {
		t.Fatalf("reassigned line=%+v ok=%t want epoch 2", got, ok)
	}
	for _, r := range []string{"r1", "r2", "r3"} {
		if err := pub.apply(bindAsk("v1", r)); !errors.Is(err, ErrBindAlreadyBound) {
			t.Fatalf("Bind %s after Reassign: want ErrBindAlreadyBound, got %v", r, err)
		}
	}
	if _, ok := pub.LastPublished("v1", "r3"); ok {
		t.Fatal("r3 was published by a Bind")
	}
}

// The file store keeps one record per volume, so after a restart only the
// current slot is in memory. A Bind for the earlier slot must still be
// refused: the volume, not the key, is bound.
func TestPublisher_BindAfterRestartRejectedForAnySlot(t *testing.T) {
	dir := t.TempDir()
	storeA, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	pubA := NewPublisher(NewStaticDirective(nil), WithStore(storeA))
	if err := pubA.apply(bindAsk("v1", "r1")); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := pubA.apply(reassignAsk("v1", "r2")); err != nil {
		t.Fatalf("Reassign: %v", err)
	}
	_ = storeA.Close()

	storeB, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer storeB.Close()
	pubB := NewPublisher(NewStaticDirective(nil), WithStore(storeB))
	for _, r := range []string{"r1", "r2", "r3"} {
		if err := pubB.apply(bindAsk("v1", r)); !errors.Is(err, ErrBindAlreadyBound) {
			t.Fatalf("Bind %s after restart: want ErrBindAlreadyBound, got %v", r, err)
		}
	}
	if got, ok := pubB.LastPublished("v1", "r2"); !ok || got.Epoch != 2 {
		t.Fatalf("reloaded line=%+v ok=%t want r2 epoch 2", got, ok)
	}
}

// The one mint never issues a primary change its target could not check: a
// reassign without a kind is refused and the line stays.
func TestPublisher_ReassignWithoutAKindIsRefused(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(bindAsk("v1", "r1")); err != nil {
		t.Fatal(err)
	}
	ask := bindAsk("v1", "r2")
	ask.Intent = IntentReassign
	if err := pub.apply(ask); !errors.Is(err, ErrReassignWithoutKind) {
		t.Fatalf("a reassign without a kind: %v; want ErrReassignWithoutKind", err)
	}
	if line, _ := pub.VolumeAuthorityLine("v1"); line.ReplicaID != "r1" || line.Epoch != 1 {
		t.Fatalf("the line moved: %+v", line)
	}
	ask.Evidence = testPromotion
	if err := pub.apply(ask); err != nil {
		t.Fatalf("a reassign with a kind: %v", err)
	}
}

// A decision made on a fencing round at e+1 is minted only while the line
// is still at e: once the line moved, it is refused and nothing changes.
func TestPublisher_AFencedDecisionIsMintedOnlyAtItsFencedEpoch(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(bindAsk("v1", "r1")); err != nil {
		t.Fatal(err)
	}
	ask := reassignAsk("v1", "r2")
	ask.Evidence.FencedEpoch = 3 // fenced for a line at 2; the line is at 1
	if err := pub.apply(ask); !errors.Is(err, ErrFenceSuperseded) {
		t.Fatalf("a decision fenced at 3 on a line at 1: %v; want ErrFenceSuperseded", err)
	}
	if line, _ := pub.VolumeAuthorityLine("v1"); line.ReplicaID != "r1" || line.Epoch != 1 {
		t.Fatalf("the line moved: %+v", line)
	}
	ask.Evidence.FencedEpoch = 2
	if err := pub.apply(ask); err != nil {
		t.Fatalf("a decision fenced at the next epoch: %v", err)
	}
}

// A promotion is minted only on a fencing round's decision or as a guarded
// planned move: one with neither (a decision read from a plain round) is
// refused, so no path can skip the fence.
func TestPublisher_AnUnfencedPromotionIsRefused(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(bindAsk("v1", "r1")); err != nil {
		t.Fatal(err)
	}
	ask := bindAsk("v1", "r2")
	ask.Intent = IntentReassign
	ask.Evidence = adapter.PrimaryEvidence{Kind: adapter.PrimaryKindPromotion, DurableLSN: 9}
	if err := pub.apply(ask); !errors.Is(err, ErrPromotionUnfenced) {
		t.Fatalf("an unfenced, unplanned promotion: %v; want ErrPromotionUnfenced", err)
	}
	if line, _ := pub.VolumeAuthorityLine("v1"); line.ReplicaID != "r1" || line.Epoch != 1 {
		t.Fatalf("the line moved: %+v", line)
	}
	ask.Evidence.Planned = true
	if err := pub.apply(ask); err != nil {
		t.Fatalf("a guarded planned move: %v", err)
	}
}
