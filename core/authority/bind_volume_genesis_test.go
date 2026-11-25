package authority

import (
	"errors"
	"testing"
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
