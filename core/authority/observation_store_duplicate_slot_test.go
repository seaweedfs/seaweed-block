package authority

import (
	"errors"
	"testing"
	"time"
)

// Review of #134 r2: an observation carrying the same volume/replica
// slot twice is refused before any mutation, so a duplicate can never
// fabricate a health transition that does not describe the stored slot.

func dupStore(t *testing.T) (*ObservationStore, time.Time) {
	t.Helper()
	base := time.Unix(1000, 0)
	return NewObservationStore(FreshnessConfig{FreshnessWindow: time.Hour}, func() time.Time { return base.Add(time.Minute) }), base
}

func slotWith(reason string) SlotFact {
	return SlotFact{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Reachable: true, Eligible: reason == "", UnhealthyReason: reason}
}

func TestObservationStore_DuplicateSlotKeysRefused(t *testing.T) {
	for _, tc := range []struct {
		name  string
		slots []SlotFact
	}{
		{"conflicting", []SlotFact{slotWith(""), slotWith("recovery_timeout")}},
		{"identical", []SlotFact{slotWith("recovery_timeout"), slotWith("recovery_timeout")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, base := dupStore(t)
			if _, err := s.IngestWithHealthTransitions(Observation{ServerID: "s1", ObservedAt: base, Slots: []SlotFact{slotWith("recovery_timeout")}}); err != nil {
				t.Fatal(err)
			}
			rev := s.Snapshot().revision
			tr, err := s.IngestWithHealthTransitions(Observation{ServerID: "s1", ObservedAt: base.Add(time.Second), Slots: tc.slots})
			if !errors.Is(err, ErrInvalidObservation) {
				t.Fatalf("duplicate slot keys: err=%v; want ErrInvalidObservation", err)
			}
			if len(tr) != 0 {
				t.Fatalf("refused observation produced transitions %+v", tr)
			}
			if got := s.Snapshot().revision; got != rev {
				t.Fatalf("refused observation mutated the store: revision %d -> %d", rev, got)
			}
			if f, ok := s.SlotFact("v1", "r1"); !ok || f.UnhealthyReason != "recovery_timeout" {
				t.Fatalf("stored slot after refusal: %+v %t; want the earlier recovery_timeout fact", f, ok)
			}
		})
	}
}

// Positive: distinct slots in one observation are accepted, and each
// changed slot yields its own transition.
func TestObservationStore_UniqueMultiSlotTransitions(t *testing.T) {
	s, base := dupStore(t)
	a := SlotFact{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Reachable: true, Eligible: true}
	b := SlotFact{VolumeID: "v2", ReplicaID: "r1", DataAddr: "d2", CtrlAddr: "c2", Reachable: true, Eligible: true}
	if _, err := s.IngestWithHealthTransitions(Observation{ServerID: "s1", ObservedAt: base, Slots: []SlotFact{a, b}}); err != nil {
		t.Fatal(err)
	}
	a2, b2 := a, b
	a2.Eligible, a2.UnhealthyReason = false, "recovery_timeout"
	b2.Eligible, b2.UnhealthyReason = false, "role_admission_failed"
	tr, err := s.IngestWithHealthTransitions(Observation{ServerID: "s1", ObservedAt: base.Add(time.Second), Slots: []SlotFact{a2, b2}})
	if err != nil {
		t.Fatal(err)
	}
	if len(tr) != 2 || tr[0].VolumeID != "v1" || tr[0].New != "recovery_timeout" || tr[1].VolumeID != "v2" || tr[1].New != "role_admission_failed" {
		t.Fatalf("transitions %+v; want v1 -> recovery_timeout and v2 -> role_admission_failed", tr)
	}
}
