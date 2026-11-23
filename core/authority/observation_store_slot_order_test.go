package authority

import (
	"fmt"
	"testing"
	"time"
)

// V1-SPEC r2.53: independent processes on one server order only their
// own (volume, replica) reports; another slot's later timestamp is irrelevant.
func TestObservationStore_InterleavedSendersOrderPerSlot(t *testing.T) {
	for _, n := range []int{10, 100} {
		t.Run(fmt.Sprintf("%d slots", n), func(t *testing.T) {
			base := time.Unix(1000, 0)
			now := base.Add(3 * time.Second)
			s := NewObservationStore(FreshnessConfig{FreshnessWindow: 10 * time.Second}, func() time.Time { return now })
			for round := 0; round < 2; round++ {
				for i := n - 1; i >= 0; i-- {
					slot := healthySlot(fmt.Sprintf("v%d", i/2), fmt.Sprintf("r%d", i%2+1))
					slot.DataAddr = fmt.Sprintf("round-%d-slot-%d", round, i)
					at := base.Add(time.Duration(round)*time.Second + time.Duration(i)*time.Millisecond)
					obs, err := HeartbeatToObservation(serverObs("one-host", at, slot))
					if err != nil {
						t.Fatal(err)
					}
					if err := s.Ingest(obs); err != nil {
						t.Fatal(err)
					}
				}
			}
			for i := 0; i < n; i++ {
				got, ok := s.SlotFactOnServer("one-host", fmt.Sprintf("v%d", i/2), fmt.Sprintf("r%d", i%2+1))
				if !ok || got.DataAddr != fmt.Sprintf("round-1-slot-%d", i) {
					t.Errorf("slot %d lost an independent sender's heartbeat: fact=%+v present=%v", i, got, ok)
				}
			}
			if got := s.SupersededCount(); got != 0 {
				t.Fatalf("other senders superseded %d slot reports", got)
			}
		})
	}
}

func TestObservationStore_MixedSlotReportKeepsOnlyNewerFacts(t *testing.T) {
	base := time.Unix(1000, 0)
	now := base.Add(4 * time.Second)
	s := NewObservationStore(FreshnessConfig{FreshnessWindow: 10 * time.Second}, func() time.Time { return now })
	report := func(at time.Duration, slots ...SlotFact) []SlotHealthTransition {
		t.Helper()
		tr, err := s.IngestWithHealthTransitions(Observation{ServerID: "one-host", ObservedAt: base.Add(at), Slots: slots})
		if err != nil {
			t.Fatal(err)
		}
		return tr
	}
	a, b, c := healthySlot("a", "r1"), healthySlot("b", "r1"), healthySlot("c", "r1")
	report(3*time.Second, a)
	report(time.Second, b)
	a.UnhealthyReason, b.UnhealthyReason, c.UnhealthyReason = "stale", "new-b", "new-c"
	mutations := 0
	s.SetOnMutation(func() { mutations++ })
	tr := report(2*time.Second, a, b, c)
	if len(tr) != 2 || tr[0].VolumeID != "b" || tr[0].New != "new-b" || tr[1].VolumeID != "c" || tr[1].New != "new-c" {
		t.Fatalf("mixed report transitions=%+v; want only b and c", tr)
	}
	if got, ok := s.SlotFactOnServer("one-host", "a", "r1"); !ok || got.UnhealthyReason != "" {
		t.Fatalf("stale a replaced its newer fact: %+v, %v", got, ok)
	}
	if s.SupersededCount() != 1 || mutations != 1 {
		t.Fatalf("mixed report: superseded=%d mutations=%d; want 1/1", s.SupersededCount(), mutations)
	}
	rev := s.Snapshot().revision
	if tr := report(time.Second, a, b, c); len(tr) != 0 {
		t.Fatalf("all-stale report emitted transitions: %+v", tr)
	}
	if s.SupersededCount() != 4 || mutations != 1 || s.Snapshot().revision != rev {
		t.Fatalf("all-stale report: superseded=%d mutations=%d revision=%d; want 4/1/%d", s.SupersededCount(), mutations, s.Snapshot().revision, rev)
	}
	// The server's timestamp stays at its newest report; each accepted slot
	// keeps its own expiry, including after a delayed mixed report.
	now = base.Add(12500 * time.Millisecond)
	if _, ok := s.SlotFactOnServer("one-host", "a", "r1"); !ok {
		t.Fatal("a lost its later expiry to another slot's older report")
	}
	for _, volume := range []string{"b", "c"} {
		if _, ok := s.SlotFactOnServer("one-host", volume, "r1"); ok {
			t.Fatalf("%s inherited a's later freshness", volume)
		}
	}
}
