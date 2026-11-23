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
