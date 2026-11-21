package durable

import (
	"fmt"
	"testing"
	"time"
)

func TestMaintenanceVolumePhasesDoNotAlign(t *testing.T) {
	interval := 15 * time.Minute
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		s := initialCutSchedule(fmt.Sprintf("volume-%d", i), interval, t0, true)
		offset := s.firstDue.Sub(t0)
		if offset <= 0 || offset >= interval {
			t.Fatalf("phase outside interval: %s", offset)
		}
		seen[offset] = true
		if _, due := testPolicy().due(s, s.firstDue.Add(-time.Nanosecond), false); due {
			t.Fatal("published before own phase")
		}
	}
	if len(seen) < 190 {
		t.Fatalf("200 volumes collapsed into %d phases", len(seen))
	}
}
