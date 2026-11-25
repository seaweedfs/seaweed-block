package master

import (
	"errors"
	"testing"
)

// A planned move without a seal is safe exactly when the target is in every
// acknowledgement set: one row per ack profile and replica count.
func TestPlannedMoveAllowedByAckProfileAndReplicas(t *testing.T) {
	for _, c := range []struct {
		profile  string
		replicas int
		want     error
	}{
		{"sync_all", 2, nil},
		{"sync_all", 3, nil},
		{"sync-quorum", 2, nil},
		{"sync_quorum", 3, ErrPlannedMoveNeedsSeal},
		{"best_effort", 2, ErrPlannedMoveBestEffort},
		{"best_effort", 3, ErrPlannedMoveBestEffort},
	} {
		if err := plannedMoveAllowed(c.profile, c.replicas); !errors.Is(err, c.want) && !(c.want == nil && err == nil) {
			t.Errorf("%s RF%d: %v; want %v", c.profile, c.replicas, err, c.want)
		}
	}
	if err := plannedMoveAllowed("unknown", 2); err == nil {
		t.Error("an unknown ack profile was allowed")
	}
}
