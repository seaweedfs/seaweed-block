package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// The promotion gate's direct reassign waits only for a restarted primary
// that is placeable by its own report and inside its boot grace (#128).
func TestPrimaryBootGraceHolds_OnlyForPlaceableRestartedPrimary(t *testing.T) {
	ctrl := authority.NewTopologyController(authority.TopologyControllerConfig{PrimaryBootGrace: 30 * time.Second}, nil)
	restarted := authority.ReplicaCandidate{
		ReplicaID: "r1", ServerID: "s1", Reachable: true, Eligible: true, Incarnation: "r1-b",
	}
	if !primaryBootGraceHolds(ctrl, "v1", restarted) {
		t.Fatal("restarted, placeable, not-ready primary: want the boot grace to hold")
	}
	for name, mut := range map[string]func(*authority.ReplicaCandidate){
		"ready":          func(c *authority.ReplicaCandidate) { c.ReadyForPrimary = true },
		"unreachable":    func(c *authority.ReplicaCandidate) { c.Reachable = false },
		"ineligible":     func(c *authority.ReplicaCandidate) { c.Eligible = false },
		"withdrawn":      func(c *authority.ReplicaCandidate) { c.Withdrawn = true },
		"no incarnation": func(c *authority.ReplicaCandidate) { c.Incarnation = "" },
	} {
		c := restarted
		mut(&c)
		if primaryBootGraceHolds(ctrl, "v1", c) {
			t.Errorf("%s: the boot grace must not hold", name)
		}
	}
	if primaryBootGraceHolds(nil, "v1", restarted) {
		t.Error("nil controller: must not hold")
	}
}
