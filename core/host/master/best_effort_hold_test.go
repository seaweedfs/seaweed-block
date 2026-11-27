package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

func blockedFor(h *Host, volume, reason string) bool {
	for _, e := range h.events.list(volume) {
		if e.Type == "volume_blocked" && e.Reason == reason {
			return true
		}
	}
	return false
}

// A best_effort volume's lost primary is not replaced automatically, on
// either gate: no fencing round, the line stays, and the volume shows the
// reason. A best_effort write may be on the primary alone, the same reason
// the operator's move refuses best_effort (plannedMoveAllowed). The same
// setup under sync-quorum fails over on its fencing round (the controls).
func TestTheAutomaticPathHoldsABestEffortVolume(t *testing.T) {
	for _, c := range []struct {
		name string
		tick func(t *testing.T, h *Host)
	}{
		{"unsupported gate", func(t *testing.T, h *Host) {
			ineligible := restarting
			ineligible.eligible = false
			unsupportedTick(t, h, ineligible)
		}},
		{"supported gate", func(t *testing.T, h *Host) {
			supportedCreatingTick(t, h, rf3Slot{incarnation: "r1-a", reachable: true, eligible: false})
		}},
	} {
		for _, profile := range []string{"best_effort", ""} {
			name := c.name + "/" + map[string]string{"best_effort": "best_effort", "": "sync-quorum control"}[profile]
			t.Run(name, func(t *testing.T) {
				h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
				defer closeTestMaster(t, h)
				p := probeOf(t, h)
				p.mu.Lock()
				p.profile = profile
				p.mu.Unlock()
				time.Sleep(250 * time.Millisecond) // past the boot grace
				c.tick(t, h)
				p.mu.Lock()
				fences := p.fenceCalls
				p.mu.Unlock()
				if profile == "best_effort" {
					if fences != 0 {
						t.Fatalf("%d fencing rounds for a best_effort volume", fences)
					}
					expectLineStaysR1(t, h, "a best_effort volume's lost primary")
					if !blockedFor(h, "vol-rf3", ops.ReasonBestEffortNoAutomaticFailover) {
						t.Fatal("the volume does not show why it is not failed over")
					}
					return
				}
				expectFailoverToR2(t, h, "a sync-quorum volume's lost primary (control)")
			})
		}
	}
}
