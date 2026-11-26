package master

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/bootstrap"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// The gate reads every frontier rule from the master's ack profile, so a
// lost primary is replaced automatically only when every answering replica
// runs that profile. On either gate, a round whose answers disagree with the
// master's profile, or do not name one, holds the volume without a fencing
// round and shows why; so does a master profile the gate does not know.
func TestTheAutomaticPathHoldsOnAnAckProfileMismatch(t *testing.T) {
	gates := []struct {
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
	}
	cases := []struct {
		name     string
		profile  string // the master's profile ("" = sync-quorum)
		mismatch string // the round's ProfileMismatch
		event    bool   // the volume shows ack_profile_mismatch
	}{
		{"best_effort volume under a sync-quorum master", "", "master sync_quorum, answers r2=best_effort", true},
		{"sync-quorum volumes under a best-effort master", "best-effort", "master best_effort, answers r1=sync_quorum,r2=sync_quorum,r3=sync_quorum", true},
		{"an answer without a profile", "", "master sync_quorum, answers r3=none", true},
		{"unknown master profile, reported", "bogus", `master ack profile "bogus" unknown`, true},
		{"unknown master profile, not reported", "bogus", "", false},
	}
	for _, g := range gates {
		for _, c := range cases {
			t.Run(g.name+"/"+c.name, func(t *testing.T) {
				h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
				defer closeTestMaster(t, h)
				p := probeOf(t, h)
				p.mu.Lock()
				p.profile, p.mismatch = c.profile, c.mismatch
				p.mu.Unlock()
				time.Sleep(250 * time.Millisecond) // past the boot grace
				g.tick(t, h)
				p.mu.Lock()
				fences := p.fenceCalls
				p.mu.Unlock()
				if fences != 0 {
					t.Fatalf("%d fencing rounds on %s", fences, c.name)
				}
				expectLineStaysR1(t, h, c.name)
				if c.event && !blockedFor(h, "vol-rf3", ops.ReasonAckProfileMismatch) {
					t.Fatalf("the volume does not show %s", ops.ReasonAckProfileMismatch)
				}
			})
		}
	}
}

// The probe round names every answer whose ack profile is missing or is not
// the master's (best-effort and best_effort are one profile), and an unknown
// master profile; it names nothing when all agree.
func TestProbeRound_ReportsAnAckProfileMismatch(t *testing.T) {
	servers := map[string]string{"r2": "s2", "r3": "s3"}
	for _, c := range []struct {
		name           string
		master, r2, r3 string
		want           []string // substrings of ProfileMismatch; nil = empty
	}{
		{"all agree", "sync-quorum", "sync_quorum", "sync_quorum", nil},
		{"all agree, best-effort", "best-effort", "best_effort", "best_effort", nil},
		{"a best_effort replica", "sync-quorum", "best_effort", "sync_quorum", []string{"r2=best_effort"}},
		{"a sync_quorum replica under best-effort", "best-effort", "best_effort", "sync_quorum", []string{"r3=sync_quorum"}},
		{"an answer without a profile", "sync-quorum", "sync_quorum", "", []string{"r3=none"}},
		{"unknown master profile", "bogus", "sync_quorum", "sync_quorum", []string{"unknown"}},
		{"empty master profile", "", "sync_quorum", "sync_quorum", []string{"unknown"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			hub := newProbeHub()
			p := &roundEvidenceProvider{hub: hub, serverOf: func(_, r string) string { return servers[r] }, ackProfile: c.master, wait: 300 * time.Millisecond, attested: attestedAs}
			for server, profile := range map[string]string{"s2": c.r2, "s3": c.r3} {
				server, profile := server, profile
				roundNode(t, hub, server, func(r *control.ProbeRound) *control.ProbeAnswer {
					a := readyAnswer(server)(r)
					a.AckProfile = profile
					return a
				})
			}
			res, err := p.ProbePromotionCandidates("v1", authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: "r2", ServerID: "s2"}, {ReplicaID: "r3", ServerID: "s3"}})
			if err != nil {
				t.Fatal(err)
			}
			if c.want == nil {
				if res.ProfileMismatch != "" {
					t.Fatalf("ProfileMismatch %q; want none", res.ProfileMismatch)
				}
				return
			}
			for _, w := range c.want {
				if !strings.Contains(res.ProfileMismatch, w) {
					t.Fatalf("ProfileMismatch %q; want it to name %q", res.ProfileMismatch, w)
				}
			}
		})
	}
}

// The fencing round's answers are the evidence the decision counts, so their
// profiles are checked too: with the plain round in agreement and a survivor
// answering the fence in another profile, neither gate publishes a primary;
// the line stays and the volume shows ack_profile_mismatch.
func TestTheFencingRoundsAckProfileIsCheckedToo(t *testing.T) {
	for _, g := range []struct {
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
		t.Run(g.name, func(t *testing.T) {
			h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
			defer closeTestMaster(t, h)
			p := probeOf(t, h)
			p.mu.Lock()
			p.fenceMismatch = "master sync_quorum, answers r2=best_effort"
			p.mu.Unlock()
			time.Sleep(250 * time.Millisecond) // past the boot grace
			g.tick(t, h)
			p.mu.Lock()
			fences := p.fenceCalls
			p.mu.Unlock()
			if fences == 0 {
				t.Fatal("no fencing round ran: the plain round agreed, so the fence must be asked")
			}
			expectLineStaysR1(t, h, "a fencing round answered in another profile")
			if !blockedFor(h, "vol-rf3", ops.ReasonAckProfileMismatch) {
				t.Fatalf("the volume does not show %s", ops.ReasonAckProfileMismatch)
			}
		})
	}
}

// Every assignment names the master's ack profile in canonical form, so a
// primary can refuse to serve a replicated line under another one.
func TestAssignmentNamesTheMastersAckProfile(t *testing.T) {
	for flag, want := range map[string]string{"sync-quorum": "sync_quorum", "best-effort": "best_effort", "sync-all": "sync_all"} {
		h := &Host{boot: &bootstrap.DurableAuthorityBootstrap{Publisher: authority.NewPublisher(authority.NewStaticDirective(nil))}, topo: authority.AcceptedTopology{Volumes: []authority.VolumeExpected{{VolumeID: "v", Slots: []authority.ExpectedSlot{{ReplicaID: "r1", ServerID: "s1"}}}}}}
		h.promotionProber = &roundEvidenceProvider{ackProfile: flag}
		fact := (&services{host: h}).assignmentFact(adapter.AssignmentInfo{VolumeID: "v", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1})
		if fact.GetAckProfile() != want {
			t.Fatalf("--launcher-replication-ack=%s: assignment names %q; want %q", flag, fact.GetAckProfile(), want)
		}
	}
}
