package master

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
)

// ackedFenceProbe is a fenceProbe that reports its ack profile, as the
// round provider does (plannedMoveEvidence reads it).
type ackedFenceProbe struct{ *fenceProbe }

func (p ackedFenceProbe) AckProfile() string { return p.plain.AckProfile }

// rf3LineOnR1 is a master whose RF3 volume's line is on r1, bound by a
// product tick.
func rf3LineOnR1(t *testing.T) (*Host, authority.AuthorityBasis) {
	t.Helper()
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
	h.SetPromotionEvidenceProvider(staticPromotionProbe{result: PromotionProbeResult{
		AckProfile: "sync_quorum",
		Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 1}, {ReplicaID: "r3", Ready: true, DurableLSN: 1}},
	}})
	seedRF3PlacementForServersWithAddrs(t, h, "vol-rf3", "node-a", "node-b", "node-c")
	ingestRF3Observation(t, h, true, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if line.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", line)
	}
	return h, line
}

func failoverTo(h *Host, current authority.AuthorityBasis, target string) error {
	return failoverVolumeTo(h, "vol-rf3", current, target)
}

func failoverVolumeTo(h *Host, volumeID string, current authority.AuthorityBasis, target string) error {
	addr := map[string][2]string{"r2": {"127.0.0.1:19201", "127.0.0.1:19202"}, "r3": {"127.0.0.1:19301", "127.0.0.1:19302"}}[target]
	_, err := h.FailbackAuthorityRuntime().ExecuteFailback(context.Background(), authority.FailbackRuntimeRequest{
		VolumeID: volumeID, ReplicaID: target, TargetDataAddr: addr[0], TargetCtrlAddr: addr[1],
		ExpectedCurrentReplicaID: current.ReplicaID, ExpectedCurrentEpoch: current.Epoch,
		AckEligible: true, FrontendFencedBeforeFailback: true, DurableFrontierCovered: true, NoCrossVolumeIdentityChange: true,
	})
	return err
}

// The operator's failover of an RF3 sync_quorum volume (its primary
// powered off: no answer) needs no seal: it runs the fencing round, and the
// decision is the round's (Fence model Open/Answer/Decide). Survivors answer
// at floor e+1, both of them (N-K+1 = 2), so the old primary can no longer
// collect an acknowledgement; the target must hold the highest fenced end and
// be ready; the published line carries basis Fenced at e+1. Anything less
// publishes nothing and leaves the fence pending, so a returning primary is
// healed at e+1 (the product loop's heal).
func TestManualFailoverRF3SyncQuorumDecidesOnTheFencingRound(t *testing.T) {
	plain := PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{
		{ReplicaID: "r2", Ready: true, DurableLSN: 10},
		{ReplicaID: "r3", Ready: true, DurableLSN: 9},
	}}
	at := func(r string, lsn uint64, floorAbove uint64, ready bool) PromotionCandidateEvidence {
		return PromotionCandidateEvidence{ReplicaID: r, Ready: ready, DurableLSN: lsn, Floor: floorAbove, TableValid: true}
	}
	for _, c := range []struct {
		name   string
		target string
		fenced func(e uint64) func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error)
		want   uint64 // the promoted line's evidence LSN; 0 = refused
	}{
		{"both fenced; the target holds the highest fenced end", "r3",
			func(e uint64) func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
				return fencedAs(at("r2", 10, e+1, true), at("r3", 12, e+1, true))
			}, 12},
		{"the target is below the other survivor's fenced end", "r2",
			func(e uint64) func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
				return fencedAs(at("r2", 10, e+1, true), at("r3", 12, e+1, true))
			}, 0},
		{"one survivor silent: too few fenced answers", "r2",
			func(e uint64) func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
				return fencedAs(at("r2", 10, e+1, true))
			}, 0},
		{"one answered at a higher floor (superseded): not counted", "r2",
			func(e uint64) func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
				return fencedAs(at("r2", 10, e+1, true), at("r3", 9, e+2, true))
			}, 0},
		{"the target holds the highest end but is not ready", "r3",
			func(e uint64) func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
				return fencedAs(at("r2", 10, e+1, true), at("r3", 12, e+1, false))
			}, 0},
		{"the fencing round failed", "r2",
			func(uint64) func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
				return func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
					return PromotionProbeResult{}, errors.New("no stream")
				}
			}, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, line := rf3LineOnR1(t)
			defer closeTestMaster(t, h)
			p := &fenceProbe{plain: plain, fenced: c.fenced(line.Epoch)}
			h.SetPromotionEvidenceProvider(ackedFenceProbe{p})

			err := failoverTo(h, line, c.target)
			if len(p.survivors) != 2 {
				t.Fatalf("fenced %v; want both survivors r2, r3 fenced before any decision", p.survivors)
			}
			after, _ := h.Publisher().VolumeAuthorityLine("vol-rf3")
			if c.want == 0 {
				if err == nil || after != line {
					t.Fatalf("err=%v line=%+v; want refused, line %+v unchanged", err, after, line)
				}
				if !h.pendingFence("vol-rf3", line.Epoch) {
					t.Fatal("a refused fenced failover left no pending fence: a returning primary would not be healed at e+1")
				}
				return
			}
			if err != nil {
				t.Fatalf("failover to %s: %v", c.target, err)
			}
			if after.ReplicaID != c.target || after.Epoch != line.Epoch+1 {
				t.Fatalf("line=%+v; want %s at %d", after, c.target, line.Epoch+1)
			}
		})
	}
}

// The evidence the fenced failover hands the publisher: kind PROMOTION,
// basis Fenced, the round's epoch (e+1), the target's fenced end.
func TestManualFailoverRF3EvidenceIsTheFencingRounds(t *testing.T) {
	h, line := rf3LineOnR1(t)
	defer closeTestMaster(t, h)
	p := &fenceProbe{
		plain: PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{
			{ReplicaID: "r2", Ready: true, DurableLSN: 10}, {ReplicaID: "r3", Ready: true, DurableLSN: 12}}},
		fenced: fencedAs(
			PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: line.Epoch + 1, TableValid: true},
			PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, DurableLSN: 12, Floor: line.Epoch + 1, TableValid: true}),
	}
	h.SetPromotionEvidenceProvider(ackedFenceProbe{p})
	ev, err := h.plannedMoveEvidence("vol-rf3", "r3")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != adapter.PrimaryKindPromotion || ev.Basis != adapter.BasisFenced || ev.FencedEpoch != line.Epoch+1 || ev.DurableLSN != 12 {
		t.Fatalf("evidence %+v; want PROMOTION, basis fenced at %d, end 12", ev, line.Epoch+1)
	}
}

// rf2LineOnR1 is a master whose RF2 volume's line is on r1, bound by a
// product tick under the given ack profile.
func rf2LineOnR1(t *testing.T, profile string) (*Host, authority.AuthorityBasis) {
	t.Helper()
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 2})
	h.SetPromotionEvidenceProvider(staticPromotionProbe{result: PromotionProbeResult{
		AckProfile: profile,
		Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 1}},
	}})
	seedRF2Placement(t, h)
	ingestRF2Observation(t, h, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf2")
	if line.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", line)
	}
	return h, line
}

// The operator's failover of an RF2 volume whose primary is unreachable (its
// host powered off: no fresh heartbeat) is decided on the fencing round too,
// one path with RF3 (Fence model Open/Answer/Decide; the count is a
// parameter: RF2 needs the one survivor's answer). The survivor must answer
// at floor e+1, on a valid table, ready; the line carries basis Fenced at
// e+1. Anything less publishes nothing and leaves the fence pending.
func TestManualFailoverRF2UnreachablePrimaryDecidesOnTheFencingRound(t *testing.T) {
	for _, profile := range []string{"sync_quorum", "sync_all"} {
		for _, c := range []struct {
			name     string
			survivor func(e uint64) PromotionCandidateEvidence
			fail     bool
			promoted bool
		}{
			{"the survivor fenced at e+1, ready", func(e uint64) PromotionCandidateEvidence {
				return PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 5, Floor: e + 1, TableValid: true}
			}, false, true},
			{"the survivor answered at a higher floor (superseded)", func(e uint64) PromotionCandidateEvidence {
				return PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 5, Floor: e + 2, TableValid: true}
			}, false, false},
			{"the survivor is not ready", func(e uint64) PromotionCandidateEvidence {
				return PromotionCandidateEvidence{ReplicaID: "r2", DurableLSN: 5, Floor: e + 1, TableValid: true}
			}, false, false},
			{"the survivor's table is invalid", func(e uint64) PromotionCandidateEvidence {
				return PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 5, Floor: e + 1, TableInvalid: true}
			}, false, false},
			{"the fencing round failed", nil, true, false},
		} {
			t.Run(profile+"/"+c.name, func(t *testing.T) {
				h, line := rf2LineOnR1(t, profile)
				defer closeTestMaster(t, h)
				ingestRF2Observation(t, h, false, true) // r1's host is gone
				p := &fenceProbe{plain: PromotionProbeResult{AckProfile: profile, SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 5}}}}
				if c.fail {
					p.fenced = func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
						return PromotionProbeResult{}, errors.New("no stream")
					}
				} else {
					p.fenced = fencedAs(c.survivor(line.Epoch))
				}
				h.SetPromotionEvidenceProvider(ackedFenceProbe{p})

				if c.promoted {
					ev, err := h.plannedMoveEvidence("vol-rf2", "r2")
					if err != nil || ev.Basis != adapter.BasisFenced || ev.FencedEpoch != line.Epoch+1 || ev.DurableLSN != 5 {
						t.Fatalf("evidence %+v err=%v; want basis fenced at %d, end 5", ev, err, line.Epoch+1)
					}
				}
				err := failoverVolumeTo(h, "vol-rf2", line, "r2")
				if len(p.survivors) == 0 || p.survivors[len(p.survivors)-1] != "r2" {
					t.Fatalf("fenced %v; want the survivor r2 fenced before any decision", p.survivors)
				}
				after, _ := h.Publisher().VolumeAuthorityLine("vol-rf2")
				if !c.promoted {
					if err == nil || after != line {
						t.Fatalf("err=%v line=%+v; want refused, line %+v unchanged", err, after, line)
					}
					if !h.pendingFence("vol-rf2", line.Epoch) {
						t.Fatal("a refused fenced failover left no pending fence")
					}
					return
				}
				if err != nil || after.ReplicaID != "r2" || after.Epoch != line.Epoch+1 {
					t.Fatalf("err=%v line=%+v; want r2 at %d", err, after, line.Epoch+1)
				}
			})
		}
	}
}

// best_effort acknowledges a write without any replica, so no round can
// prove the survivor holds every acknowledged write: the operator's failover
// of an unreachable best_effort primary is refused (never basis Fenced),
// and no fencing round runs.
func TestManualFailoverRF2BestEffortIsNeverFenced(t *testing.T) {
	h, line := rf2LineOnR1(t, "best_effort")
	defer closeTestMaster(t, h)
	ingestRF2Observation(t, h, false, true)
	p := &fenceProbe{
		plain:  PromotionProbeResult{AckProfile: "best_effort", Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 5}}},
		fenced: fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 5, Floor: line.Epoch + 1, TableValid: true}),
	}
	h.SetPromotionEvidenceProvider(ackedFenceProbe{p})
	if _, err := h.plannedMoveEvidence("vol-rf2", "r2"); !errors.Is(err, ErrPlannedMoveBestEffort) || len(p.survivors) != 0 {
		t.Fatalf("err=%v fenced=%v; want ErrPlannedMoveBestEffort and no fencing round", err, p.survivors)
	}
}

// An RF2 sync_quorum failover from a reachable primary stays a planned move
// (its target is in every acknowledgement set): no fencing round, basis
// Planned.
func TestManualFailoverRF2ReachablePrimaryStaysPlanned(t *testing.T) {
	h, _ := rf2LineOnR1(t, "sync_quorum")
	defer closeTestMaster(t, h)
	p := &fenceProbe{
		plain:  PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 5}}},
		fenced: fencedAs(),
	}
	h.SetPromotionEvidenceProvider(ackedFenceProbe{p})
	ev, err := h.plannedMoveEvidence("vol-rf2", "r2")
	if err != nil {
		t.Fatal(err)
	}
	if ev.Basis != adapter.BasisPlanned || ev.FencedEpoch != 0 || len(p.survivors) != 0 {
		t.Fatalf("evidence %+v fenced=%v; want a planned move with no fencing round", ev, p.survivors)
	}
}
