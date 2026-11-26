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
	addr := map[string][2]string{"r2": {"127.0.0.1:19201", "127.0.0.1:19202"}, "r3": {"127.0.0.1:19301", "127.0.0.1:19302"}}[target]
	_, err := h.FailbackAuthorityRuntime().ExecuteFailback(context.Background(), authority.FailbackRuntimeRequest{
		VolumeID: "vol-rf3", ReplicaID: target, TargetDataAddr: addr[0], TargetCtrlAddr: addr[1],
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

// An RF2 sync_quorum failover stays a planned move (its target is in every
// acknowledgement set): no fencing round, basis Planned.
func TestManualFailoverRF2SyncQuorumStaysPlanned(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 2})
	defer closeTestMaster(t, h)
	seedRF2Placement(t, h)
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
