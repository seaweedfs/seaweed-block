package master

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/storage"
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

// The operator's failover of an RF3 sync_quorum volume whose primary is
// unreachable (its host powered off: no fresh heartbeat) runs the fencing
// round, which is the seal a planned move there lacks, and the
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
			ingestRF3Observation(t, h, false, true, true) // r1's host is gone
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
	ingestRF3Observation(t, h, false, true, true)
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

// A reachable RF3 sync_quorum primary's move is a planned move, and a planned
// move there needs a seal v1 does not have: refused, with no fencing round.
// Only the failover of an unreachable primary is fenced.
func TestManualFailoverRF3ReachablePrimaryIsRefused(t *testing.T) {
	h, line := rf3LineOnR1(t)
	defer closeTestMaster(t, h)
	p := &fenceProbe{
		plain:  PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 10}, {ReplicaID: "r3", Ready: true, DurableLSN: 12}}},
		fenced: fencedAs(),
	}
	h.SetPromotionEvidenceProvider(ackedFenceProbe{p})
	err := failoverTo(h, line, "r3")
	if !errors.Is(err, ErrPlannedMoveNeedsSeal) || len(p.survivors) != 0 {
		t.Fatalf("err=%v fenced=%v; want ErrPlannedMoveNeedsSeal and no fencing round", err, p.survivors)
	}
	if after, _ := h.Publisher().VolumeAuthorityLine("vol-rf3"); after != line {
		t.Fatalf("line=%+v; want %+v unchanged", after, line)
	}
}

// meetingFenceProbe holds each fencing round until two have arrived (or a
// bound passes), so two callers decide the same volume at the same time.
type meetingFenceProbe struct {
	*fenceProbe
	mu      sync.Mutex
	arrived int
	both    chan struct{}
}

func (p *meetingFenceProbe) FencePromotionCandidates(v string, current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	p.mu.Lock()
	p.arrived++
	if p.arrived == 2 {
		close(p.both)
	}
	p.mu.Unlock()
	select {
	case <-p.both:
	case <-time.After(2 * time.Second):
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fenceProbe.FencePromotionCandidates(v, current, survivors)
}

func (p *meetingFenceProbe) AckProfile() string { return p.plain.AckProfile }

// The operator's failover and the master's own promotion loop decide the
// same volume at once: both fence at the same next epoch and both allow the
// target, but exactly one line is published (the line's conditional update
// refuses the second), so the epoch moves once.
func TestManualFailoverAndThePromotionLoopPublishOneLine(t *testing.T) {
	h, line := rf3LineOnR1(t)
	defer closeTestMaster(t, h)
	ingestRF3Observation(t, h, false, true, true)
	e := line.Epoch
	p := &meetingFenceProbe{both: make(chan struct{}), fenceProbe: &fenceProbe{
		// The primary answered not ready: the loop's own promotion applies.
		plain: PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, CurrentKnown: true,
			Current:    PromotionCandidateEvidence{ReplicaID: "r1"},
			Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 10}, {ReplicaID: "r3", Ready: true, DurableLSN: 12}}},
		fenced: fencedAs(
			PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: e + 1, TableValid: true},
			PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, DurableLSN: 12, Floor: e + 1, TableValid: true}),
	}}
	h.SetPromotionEvidenceProvider(p)

	var wg sync.WaitGroup
	var opErr, loopErr error
	wg.Add(2)
	go func() { defer wg.Done(); opErr = failoverTo(h, line, "r3") }()
	go func() { defer wg.Done(); _, loopErr = h.RunLifecycleProductTick() }()
	wg.Wait()
	if loopErr != nil {
		t.Fatalf("product tick: %v", loopErr)
	}
	p.mu.Lock()
	met := p.arrived
	p.mu.Unlock()
	if met < 2 {
		t.Fatalf("%d fencing rounds; want both callers deciding at once", met)
	}
	after := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r3")
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if cur, _ := h.Publisher().VolumeAuthorityLine("vol-rf3"); cur.Epoch != e+1 || cur.ReplicaID != "r3" {
			t.Fatalf("line=%+v; want exactly one publication: r3 at %d (operator err=%v)", cur, e+1, opErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if after.Epoch != e+1 {
		t.Fatalf("line=%+v; want r3 at %d", after, e+1)
	}
}

// healedR1 is the line's own primary answering a heal round ready.
var healedR1 = PromotionProbeResult{CurrentKnown: true, Current: PromotionCandidateEvidence{ReplicaID: "r1", Ready: true, DurableLSN: 11,
	LastDataEpoch: 1, LastDataFirstLSN: 1, StoreID: storage.StoreID{7}, TableValid: true}}

// A fencing round the operator's failover allowed, whose publication then
// failed (here: the evidence is decided and never applied), has fenced the
// survivors above the line all the same. The fence is pending from the
// moment the round is attempted, so when the primary comes back the next
// tick continues it at e+1, without a ready survivor (they refuse it now).
func TestManualFailoverAllowedButUnpublishedFenceIsHealed(t *testing.T) {
	h, line := rf3LineOnR1(t)
	defer closeTestMaster(t, h)
	ingestRF3Observation(t, h, false, true, true)
	e := line.Epoch
	p := &fenceProbe{
		plain: PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 10}, {ReplicaID: "r3", Ready: true, DurableLSN: 12}}},
		fenced: fencedAs(
			PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: e + 1, TableValid: true},
			PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, DurableLSN: 12, Floor: e + 1, TableValid: true}),
	}
	h.SetPromotionEvidenceProvider(ackedFenceProbe{p})
	if _, err := h.plannedMoveEvidence("vol-rf3", "r3"); err != nil {
		t.Fatalf("the fenced failover was not allowed: %v", err)
	}
	// The publication failed: the line is still r1 at e.
	if !h.pendingFence("vol-rf3", e) {
		t.Fatal("an allowed fencing round left no pending fence")
	}
	p.heal = healedR1
	snap := rf3DeadPrimary()
	snap.Volumes[0].Authority.Epoch = e
	s := snap.Volumes[0].Slots
	s[0].Reachable, s[0].ReadyForPrimary = true, true
	s[1].ReadyForPrimary, s[2].ReadyForPrimary = false, false // fenced: they refuse r1 at e
	_, _, asks := h.applyPromotionEvidenceGate(&snap)
	if len(asks) != 1 || asks[0].ReplicaID != "r1" || asks[0].Evidence.Basis != adapter.BasisHeal || asks[0].Evidence.FencedEpoch != e+1 {
		t.Fatalf("asks %+v; want r1 healed at %d", asks, e+1)
	}
}

// A refused operator failover leaves the fence pending; the primary then
// returns ready while another slot is still stale, so the volume is
// unsupported (partial inventory). The unsupported gate heals the line's own
// primary at e+1 before any candidate check, as the supported gate does.
func TestManualFailoverRefusedFenceIsHealedOnTheUnsupportedPath(t *testing.T) {
	h, line := rf3LineOnR1(t)
	defer closeTestMaster(t, h)
	ingestRF3Observation(t, h, false, true, true)
	e := line.Epoch
	p := &fenceProbe{
		plain: PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 10}, {ReplicaID: "r3", Ready: true, DurableLSN: 12}}},
		fenced: fencedAs(
			PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: e + 1, TableValid: true},
			PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, DurableLSN: 12, Floor: e + 1, TableValid: true}),
	}
	h.SetPromotionEvidenceProvider(ackedFenceProbe{p})
	if err := failoverTo(h, line, "r2"); err == nil {
		t.Fatal("a failover to the survivor below the highest fenced end was allowed")
	}
	// r1 is back and ready; r3's slot is stale.
	ingestRF3ObservationForServersWithReady(t, h, "vol-rf3", "node-a", "node-b", "node-c", true, true, true, true, false, false)
	p.plain = PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, CurrentKnown: true, Current: PromotionCandidateEvidence{ReplicaID: "r1", Ready: true},
		Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", DurableLSN: 10}}}
	p.heal = healedR1
	report := authority.SupportabilityReport{Unsupported: map[string]authority.VolumeUnsupportedEvidence{
		"vol-rf3": {VolumeID: "vol-rf3", Reasons: []string{authority.ReasonPartialInventory}},
	}}
	_, _, asks := h.applyUnsupportedPromotionEvidenceGate(h.Lifecycle().Placements.ListPlacements(), report, nil)
	if len(asks) != 1 || asks[0].ReplicaID != "r1" || asks[0].Evidence.Basis != adapter.BasisHeal || asks[0].Evidence.FencedEpoch != e+1 {
		t.Fatalf("asks %+v; want r1 healed at %d on the unsupported path", asks, e+1)
	}
}
