package master

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
	return rf3LineOnR1With(t, newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3}))
}

// rf3MasterShortGrace is a master whose observation store reports a partial
// inventory at once (no startup pending grace), so a fresh server that omits
// a slot makes the volume unsupported.
func rf3MasterShortGrace(t *testing.T) *Host {
	t.Helper()
	h, err := New(Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0",
		ControllerConfig: authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3},
		Freshness:        authority.FreshnessConfig{PendingGrace: time.Millisecond}})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	return h
}

func rf3LineOnR1With(t *testing.T, h *Host) (*Host, authority.AuthorityBasis) {
	t.Helper()
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

// meetingFenceProbe holds each fencing round until two have arrived, so two
// callers decide the same volume at the same time. A caller that waits past
// the bound gets an error (no decision) and marks the probe late: the test
// then fails, it never proceeds with one caller alone.
type meetingFenceProbe struct {
	*fenceProbe
	mu      sync.Mutex
	arrived int
	late    bool
	bound   time.Duration
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
	case <-time.After(p.bound):
		p.mu.Lock()
		p.late = true
		p.mu.Unlock()
		return PromotionProbeResult{}, errors.New("meetingFenceProbe: the other caller did not arrive")
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
	p := &meetingFenceProbe{both: make(chan struct{}), bound: 5 * time.Second, fenceProbe: &fenceProbe{
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
	met, late := p.arrived, p.late
	p.mu.Unlock()
	if met != 2 || late {
		t.Fatalf("%d fencing rounds, late=%t; want both callers held together before either decided", met, late)
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

// The barrier's negative control: with one caller only, the round is refused
// at the bound and the probe is marked late, so a test that relies on two
// callers held together cannot pass with one.
func TestMeetingFenceProbeFailsWhenTheOtherCallerIsLate(t *testing.T) {
	p := &meetingFenceProbe{both: make(chan struct{}), bound: 50 * time.Millisecond, fenceProbe: &fenceProbe{fenced: fencedAs()}}
	if _, err := p.FencePromotionCandidates("v", authority.AuthorityBasis{Epoch: 1}, nil); err == nil || !p.late {
		t.Fatalf("err=%v late=%t; want a refusal and late", err, p.late)
	}
}

// storeFault makes every authority record write of one volume fail (a
// directory where the store writes the record's temp file; open fails
// before any rename, as a full or failing disk would) until cleared.
func storeFault(t *testing.T, h *Host, volumeID string) (arm, clear func()) {
	t.Helper()
	var record string
	matches, _ := filepath.Glob(filepath.Join(h.cfg.AuthorityStoreDir, volumeID+".*"))
	for _, m := range matches {
		if !strings.HasSuffix(m, ".tmp") {
			record = m
		}
	}
	if record == "" {
		t.Fatalf("no authority record for %s in %s (%v)", volumeID, h.cfg.AuthorityStoreDir, matches)
	}
	tmp := record + ".tmp"
	arm = func() {
		if err := os.Mkdir(tmp, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	clear = func() { _ = os.Remove(tmp) }
	t.Cleanup(clear)
	return arm, clear
}

// waitLine polls until the volume's line is replica at epoch, else fails.
func waitLine(t *testing.T, h *Host, volumeID, replica string, epoch uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		cur, _ := h.Publisher().VolumeAuthorityLine(volumeID)
		if cur.ReplicaID == replica && cur.Epoch == epoch {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("line=%+v; want %s at %d", cur, replica, epoch)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// healedR1 is the line's own primary answering a heal round ready.
var healedR1 = PromotionProbeResult{CurrentKnown: true, Current: PromotionCandidateEvidence{ReplicaID: "r1", Ready: true, DurableLSN: 11,
	LastDataEpoch: 1, LastDataFirstLSN: 1, StoreID: storage.StoreID{7}, TableValid: true}}

// back puts r1 back ready and the fenced survivors not ready for primary
// (they refuse r1 at e); with r3Missing its server reports no slot for the
// volume (a partial inventory: the volume is unsupported).
func back(t *testing.T, h *Host, r3Missing bool) {
	t.Helper()
	now := time.Now().UTC()
	slot := func(r, d, c string, ready bool) []authority.SlotFact {
		return []authority.SlotFact{{VolumeID: "vol-rf3", ReplicaID: r, DataAddr: d, CtrlAddr: c, Reachable: true, ReadyForPrimary: ready, Eligible: true, EvidenceScore: 10}}
	}
	r3 := slot("r3", "127.0.0.1:19301", "127.0.0.1:19302", false)
	if r3Missing {
		r3 = nil
	}
	for _, obs := range []authority.Observation{
		{ServerID: "node-a", ObservedAt: now, Server: authority.ServerFact{Reachable: true, Eligible: true}, Slots: slot("r1", "127.0.0.1:19101", "127.0.0.1:19102", true)},
		{ServerID: "node-b", ObservedAt: now, Server: authority.ServerFact{Reachable: true, Eligible: true}, Slots: slot("r2", "127.0.0.1:19201", "127.0.0.1:19202", false)},
		{ServerID: "node-c", ObservedAt: now, Server: authority.ServerFact{Reachable: true, Eligible: true}, Slots: r3},
	} {
		if err := h.ObservationHost().Ingest(obs); err != nil {
			t.Fatal(err)
		}
	}
}

// End to end: the operator's failover is allowed by its fencing round, and
// the publication fails (the authority store refuses the write). The line is
// still r1 at e, but its survivors are fenced at e+1. When r1 is back, a
// normal product tick publishes r1 at e+1 (the heal, through the same
// dispatch as every tick), and a further tick adds no epoch.
func TestManualFailoverPublicationFailureIsHealedByTheNextTick(t *testing.T) {
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
	arm, clear := storeFault(t, h, "vol-rf3")
	arm()
	if err := failoverTo(h, line, "r3"); err == nil {
		t.Fatal("the failover published through a failing authority store")
	}
	clear()
	if cur, _ := h.Publisher().VolumeAuthorityLine("vol-rf3"); cur != line || len(p.survivors) != 2 {
		t.Fatalf("after the fault line=%+v fenced=%v; want %+v unchanged and both survivors fenced", cur, p.survivors, line)
	}

	back(t, h, false)
	p.heal = healedR1
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	waitLine(t, h, "vol-rf3", "r1", e+1)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	waitLine(t, h, "vol-rf3", "r1", e+1)
}

// End to end on the unsupported path: the operator's failover is refused (the
// target is below the highest fenced end), so its survivors are fenced at e+1
// and nothing is published. r1 is back while r3's server reports no slot (a
// partial inventory: unsupported). A normal product tick publishes r1 at
// e+1, and a further tick adds no epoch.
func TestManualFailoverRefusedFenceIsHealedByTheNextTickOnTheUnsupportedPath(t *testing.T) {
	h, line := rf3LineOnR1With(t, rf3MasterShortGrace(t))
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

	back(t, h, true)
	time.Sleep(5 * time.Millisecond) // past the pending grace
	topo, _ := acceptedTopologyFromPlacementIntents(h.Lifecycle().Placements.ListPlacements())
	if b := authority.BuildSnapshot(h.obs.Store().Snapshot(), topo, h.Publisher()); len(b.Report.Unsupported["vol-rf3"].Reasons) == 0 {
		t.Fatalf("vol-rf3 is not unsupported (report %+v): the test would not reach the unsupported gate", b.Report.Unsupported)
	}
	p.plain = PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, CurrentKnown: true, Current: PromotionCandidateEvidence{ReplicaID: "r1", Ready: true},
		Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", DurableLSN: 10}}}
	p.heal = healedR1
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	waitLine(t, h, "vol-rf3", "r1", e+1)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	waitLine(t, h, "vol-rf3", "r1", e+1)
}
