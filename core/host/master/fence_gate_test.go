package master

import (
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// fenceProbe answers the plain round with plain, the fencing round with
// fenced (given the survivors), and a heal round (no candidates) with heal.
type fenceProbe struct {
	plain     PromotionProbeResult
	fenced    func(current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error)
	heal      PromotionProbeResult
	survivors []string
}

func (p *fenceProbe) ProbePromotionCandidates(_ string, _ authority.AuthorityBasis, candidates []authority.ReplicaCandidate) (PromotionProbeResult, error) {
	if candidates == nil {
		return p.heal, nil
	}
	return p.plain, nil
}

func (p *fenceProbe) FencePromotionCandidates(_ string, current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	for _, s := range survivors {
		p.survivors = append(p.survivors, s.ReplicaID)
	}
	return p.fenced(current, survivors)
}

// fencedAs answers the fencing round with these candidates (their floors as
// given).
func fencedAs(cs ...PromotionCandidateEvidence) func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
	return func(current authority.AuthorityBasis, _ []PromotionCandidateEvidence) (PromotionProbeResult, error) {
		return PromotionProbeResult{FencedEpoch: current.Epoch + 1, Candidates: cs}, nil
	}
}

// rf3DeadPrimary is an RF3 snapshot whose primary r1 (line at epoch 1) is
// not acceptable: the gate is to replace it.
func rf3DeadPrimary() authority.ClusterSnapshot {
	slot := func(r, s string, up bool) authority.ReplicaCandidate {
		return authority.ReplicaCandidate{ReplicaID: r, ServerID: s, DataAddr: "d-" + r, CtrlAddr: "c-" + r, Reachable: up, ReadyForPrimary: up, Eligible: true}
	}
	return authority.ClusterSnapshot{
		Servers: []authority.ServerObservation{{ServerID: "s1", Reachable: true, Eligible: true}, {ServerID: "s2", Reachable: true, Eligible: true}, {ServerID: "s3", Reachable: true, Eligible: true}},
		Volumes: []authority.VolumeTopologySnapshot{{
			VolumeID:  "vol-rf3",
			Authority: authority.AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d-r1", CtrlAddr: "c-r1"},
			Slots:     []authority.ReplicaCandidate{slot("r1", "s1", false), slot("r2", "s2", true), slot("r3", "s3", true)},
		}},
	}
}

func allowedSlots(snap authority.ClusterSnapshot) map[string]adapter.PrimaryEvidence {
	out := map[string]adapter.PrimaryEvidence{}
	for _, s := range snap.Volumes[0].Slots[1:] {
		if s.ReadyForPrimary {
			out[s.ReplicaID] = s.PromotionEvidence
		}
	}
	return out
}

// The decision to replace a primary is read from the fencing round: the
// survivors must answer at floor e+1 (N-K+1 of them under sync_quorum RF3),
// the target is the highest FENCED end, and its evidence names the fenced
// epoch. Anything less publishes nothing.
func TestPromotionGate_DecidesOnTheFencingRound(t *testing.T) {
	plain := PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{
		{ReplicaID: "r2", Ready: true, DurableLSN: 10},
		{ReplicaID: "r3", Ready: true, DurableLSN: 9},
	}}
	for _, c := range []struct {
		name   string
		fenced func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error)
		want   map[string]uint64 // allowed replica -> its evidence LSN
	}{
		{"both fenced: the highest fenced end, not the plain one",
			fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}, PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, DurableLSN: 12, Floor: 2, TableValid: true}),
			map[string]uint64{"r3": 12}},
		{"one fenced, one silent: too few",
			fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}),
			map[string]uint64{}},
		{"one answered at a higher floor (superseded): not counted",
			fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}, PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, DurableLSN: 12, Floor: 3, TableValid: true}),
			map[string]uint64{}},
		{"one answered without the fence (another store: its floor did not move)",
			fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}, PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, DurableLSN: 12, Floor: 1, TableValid: true}),
			map[string]uint64{}},
		{"r3 lagging (not ready) with a lower fenced end: counted, r2 is the target",
			fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}, PromotionCandidateEvidence{ReplicaID: "r3", DurableLSN: 8, Floor: 2, TableValid: true}),
			map[string]uint64{"r2": 10}},
		{"r3 lagging (not ready) holds the highest fenced end: wait",
			fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}, PromotionCandidateEvidence{ReplicaID: "r3", DurableLSN: 12, Floor: 2, TableValid: true}),
			map[string]uint64{}},
		{"r3 on an invalid table: not counted",
			fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}, PromotionCandidateEvidence{ReplicaID: "r3", DurableLSN: 8, Floor: 2, TableInvalid: true}),
			map[string]uint64{}},
		{"r3 fenced but its evidence unreadable (table absent): not counted",
			fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}, PromotionCandidateEvidence{ReplicaID: "r3", Floor: 2}),
			map[string]uint64{}},
		{"the fencing round failed",
			func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error) {
				return PromotionProbeResult{}, errors.New("no stream")
			},
			map[string]uint64{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
			defer closeTestMaster(t, h)
			p := &fenceProbe{plain: plain, fenced: c.fenced}
			h.SetPromotionEvidenceProvider(p)
			snap := rf3DeadPrimary()
			h.applyPromotionEvidenceGate(&snap)
			if len(p.survivors) != 2 {
				t.Fatalf("fenced %v; want every replica that answered the plain round: r2, r3", p.survivors)
			}
			got := allowedSlots(snap)
			if len(got) != len(c.want) {
				t.Fatalf("allowed %v; want %v", got, c.want)
			}
			for r, lsn := range c.want {
				ev, ok := got[r]
				if !ok || ev.DurableLSN != lsn || ev.FencedEpoch != 2 || ev.Kind != adapter.PrimaryKindPromotion {
					t.Fatalf("%s evidence %+v; want PROMOTION at %d fenced at 2", r, ev, lsn)
				}
			}
		})
	}
}

// An empty replicated volume whose primary was lost: every fenced end is 0,
// so nothing was acknowledged, and a ready survivor takes the line. With one
// survivor silent the same volume waits (two are needed to prove it).
func TestPromotionGate_EmptyVolumePrimaryLost(t *testing.T) {
	plain := PromotionProbeResult{AckProfile: "sync_quorum", Candidates: []PromotionCandidateEvidence{
		{ReplicaID: "r2", Ready: true}, {ReplicaID: "r3", Ready: true},
	}}
	for name, c := range map[string]struct {
		fenced func(authority.AuthorityBasis, []PromotionCandidateEvidence) (PromotionProbeResult, error)
		want   int
	}{
		"both fenced at end 0": {fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, Floor: 2, TableValid: true}, PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, Floor: 2, TableValid: true}), 2},
		"one fenced":           {fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, Floor: 2, TableValid: true}), 0},
		// Empty means evidence read on a present, valid table at end 0: an
		// answer whose read failed (no table reported) proves nothing.
		"both at end 0, one read failed": {fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, Floor: 2, TableValid: true}, PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, Floor: 2}), 0},
	} {
		t.Run(name, func(t *testing.T) {
			h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
			defer closeTestMaster(t, h)
			h.SetPromotionEvidenceProvider(&fenceProbe{plain: plain, fenced: c.fenced})
			snap := rf3DeadPrimary()
			h.applyPromotionEvidenceGate(&snap)
			got := allowedSlots(snap)
			if len(got) != c.want {
				t.Fatalf("allowed %v; want %d", got, c.want)
			}
			for r, ev := range got {
				if ev.FencedEpoch != 2 || ev.DurableLSN != 0 {
					t.Fatalf("%s evidence %+v; want end 0 fenced at 2", r, ev)
				}
			}
		})
	}
}

// A fencing round that publishes nothing leaves survivors refusing the
// primary at e: the gate continues the line's own primary at e+1 when it
// answers ready (a PROMOTION with its own evidence), else the volume waits
// with a visible reason.
func TestPromotionGate_HealsAFailedFencingRoundOnTheCurrentPrimary(t *testing.T) {
	store := storage.StoreID{7}
	for _, ready := range []bool{true, false} {
		t.Run(map[bool]string{true: "primary answers ready", false: "primary not ready: wait"}[ready], func(t *testing.T) {
			h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
			defer closeTestMaster(t, h)
			p := &fenceProbe{
				plain: PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, CurrentKnown: true, Current: PromotionCandidateEvidence{ReplicaID: "r1"},
					Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 10}, {ReplicaID: "r3", Ready: true, DurableLSN: 10}}},
				fenced: fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}), // r3 silent
				heal: PromotionProbeResult{CurrentKnown: true, Current: PromotionCandidateEvidence{ReplicaID: "r1", Ready: ready, DurableLSN: 11,
					LastDataEpoch: 1, LastDataFirstLSN: 1, StoreID: store, TableValid: true}},
			}
			h.SetPromotionEvidenceProvider(p)
			snap := rf3DeadPrimary()
			snap.Volumes[0].Slots[0].Reachable, snap.Volumes[0].Slots[0].ReadyForPrimary = true, true
			_, _, asks := h.applyPromotionEvidenceGate(&snap)
			if got := allowedSlots(snap); len(got) != 0 {
				t.Fatalf("a survivor was allowed after a failed fencing round: %v", got)
			}
			if !ready {
				if len(asks) != 0 {
					t.Fatalf("asks %+v; want none (the volume waits)", asks)
				}
				return
			}
			if len(asks) != 1 {
				t.Fatalf("asks %+v; want the heal", asks)
			}
			a := asks[0]
			if a.ReplicaID != "r1" || a.Intent != authority.IntentReassign || a.Evidence.Kind != adapter.PrimaryKindPromotion ||
				a.Evidence.DurableLSN != 11 || a.Evidence.StoreID != store || a.Evidence.FencedEpoch != 2 {
				t.Fatalf("heal ask %+v; want r1 continued at 2 on its own evidence", a)
			}
		})
	}
}

// The heal outlives the tick of the failed fencing round: the primary did not
// answer then, and on a later tick (now acceptable, its survivors no longer
// looking ready) it is continued at e+1. Without a remembered round, a
// survivor answering above the line triggers the same heal.
func TestPromotionGate_HealsOnALaterTick(t *testing.T) {
	store := storage.StoreID{7}
	healed := PromotionProbeResult{CurrentKnown: true, Current: PromotionCandidateEvidence{ReplicaID: "r1", Ready: true, DurableLSN: 11, StoreID: store, TableValid: true}}
	t.Run("remembered failed round", func(t *testing.T) {
		h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
		defer closeTestMaster(t, h)
		p := &fenceProbe{
			plain:  PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 10}, {ReplicaID: "r3", Ready: true, DurableLSN: 10}}},
			fenced: fencedAs(PromotionCandidateEvidence{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}),
		}
		h.SetPromotionEvidenceProvider(p)
		snap := rf3DeadPrimary()
		if _, _, asks := h.applyPromotionEvidenceGate(&snap); len(asks) != 0 {
			t.Fatalf("tick 1 asks %+v; want none (primary silent)", asks)
		}
		p.heal = healed
		snap = rf3DeadPrimary()
		s := snap.Volumes[0].Slots
		s[0].Reachable, s[0].ReadyForPrimary = true, true
		s[1].ReadyForPrimary, s[2].ReadyForPrimary = false, false // fenced: they refuse the primary
		_, _, asks := h.applyPromotionEvidenceGate(&snap)
		if len(asks) != 1 || asks[0].ReplicaID != "r1" || asks[0].Evidence.FencedEpoch != 2 {
			t.Fatalf("tick 2 asks %+v; want r1 continued at 2", asks)
		}
	})
	t.Run("a survivor answers above the line", func(t *testing.T) {
		h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
		defer closeTestMaster(t, h)
		h.SetPromotionEvidenceProvider(&fenceProbe{
			plain: PromotionProbeResult{AckProfile: "sync_quorum", SyncAckKnown: true, CurrentKnown: true, Current: PromotionCandidateEvidence{ReplicaID: "r1", Ready: true},
				Candidates: []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 10, Floor: 2, TableValid: true}, {ReplicaID: "r3", Ready: true, DurableLSN: 10, Floor: 1, TableValid: true}}},
			heal: healed,
		})
		snap := rf3DeadPrimary()
		snap.Volumes[0].Slots[0].Reachable, snap.Volumes[0].Slots[0].ReadyForPrimary = true, true
		_, _, asks := h.applyPromotionEvidenceGate(&snap)
		if len(asks) != 1 || asks[0].ReplicaID != "r1" || asks[0].Evidence.FencedEpoch != 2 {
			t.Fatalf("asks %+v; want r1 continued at 2", asks)
		}
	})
}

// The round sends a fencing ask with the fenced epoch and the reference store
// (the last attested), and reads each answer's floor; an answer from another
// store carries no floor.
func TestProbeRound_FencingAskCarriesTheEpochAndStore(t *testing.T) {
	hub := newProbeHub()
	p := &roundEvidenceProvider{hub: hub, serverOf: func(_, r string) string { return "s2" }, wait: 300 * time.Millisecond, attested: attestedAs}
	var asked *control.ProbeRound
	roundNode(t, hub, "s2", func(r *control.ProbeRound) *control.ProbeAnswer {
		asked = r
		a := readyAnswer("s2")(r)
		a.Floor = r.FenceEpoch
		return a
	})
	res, err := p.FencePromotionCandidates("v1", authority.AuthorityBasis{ReplicaID: "r1", Epoch: 4}, []PromotionCandidateEvidence{{ReplicaID: "r2", StoreID: storeOf("r2")}})
	if err != nil {
		t.Fatal(err)
	}
	want := storeOf("r2")
	if asked == nil || asked.FenceEpoch != 5 || string(asked.ExpectedStoreId) != string(want[:]) {
		t.Fatalf("fencing ask %+v; want fence epoch 5 on r2's store", asked)
	}
	if res.FencedEpoch != 5 || len(res.Candidates) != 1 || res.Candidates[0].Floor != 5 || !res.Candidates[0].Ready {
		t.Fatalf("fencing result %+v", res)
	}
}

func TestProbeRound_FencingAnswerFromAnotherStoreIsNotCounted(t *testing.T) {
	hub := newProbeHub()
	p := &roundEvidenceProvider{hub: hub, serverOf: func(_, r string) string { return "s2" }, wait: 300 * time.Millisecond, attested: attestedAs}
	roundNode(t, hub, "s2", func(r *control.ProbeRound) *control.ProbeAnswer {
		a := readyAnswer("s2")(r)
		a.StoreId = []byte("another-store-16")
		a.Floor = r.FenceEpoch
		return a
	})
	res, err := p.FencePromotionCandidates("v1", authority.AuthorityBasis{ReplicaID: "r1", Epoch: 4}, []PromotionCandidateEvidence{{ReplicaID: "r2"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Candidates) != 1 || res.Candidates[0].Floor != 0 {
		t.Fatalf("an answer from another store kept its floor: %+v", res.Candidates)
	}
}
