package master

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
)

// #181: an RF3 promotion must prove it covers the required acknowledgement
// frontier, either from a known frontier or, when the frontier is unknown,
// from enough fresh ready survivors to intersect every ack quorum.

func TestUnknownFrontierSurvivorsNeeded(t *testing.T) {
	for _, tc := range []struct {
		profile   string
		peerCount int
		want      int
	}{
		{"sync-quorum", 2, 2}, // RF3: K=1, every acked write is on >=1 of 2 peers
		{"sync_quorum", 2, 2},
		{"sync-all", 2, 1},    // RF3: K=2, every acked write is on both peers
		{"sync-quorum", 4, 3}, // RF5: K=2
		{"sync-all", 4, 1},
	} {
		if got := unknownFrontierSurvivorsNeeded(tc.profile, tc.peerCount); got != tc.want {
			t.Errorf("unknownFrontierSurvivorsNeeded(%q, %d)=%d want %d", tc.profile, tc.peerCount, got, tc.want)
		}
	}
}

func TestPromotionAllowedReplicas_RequiredFrontierCoverage(t *testing.T) {
	ready := func(id string, lsn uint64) PromotionCandidateEvidence {
		return PromotionCandidateEvidence{ReplicaID: id, Ready: true, DurableLSN: lsn}
	}
	missing := func(id string) PromotionCandidateEvidence {
		return PromotionCandidateEvidence{ReplicaID: id, ProbeAddr: "missing"}
	}
	bothFresh := map[string]bool{"r2": true, "r3": true}
	for _, tc := range []struct {
		name   string
		result PromotionProbeResult
		ok     bool
		fresh  map[string]bool
		want   map[string]bool
	}{
		{
			name:   "known frontier covered is allowed with one survivor",
			result: PromotionProbeResult{AckProfile: "sync-quorum", SyncAckLSN: 52, Candidates: []PromotionCandidateEvidence{ready("r2", 60), missing("r3")}},
			ok:     true, fresh: bothFresh, want: map[string]bool{"r2": true},
		},
		{
			name:   "known frontier not covered is refused",
			result: PromotionProbeResult{AckProfile: "sync-quorum", SyncAckLSN: 52, Candidates: []PromotionCandidateEvidence{ready("r2", 51), missing("r3")}},
			ok:     true, fresh: bothFresh, want: map[string]bool{},
		},
		{
			name:   "known zero frontier is known, not unknown",
			result: PromotionProbeResult{AckProfile: "sync-quorum", SyncAckKnown: true, Candidates: []PromotionCandidateEvidence{ready("r2", 10), missing("r3")}},
			ok:     true, fresh: bothFresh, want: map[string]bool{"r2": true},
		},
		{
			name:   "unknown frontier with every survivor fresh and ready picks the highest",
			result: PromotionProbeResult{AckProfile: "sync-quorum", Candidates: []PromotionCandidateEvidence{ready("r2", 70), ready("r3", 90)}},
			ok:     true, fresh: bothFresh, want: map[string]bool{"r3": true},
		},
		{
			name:   "unknown frontier with one survivor unseen is refused",
			result: PromotionProbeResult{AckProfile: "sync-quorum", Candidates: []PromotionCandidateEvidence{ready("r2", 51), missing("r3")}},
			ok:     true, fresh: bothFresh, want: map[string]bool{},
		},
		{
			name:   "unknown frontier with one survivor not ready is refused",
			result: PromotionProbeResult{AckProfile: "sync-quorum", Candidates: []PromotionCandidateEvidence{ready("r2", 51), {ReplicaID: "r3", DurableLSN: 90}}},
			ok:     true, fresh: bothFresh, want: map[string]bool{},
		},
		{
			name:   "a ready probe without a fresh heartbeat does not count",
			result: PromotionProbeResult{AckProfile: "sync-quorum", Candidates: []PromotionCandidateEvidence{ready("r2", 70), ready("r3", 90)}},
			ok:     true, fresh: map[string]bool{"r2": true}, want: map[string]bool{},
		},
		{
			name:   "a survivor reported twice counts once",
			result: PromotionProbeResult{AckProfile: "sync-quorum", Candidates: []PromotionCandidateEvidence{ready("r2", 70), ready("r2", 70)}},
			ok:     true, fresh: bothFresh, want: map[string]bool{},
		},
		{
			name:   "sync_all unknown frontier needs one fresh ready survivor",
			result: PromotionProbeResult{AckProfile: "sync-all", Candidates: []PromotionCandidateEvidence{ready("r2", 51), missing("r3")}},
			ok:     true, fresh: bothFresh, want: map[string]bool{"r2": true},
		},
		{
			name:   "best_effort unknown frontier is unchanged",
			result: PromotionProbeResult{AckProfile: "best-effort", Candidates: []PromotionCandidateEvidence{ready("r2", 51), missing("r3")}},
			ok:     true, fresh: map[string]bool{}, want: map[string]bool{"r2": true},
		},
		{
			name:   "best_effort with a required frontier is unchanged (refused)",
			result: PromotionProbeResult{AckProfile: "best-effort", SyncAckLSN: 52, Candidates: []PromotionCandidateEvidence{ready("r2", 60), ready("r3", 60)}},
			ok:     true, fresh: bothFresh, want: map[string]bool{},
		},
		{
			name:   "probe unavailable is refused",
			result: PromotionProbeResult{AckProfile: "sync-quorum", SyncAckLSN: 52, Candidates: []PromotionCandidateEvidence{ready("r2", 60), ready("r3", 60)}},
			ok:     false, fresh: bothFresh, want: map[string]bool{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := promotionAllowedReplicas(tc.result, tc.ok, 2, tc.fresh)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("allowed=%v want %v", got, tc.want)
			}
		})
	}
}

// A volume absent from RequiredLSNByVolume has an unknown frontier; one
// present with zero has a known zero frontier.
func TestWorkloadPlanProvider_RequiredFrontierKnownness(t *testing.T) {
	for _, tc := range []struct {
		name     string
		required map[string]uint64
		want     bool
	}{
		{"absent map", nil, false},
		{"other volume only", map[string]uint64{"vol-other": 9}, false},
		{"present zero", map[string]uint64{"vol-rf3": 0}, true},
		{"present nonzero", map[string]uint64{"vol-rf3": 52}, true},
	} {
		provider := NewWorkloadPlanPromotionEvidenceProvider(nil, WorkloadPlanPromotionProbeConfig{AckProfile: "sync-quorum", RequiredLSNByVolume: tc.required})
		result, err := provider.ProbePromotionCandidates("vol-rf3", authority.AuthorityBasis{}, nil)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if result.SyncAckKnown != tc.want || result.SyncAckLSN != tc.required["vol-rf3"] {
			t.Fatalf("%s: known=%t lsn=%d want known=%t lsn=%d", tc.name, result.SyncAckKnown, result.SyncAckLSN, tc.want, tc.required["vol-rf3"])
		}
	}
}

// Only peers with a fresh heartbeat slot count toward the survivor rule.
func TestFreshPromotionPeers_UsesObservationFreshness(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
	defer closeTestMaster(t, h)
	for _, s := range []struct{ server, replica string }{{"node-a", "r1"}, {"node-b", "r2"}} {
		if err := h.ObservationHost().Ingest(authority.Observation{
			ServerID:   s.server,
			ObservedAt: time.Now().UTC(),
			Server:     authority.ServerFact{Reachable: true, Eligible: true},
			Slots: []authority.SlotFact{{
				VolumeID: "vol-rf3", ReplicaID: s.replica,
				DataAddr: "127.0.0.1:1", CtrlAddr: "127.0.0.1:2",
				Reachable: true, ReadyForPrimary: true, Eligible: true,
			}},
		}); err != nil {
			t.Fatalf("ingest %s: %v", s.server, err)
		}
	}
	got := h.freshPromotionPeers("vol-rf3", []authority.ReplicaCandidate{{ReplicaID: "r2"}, {ReplicaID: "r3"}})
	if !reflect.DeepEqual(got, map[string]bool{"r2": true}) {
		t.Fatalf("fresh peers=%v want only r2 (r3 never observed)", got)
	}
}

type swapPromotionProbe struct {
	mu     sync.Mutex
	result PromotionProbeResult
}

func (p *swapPromotionProbe) set(r PromotionProbeResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.result = r
}

func (p *swapPromotionProbe) ProbePromotionCandidates(string, authority.AuthorityBasis, []authority.ReplicaCandidate) (PromotionProbeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.result, nil
}

// Survivors count only from this tick's probe: r3 was probed ready on the
// previous tick, but this tick its probe is missing, so with an unknown
// frontier r2 alone cannot be promoted.
func TestProductLoop_UnknownFrontierSurvivorReadyLastTickMissingNowRefused(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3})
	defer closeTestMaster(t, h)
	probe := &swapPromotionProbe{}
	h.SetPromotionEvidenceProvider(probe)
	seedRF3Placement(t, h)
	ingestRF3Observation(t, h, true, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	if initial := waitAuthorityLine(t, h.Publisher(), "vol-rf3"); initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}
	establishVolume(t, h, "vol-rf3")

	// Previous tick: both survivors probed ready, current primary ready.
	probe.set(PromotionProbeResult{
		AckProfile:   "sync-quorum",
		CurrentKnown: true,
		Current:      PromotionCandidateEvidence{ReplicaID: "r1", Ready: true, DurableLSN: 90},
		Candidates: []PromotionCandidateEvidence{
			{ReplicaID: "r2", Ready: true, DurableLSN: 90},
			{ReplicaID: "r3", Ready: true, DurableLSN: 90},
		},
	})
	ingestRF3Observation(t, h, true, true, true)
	prev, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatal(err)
	}
	if prev.PromotionProbes != 1 || prev.PromotionBlocked != 0 {
		t.Fatalf("previous tick=%+v want one probe that admits both survivors", prev)
	}

	// This tick: the primary is lost and r3's probe is missing.
	probe.set(PromotionProbeResult{
		AckProfile: "sync-quorum",
		Candidates: []PromotionCandidateEvidence{
			{ReplicaID: "r2", Ready: true, DurableLSN: 90},
			{ReplicaID: "r3", ProbeAddr: "missing"},
		},
	})
	ingestRF3Observation(t, h, false, true, true)
	now, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatal(err)
	}
	if now.PromotionProbes != 1 || now.PromotionBlocked != 1 {
		t.Fatalf("this tick=%+v want the promotion blocked: r3 is not seen this tick", now)
	}
	time.Sleep(200 * time.Millisecond)
	if line, _ := h.Publisher().VolumeAuthorityLine("vol-rf3"); line.ReplicaID != "r1" {
		t.Fatalf("authority moved to %+v on a partial view", line)
	}
}

// RF2 never consults the RF3 promotion gate: a probe that would refuse every
// candidate (unknown frontier, no survivors) does not stop RF2 failover.
// RF2 decides a promotion on the promotion gate too (one replica besides the
// primary): with the candidate's answer ready the failover goes to it with
// the gate's evidence on the line; without a ready answer nothing is
// promoted, whatever the heartbeats say.
func TestMountedFailover_ProductLoopRF2DecidesOnThePromotionGate(t *testing.T) {
	for _, ready := range []bool{true, false} {
		t.Run(map[bool]string{true: "candidate ready", false: "no ready answer"}[ready], func(t *testing.T) {
			h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 2})
			defer closeTestMaster(t, h)
			h.SetPromotionEvidenceProvider(staticPromotionProbe{result: PromotionProbeResult{
				AckProfile:   "sync-quorum",
				CurrentKnown: true,
				Current:      PromotionCandidateEvidence{ReplicaID: "r1"},
				Candidates:   []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: ready, DurableLSN: 1}},
			}})
			seedRF2Placement(t, h)
			ingestRF2Observation(t, h, true, true)
			if _, err := h.RunLifecycleProductTick(); err != nil {
				t.Fatalf("initial product tick: %v", err)
			}
			if initial := waitAuthorityLine(t, h.Publisher(), "vol-rf2"); initial.ReplicaID != "r1" {
				t.Fatalf("initial line=%+v want r1", initial)
			}
			establishVolume(t, h, "vol-rf2")
			ingestRF2Observation(t, h, false, true)
			result, err := h.RunLifecycleProductTick()
			if err != nil {
				t.Fatalf("failover product tick: %v", err)
			}
			if result.PromotionProbes == 0 {
				t.Fatalf("result=%+v: RF2 was not decided on the promotion gate", result)
			}
			if !ready {
				time.Sleep(200 * time.Millisecond)
				if line, _ := h.Publisher().VolumeAuthorityLine("vol-rf2"); line.ReplicaID != "r1" || line.Epoch != 1 {
					t.Fatalf("promoted without a ready answer: line %+v", line)
				}
				return
			}
			failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf2", "r2")
			establishVolume(t, h, "vol-rf2")
			if failover.Epoch != 2 {
				t.Fatalf("failover line=%+v want r2@2", failover)
			}
			info, _ := h.Publisher().LastPublished("vol-rf2", "r2")
			if info.Primary.Kind != adapter.PrimaryKindPromotion || info.Primary.DurableLSN != 1 {
				t.Fatalf("failover line evidence %+v; want the gate's PROMOTION at durable LSN 1", info.Primary)
			}
		})
	}
}
