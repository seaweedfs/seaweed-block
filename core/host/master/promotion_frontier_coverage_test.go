package master

import (
	"reflect"
	"testing"
	"time"

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

// RF2 never consults the RF3 promotion gate: a probe that would refuse every
// candidate (unknown frontier, no survivors) does not stop RF2 failover.
func TestMountedFailover_ProductLoopRF2IgnoresRF3FrontierGate(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 2})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{result: PromotionProbeResult{AckProfile: "sync-quorum"}})
	seedRF2Placement(t, h)
	ingestRF2Observation(t, h, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	if initial := waitAuthorityLine(t, h.Publisher(), "vol-rf2"); initial.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", initial)
	}
	ingestRF2Observation(t, h, false, true)
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("failover product tick: %v", err)
	}
	if result.PromotionProbes != 0 {
		t.Fatalf("result=%+v want no RF3 promotion probe for RF2", result)
	}
	if failover := waitAuthorityReplica(t, h.Publisher(), "vol-rf2", "r2"); failover.Epoch != 2 {
		t.Fatalf("failover line=%+v want r2@2", failover)
	}
}
