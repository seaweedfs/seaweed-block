package authority

import (
	"testing"
	"time"
)

func twoSlotVolume(volumeID string) VolumeExpected {
	return VolumeExpected{
		VolumeID: volumeID,
		Slots: []ExpectedSlot{
			{ReplicaID: "r1", ServerID: "s1"},
			{ReplicaID: "r2", ServerID: "s2"},
		},
	}
}

func TestG8_1_BuildSnapshot_ExistingAuthorityLineAllowsDegradedFailoverInput(t *testing.T) {
	start := time.Date(2026, 5, 2, 15, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{
		FreshnessWindow: 500 * time.Millisecond,
		PendingGrace:    100 * time.Millisecond,
	}, clk.Now)
	topo := AcceptedTopology{Volumes: []VolumeExpected{twoSlotVolume("v1")}}

	_ = ingest(t, store, serverObs("s1", start, healthySlot("v1", "r1")))
	_ = ingest(t, store, serverObs("s2", start, healthySlot("v1", "r2")))

	reader := newFakeReaderForHost()
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "data-r1", CtrlAddr: "ctrl-r1",
	})

	clk.Advance(time.Second)
	_ = ingest(t, store, serverObs("s2", clk.Now(), healthySlot("v1", "r2")))

	result := BuildSnapshot(store.Snapshot(), topo, reader)
	if _, unsupported := result.Report.Unsupported["v1"]; unsupported {
		t.Fatalf("existing authority line must allow degraded failover input, got unsupported=%+v", result.Report.Unsupported["v1"])
	}
	v, ok := findSupported(result, "v1")
	if !ok {
		t.Fatalf("v1 must stay supported as degraded failover input; result=%+v", result.Report)
	}
	if len(v.Slots) != 2 {
		t.Fatalf("supported degraded snapshot must preserve configured slot shape, got %d slots: %+v", len(v.Slots), v.Slots)
	}
	r1, ok := candidateByReplica(v, "r1")
	if !ok {
		t.Fatalf("stale current replica must remain present as unacceptable candidate: %+v", v.Slots)
	}
	if r1.Reachable || r1.ReadyForPrimary || r1.Eligible || r1.DataAddr != "data-r1" || r1.CtrlAddr != "ctrl-r1" {
		t.Fatalf("stale current replica should be an unreachable placeholder, got %+v", r1)
	}
	r2, ok := candidateByReplica(v, "r2")
	if !ok || !r2.Reachable || !r2.ReadyForPrimary || !r2.Eligible || r2.DataAddr == "" || r2.CtrlAddr == "" {
		t.Fatalf("fresh replica must remain acceptable failover candidate, got ok=%v slot=%+v", ok, r2)
	}
	if !v.Authority.Assigned || v.Authority.ReplicaID != "r1" || v.Authority.Epoch != 1 {
		t.Fatalf("snapshot must carry current authority basis for failover stale-resistance, got %+v", v.Authority)
	}
}

func TestG8_1_BuildSnapshot_InitialBindStillRequiresFullCoverage(t *testing.T) {
	start := time.Date(2026, 5, 2, 15, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{
		FreshnessWindow: 500 * time.Millisecond,
		PendingGrace:    100 * time.Millisecond,
	}, clk.Now)
	topo := AcceptedTopology{Volumes: []VolumeExpected{twoSlotVolume("v1")}}

	_ = ingest(t, store, serverObs("s1", start, healthySlot("v1", "r1")))
	_ = ingest(t, store, serverObs("s2", start, healthySlot("v1", "r2")))
	clk.Advance(time.Second)
	_ = ingest(t, store, serverObs("s2", clk.Now(), healthySlot("v1", "r2")))

	result := BuildSnapshot(store.Snapshot(), topo, nil)
	if _, ok := findSupported(result, "v1"); ok {
		t.Fatalf("initial bind must not support a degraded partial snapshot without an authority line")
	}
	if _, unsupported := result.Report.Unsupported["v1"]; !unsupported {
		t.Fatalf("initial degraded partial snapshot must remain unsupported, report=%+v", result.Report)
	}
}
