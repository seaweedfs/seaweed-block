package master

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// #128 rule 6 on the master: an explicit unhealthy slot (Eligible=false
// plus a reason) fails over at once to a qualified peer, promotes nobody
// without one, and surfaces the reason in volume status and as events.
// Heartbeats go through the real ReportHeartbeat wire path.

type wireSlot struct {
	incarnation string
	ready       bool
	unhealthy   string // non-empty: Eligible=false with this reason
}

func reportRF3(t *testing.T, svc *services, r1, r2, r3 wireSlot) {
	t.Helper()
	for i, s := range []struct {
		server, replica string
		slot            wireSlot
	}{
		{"node-a", "r1", r1}, {"node-b", "r2", r2}, {"node-c", "r3", r3},
	} {
		_, err := svc.ReportHeartbeat(context.Background(), &control.HeartbeatReport{
			ServerId: s.server, SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
			Slots: []*control.HeartbeatSlot{{
				VolumeId:        "vol-rf3",
				ReplicaId:       s.replica,
				DataAddr:        "127.0.0.1:19" + string(rune('1'+i)) + "01",
				CtrlAddr:        "127.0.0.1:19" + string(rune('1'+i)) + "02",
				Reachable:       true,
				ReadyForPrimary: s.slot.ready,
				Eligible:        s.slot.unhealthy == "",
				EvidenceScore:   uint64(30 - i),
				Incarnation:     s.slot.incarnation,
				UnhealthyReason: s.slot.unhealthy,
			}},
		})
		if err != nil {
			t.Fatalf("ReportHeartbeat %s: %v", s.server, err)
		}
	}
}

func watchdogMaster(t *testing.T) (*Host, *services) {
	t.Helper()
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3, PrimaryBootGrace: time.Minute,
	})
	seedRF3PlacementForServersWithAddrs(t, h, "vol-rf3", "node-a", "node-b", "node-c")
	svc := newServices(h)
	reportRF3(t, svc, wireSlot{"r1-a", true, ""}, wireSlot{"r2-a", true, ""}, wireSlot{"r3-a", true, ""})
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	if line := waitAuthorityLine(t, h.Publisher(), "vol-rf3"); line.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", line)
	}
	return h, svc
}

func unhealthyReplicas(t *testing.T, svc *services) map[string]string {
	t.Helper()
	resp, err := svc.QueryVolumeStatus(context.Background(), &control.StatusRequest{VolumeId: "vol-rf3"})
	if err != nil {
		t.Fatalf("QueryVolumeStatus: %v", err)
	}
	out := map[string]string{}
	for _, u := range resp.GetUnhealthyReplicas() {
		out[u.GetReplicaId()] = u.GetReason()
	}
	return out
}

func timelineEvents(t *testing.T, svc *services) []*control.ClusterEvent {
	t.Helper()
	tl, err := svc.GetVolumeTimeline(context.Background(), &control.GetVolumeTimelineRequest{VolumeId: "vol-rf3"})
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}
	return tl.GetEvents()
}

// No qualified peer: nobody is promoted; the reason is in volume status
// and the timeline; a later ready report clears both.
func TestRecoveryWatchdog_MasterNoQualifiedPeerPromotesNobody(t *testing.T) {
	h, svc := watchdogMaster(t)
	defer closeTestMaster(t, h)

	reportRF3(t, svc, wireSlot{"r1-b", false, "recovery_timeout"}, wireSlot{"r2-a", false, ""}, wireSlot{"r3-a", false, ""})
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if line, _ := h.Publisher().VolumeAuthorityLine("vol-rf3"); line.ReplicaID != "r1" || line.Epoch != 1 {
		t.Fatalf("no qualified peer: line=%+v want r1@1 unchanged", line)
	}
	if got := unhealthyReplicas(t, svc); got["r1"] != "recovery_timeout" || len(got) != 1 {
		t.Fatalf("volume status unhealthy replicas=%v; want r1=recovery_timeout", got)
	}
	if !timelineHasEvent(timelineEvents(t, svc), "replica_unhealthy", "recovery_timeout") {
		t.Fatalf("timeline has no replica_unhealthy/recovery_timeout event")
	}

	reportRF3(t, svc, wireSlot{"r1-b", true, ""}, wireSlot{"r2-a", false, ""}, wireSlot{"r3-a", false, ""})
	if got := unhealthyReplicas(t, svc); len(got) != 0 {
		t.Fatalf("after a ready report: unhealthy replicas=%v; want none", got)
	}
	cleared := false
	for _, e := range timelineEvents(t, svc) {
		if e.GetEventType() == "replica_unhealthy_cleared" && e.GetReplicaId() == "r1" && e.GetOldValue() == "recovery_timeout" {
			cleared = true
		}
	}
	if !cleared {
		t.Fatalf("timeline has no replica_unhealthy_cleared event for r1")
	}
}

// A qualified peer exists: the unhealthy primary fails over at once, even
// though it is a restarted incarnation that would otherwise be in its
// boot grace.
func TestRecoveryWatchdog_MasterUnhealthyFailsOverToQualifiedPeer(t *testing.T) {
	h, svc := watchdogMaster(t)
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(&switchProbe{}) // r1 not ready, r2 ready with evidence

	reportRF3(t, svc, wireSlot{"r1-b", false, "recovery_timeout"}, wireSlot{"r2-a", true, ""}, wireSlot{"r3-a", false, ""})
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if line := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r2"); line.Epoch != 2 {
		t.Fatalf("unhealthy primary with a qualified peer: line=%+v want r2@2", line)
	}
}

// A running primary that was ready and later reports not ready on the
// same incarnation (no unhealthy reason) follows the normal failover
// rules: no boot grace for that incarnation, so it fails over to the
// qualified peer, and nothing is listed as unhealthy (claude05, 21:24Z).
func TestRecoveryWatchdog_MasterLaterDegradationFollowsNormalRules(t *testing.T) {
	h, svc := watchdogMaster(t)
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(&switchProbe{}) // r1 not ready, r2 ready with evidence

	reportRF3(t, svc, wireSlot{"r1-a", false, ""}, wireSlot{"r2-a", true, ""}, wireSlot{"r3-a", false, ""})
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if line := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r2"); line.Epoch != 2 {
		t.Fatalf("degraded running primary with a qualified peer: line=%+v want r2@2", line)
	}
	if got := unhealthyReplicas(t, svc); len(got) != 0 {
		t.Fatalf("unhealthy replicas=%v; want none (no explicit unhealthy fact)", got)
	}
}

func reportR1(t *testing.T, svc *services, slot wireSlot, sentAt time.Time) {
	t.Helper()
	_, err := svc.ReportHeartbeat(context.Background(), &control.HeartbeatReport{
		ServerId: "node-a", SentAt: timestamppb.New(sentAt), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{
			VolumeId: "vol-rf3", ReplicaId: "r1", DataAddr: "127.0.0.1:19101", CtrlAddr: "127.0.0.1:19102",
			Reachable: true, ReadyForPrimary: slot.ready, Eligible: slot.unhealthy == "",
			EvidenceScore: 30, Incarnation: slot.incarnation, UnhealthyReason: slot.unhealthy,
		}},
	})
	if err != nil {
		t.Errorf("ReportHeartbeat r1: %v", err)
	}
}

func r1HealthEvents(t *testing.T, svc *services) []string {
	t.Helper()
	var out []string
	for _, e := range timelineEvents(t, svc) {
		if e.GetReplicaId() == "r1" && (e.GetEventType() == "replica_unhealthy" || e.GetEventType() == "replica_unhealthy_cleared") {
			out = append(out, e.GetEventType())
		}
	}
	return out
}

// onFirstMutation runs fn once, right after the next accepted store
// mutation (inside that ingest, after the store lock is released).
func onFirstMutation(h *Host, fn func()) {
	var once sync.Once
	h.ObservationHost().Store().SetOnMutation(func() { once.Do(fn) })
}

// Review of #134 (P2 3): a second heartbeat lands between the first
// one's ingest and its event bookkeeping. Both transitions must be
// recorded, in accepted order: unhealthy, then cleared.
func TestRecoveryWatchdog_MasterConcurrentHeartbeatsKeepBothTransitions(t *testing.T) {
	h, svc := watchdogMaster(t)
	defer closeTestMaster(t, h)

	t0 := time.Now()
	done := make(chan struct{})
	onFirstMutation(h, func() {
		go func() {
			reportR1(t, svc, wireSlot{"r1-b", true, ""}, t0.Add(time.Millisecond)) // healthy, newer
			close(done)
		}()
		select { // let it run now if nothing blocks it
		case <-done:
		case <-time.After(300 * time.Millisecond):
		}
	})
	reportR1(t, svc, wireSlot{"r1-b", false, "recovery_timeout"}, t0) // unhealthy
	<-done
	got := r1HealthEvents(t, svc)
	if len(got) != 2 || got[0] != "replica_unhealthy" || got[1] != "replica_unhealthy_cleared" {
		t.Fatalf("r1 health events=%v; want [replica_unhealthy replica_unhealthy_cleared]", got)
	}
}

// Review of #134 (P2 3): the stored unhealthy fact expires during an
// ingest that reports unhealthy again. No clear may be fabricated.
func TestRecoveryWatchdog_MasterExpiryFabricatesNoClear(t *testing.T) {
	h, svc := watchdogMaster(t)
	defer closeTestMaster(t, h)
	var offset atomic.Int64
	h.ObservationHost().SetNowForTest(func() time.Time { return time.Now().Add(time.Duration(offset.Load())) })

	t0 := time.Now()
	reportR1(t, svc, wireSlot{"r1-b", false, "recovery_timeout"}, t0)
	onFirstMutation(h, func() { offset.Store(int64(time.Hour)) }) // everything expires mid-ingest
	reportR1(t, svc, wireSlot{"r1-b", false, "recovery_timeout"}, t0.Add(time.Millisecond))
	for _, ev := range r1HealthEvents(t, svc) {
		if ev == "replica_unhealthy_cleared" {
			t.Fatalf("r1 health events=%v: a clear was fabricated by expiry", r1HealthEvents(t, svc))
		}
	}
}

// Control: an older heartbeat the store refuses records nothing.
func TestRecoveryWatchdog_MasterRejectedHeartbeatRecordsNothing(t *testing.T) {
	h, svc := watchdogMaster(t)
	defer closeTestMaster(t, h)
	t0 := time.Now()
	reportR1(t, svc, wireSlot{"r1-b", false, "recovery_timeout"}, t0)
	reportR1(t, svc, wireSlot{"r1-b", true, ""}, t0.Add(-time.Second)) // older: refused
	if got := r1HealthEvents(t, svc); len(got) != 1 || got[0] != "replica_unhealthy" {
		t.Fatalf("r1 health events=%v; want only [replica_unhealthy]", got)
	}
}

// Review of #134 r2: a heartbeat carrying the same slot twice is
// refused at the RPC; volume status and the timeline stay unchanged.
func TestRecoveryWatchdog_MasterDuplicateSlotHeartbeatRefused(t *testing.T) {
	h, svc := watchdogMaster(t)
	defer closeTestMaster(t, h)
	t0 := time.Now()
	reportR1(t, svc, wireSlot{"r1-b", false, "recovery_timeout"}, t0)
	before := r1HealthEvents(t, svc)

	slot := func(reason string) *control.HeartbeatSlot {
		return &control.HeartbeatSlot{
			VolumeId: "vol-rf3", ReplicaId: "r1", DataAddr: "127.0.0.1:19101", CtrlAddr: "127.0.0.1:19102",
			Reachable: true, Eligible: reason == "", Incarnation: "r1-b", UnhealthyReason: reason,
		}
	}
	for i, slots := range [][]*control.HeartbeatSlot{
		{slot(""), slot("recovery_timeout")},                 // conflicting
		{slot("recovery_timeout"), slot("recovery_timeout")}, // identical
	} {
		_, err := svc.ReportHeartbeat(context.Background(), &control.HeartbeatReport{
			ServerId: "node-a", SentAt: timestamppb.New(t0.Add(time.Duration(i+1) * time.Millisecond)),
			Reachable: true, Eligible: true, Slots: slots,
		})
		if err == nil {
			t.Fatalf("duplicate-slot heartbeat %d accepted", i)
		}
	}
	if got := r1HealthEvents(t, svc); len(got) != len(before) {
		t.Fatalf("health events changed by refused heartbeats: %v -> %v", before, got)
	}
	if got := unhealthyReplicas(t, svc); got["r1"] != "recovery_timeout" {
		t.Fatalf("volume status after refusal: %v; want r1=recovery_timeout", got)
	}
}
