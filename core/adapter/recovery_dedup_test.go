package adapter

import (
	"strconv"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

func dedupAdapter(t *testing.T) (*VolumeReplicaAdapter, *mockExecutor, ProbeResult) {
	t.Helper()
	exec := newMockExecutor()
	exec.autoClose = false
	// The automatic probe is irrelevant to this controlled delivery schedule.
	// Its wrong-replica result is rejected regardless of when it returns.
	exec.probeResults["r1"] = ProbeResult{ReplicaID: "other"}
	a := NewVolumeReplicaAdapter(exec)
	a.OnAssignment(AssignmentInfo{VolumeID: "vol1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1})
	probe := ProbeResult{ReplicaID: "r1", Success: true, EndpointVersion: 1, TransportEpoch: 1,
		ReplicaFlushedLSN: 50, PrimaryTailLSN: 10, PrimaryHeadLSN: 100}
	a.OnProbeResult(probe)
	t.Cleanup(func() {
		exec.mu.Lock()
		var sid, target uint64
		for _, start := range exec.starts {
			if start.sessionID > sid {
				sid, target = start.sessionID, start.targetLSN
			}
		}
		exec.mu.Unlock()
		a.OnSessionClose(SessionCloseResult{ReplicaID: "r1", SessionID: sid, Success: true, AchievedLSN: target})
	})
	return a, exec, probe
}

func TestAdapter_OneRecoveryPreparedWithinProbeBatch(t *testing.T) {
	a, exec, probe := dedupAdapter(t)
	sid := exec.latestSessionID("StartCatchUp")
	// A failed start permits a new probe to retry. Both reachability and facts
	// in that one probe must see the first newly prepared recovery session.
	a.OnSessionClose(SessionCloseResult{ReplicaID: "r1", SessionID: sid, FailureKind: engine.RecoveryFailureStartTimeout})
	a.OnProbeResult(probe)
	if got := countStarts(exec, "StartCatchUp"); got != 2 {
		t.Fatalf("one probe batch emitted duplicate recovery: starts=%d want initial+one retry", got)
	}
}

func TestAdapter_ProbeUsesFreshBoundsBeforePreparingRecovery(t *testing.T) {
	a, exec, probe := dedupAdapter(t)
	a.OnSessionClose(SessionCloseResult{ReplicaID: "r1", SessionID: exec.latestSessionID("StartCatchUp"), FailureKind: engine.RecoveryFailureStartTimeout})
	probe.PrimaryTailLSN = 60 // the former catch-up prefix is now recycled
	a.OnProbeResult(probe)
	if catchup, rebuild := countStarts(exec, "StartCatchUp"), countStarts(exec, "StartRebuild"); catchup != 1 || rebuild != 1 {
		t.Fatalf("probe reserved recovery from stale bounds: catchup=%d rebuild=%d", catchup, rebuild)
	}
}

func TestAdapter_OneRecoveryFromPostCloseStalledAck(t *testing.T) {
	for _, kind := range []string{"StartCatchUp", "StartRebuild"} {
		t.Run(kind, func(t *testing.T) {
			a, exec, _ := dedupAdapter(t)
			a.OnSessionClose(SessionCloseResult{ReplicaID: "r1", SessionID: exec.latestSessionID("StartCatchUp"), Success: true, AchievedLSN: 100})
			initial, tail := 1, uint64(10)
			if kind == "StartRebuild" {
				initial, tail = 0, 110
			}
			for i := 0; i < 8; i++ {
				a.OnDurableAck(DurableAckResult{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, DurableLSN: 100, PrimaryTailLSN: tail, PrimaryHeadLSN: uint64(200 + i)})
				if got := countStarts(exec, kind); got > initial {
					if got != initial+1 {
						t.Fatalf("one stalled ack emitted duplicate recovery: kind=%s starts=%d want=%d", kind, got, initial+1)
					}
					return
				}
			}
			t.Fatal("stalled ack never requested legitimate new recovery")
		})
	}
}

func TestAdapter_ConcurrentOldProbesNeverRestartHealthyRecovery(t *testing.T) {
	for _, n := range []int{8, 5} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			a, exec, probe := dedupAdapter(t)
			a.OnSessionClose(SessionCloseResult{ReplicaID: "r1", SessionID: exec.latestSessionID("StartCatchUp"), Success: true, AchievedLSN: 100})
			a.OnDurableAck(DurableAckResult{ReplicaID: "r1", EndpointVersion: 1, TransportEpoch: 1, DurableLSN: 100, PrimaryTailLSN: 10, PrimaryHeadLSN: 100})
			if p := a.Projection(); p.Mode != engine.ModeHealthy {
				t.Fatalf("precondition not healthy: %+v", p)
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); <-start; a.OnProbeResult(probe) }()
			}
			close(start)
			wg.Wait()
			if got := countStarts(exec, "StartCatchUp"); got != 1 {
				t.Fatalf("identical delayed probes restarted healthy recovery: starts=%d path=%v", got, a.CommandLog())
			}
			if p := a.Projection(); p.Mode != engine.ModeHealthy {
				t.Fatalf("old facts regressed healthy replica: %+v", p)
			}
		})
	}
}
