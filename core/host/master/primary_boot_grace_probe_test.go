package master

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

// #128 boot grace with the production promotion probe composition
// (review of #130 r4): the provider probes the current primary first,
// then the peers. These tests hold the peer query while the current
// primary's actual status and heartbeat change.

type fakeStatus struct {
	mu        sync.Mutex
	replicaID string
	body      map[string]any
	holdFirst bool
	entered   chan struct{}
	release   chan struct{}
}

func (f *fakeStatus) set(body map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body = body
}

func (f *fakeStatus) serve(t *testing.T) int {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			f.mu.Lock()
			hold := f.holdFirst
			f.holdFirst = false
			entered, release := f.entered, f.release
			f.mu.Unlock()
			if hold {
				close(entered)
				<-release
			}
			f.mu.Lock()
			body := f.body
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(body)
		case "/status/durable":
			_ = json.NewEncoder(w).Encode(map[string]any{"Volumes": []map[string]any{{
				"VolumeID": "vol-rf3", "ReplicaID": f.replicaID, "StoreID": testStoreHex, "FrontierKnown": true, "DurableLSN": 70,
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	_, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	if port <= 20000 {
		t.Skipf("status port %d too low for the provider's +20000 convention", port)
	}
	return port - 20000
}

func currentStatus(frontendReady bool) map[string]any {
	return map[string]any{
		"VolumeID": "vol-rf3", "ReplicaID": "r1", "AuthorityRole": "primary",
		"Healthy": frontendReady, "FrontendPrimaryReady": frontendReady,
	}
}

// useProductionProbe installs the real workload-plan provider over fake
// status servers; r2's first /status query blocks until released.
func useProductionProbe(t *testing.T, h *Host) (r1, r2 *fakeStatus) {
	t.Helper()
	r1 = &fakeStatus{replicaID: "r1", body: currentStatus(false)}
	r2 = &fakeStatus{replicaID: "r2", body: map[string]any{
		"VolumeID": "vol-rf3", "ReplicaID": "r2", "ReplicationRole": "replica_ready",
	}, holdFirst: true, entered: make(chan struct{}), release: make(chan struct{})}
	plans := []lifecycle.BlockVolumeWorkloadPlan{{
		VolumeID: "vol-rf3",
		Protocol: "iscsi",
		Replicas: []lifecycle.BlockVolumeReplicaWorkload{
			{ServerID: "node-a", ReplicaID: "r1", ISCSIListenPort: r1.serve(t)},
			{ServerID: "node-b", ReplicaID: "r2", ISCSIListenPort: r2.serve(t)},
			{ServerID: "node-c", ReplicaID: "r3"},
		},
	}}
	// r3 has no status server, so r2's promotion needs a known frontier
	// it covers (#181): r2 reports durable 70.
	h.SetPromotionEvidenceProvider(NewWorkloadPlanPromotionEvidenceProvider(plans, WorkloadPlanPromotionProbeConfig{
		AckProfile:          "sync-quorum",
		Attested:            attestEvery(testStoreID),
		RequiredLSNByVolume: map[string]uint64{"vol-rf3": 70},
	}))
	return r1, r2
}

// productionHeldTick runs a product tick whose peer query (r2) is held
// after the current primary was probed not ready, runs during(), then
// releases it and waits for the tick.
func productionHeldTick(t *testing.T, h *Host, r2 *fakeStatus, r3 rf3Slot, during func()) {
	t.Helper()
	ingestRF3Incarnations(t, h, restarting, readySlot("r2-a"), r3)
	done := make(chan error, 1)
	go func() {
		_, err := h.RunLifecycleProductTick()
		done <- err
	}()
	select {
	case <-r2.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("peer query never started")
	}
	during()
	close(r2.release)
	if err := <-done; err != nil {
		t.Fatalf("product tick: %v", err)
	}
}

// recovers: the current primary's frontend finishes recovery and a newer
// ready heartbeat arrives while the peer query is held.
func recovers(t *testing.T, h *Host, r1 *fakeStatus, r3 rf3Slot) func() {
	return func() {
		r1.set(currentStatus(true))
		time.Sleep(5 * time.Millisecond)
		readyB := restarting
		readyB.ready = true
		ingestRF3Incarnations(t, h, readyB, readySlot("r2-a"), r3)
	}
}

// engineReadyOnly: a newer heartbeat reports engine readiness, but the
// frontend is still not ready (the actual status stays unready).
func engineReadyOnly(t *testing.T, h *Host, r3 rf3Slot) func() {
	return func() {
		time.Sleep(5 * time.Millisecond)
		readyB := restarting
		readyB.ready = true
		ingestRF3Incarnations(t, h, readyB, readySlot("r2-a"), r3)
	}
}

func TestBootGrace_ProductionProbeRecoveryDuringPeerQueryUnsupported(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)
	r1, r2 := useProductionProbe(t, h)
	productionHeldTick(t, h, r2, silent, recovers(t, h, r1, silent))
	expectLineStaysR1(t, h, "current recovered during the held peer query (unsupported path)")
}

func TestBootGrace_ProductionProbeRecoveryDuringPeerQuerySupported(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)
	r1, r2 := useProductionProbe(t, h)
	r3 := readySlot("r3-a") // node-c reports again: the volume is supported
	productionHeldTick(t, h, r2, r3, recovers(t, h, r1, r3))
	expectLineStaysR1(t, h, "current recovered during the held peer query (supported path)")
}

// Counter-control: heartbeat engine-ready alone is not frontend-ready.
// The fresh probe still finds the frontend unready, so failover stands.
func TestBootGrace_ProductionProbeEngineReadyButFrontendUnreadyFailsOver(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)
	_, r2 := useProductionProbe(t, h)
	productionHeldTick(t, h, r2, silent, engineReadyOnly(t, h, silent))
	expectFailoverToR2(t, h, "engine-ready heartbeat but frontend still unready")
}

// Counter-controls (review of #130 r5): while the peer query is held,
// the frontend finishes recovery but the newer heartbeat, though
// engine-ready, excludes the current primary (ineligible, withdrawn or
// unreachable). Frontend readiness must not preserve an excluded
// primary: the failover stands.
func TestBootGrace_ProductionProbeRecoveredButExcludedFailsOver(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*rf3Slot)
	}{
		{"ineligible", func(s *rf3Slot) { s.eligible = false }},
		{"withdrawn", func(s *rf3Slot) { s.withdrawn = true }},
		{"unreachable", func(s *rf3Slot) { s.reachable = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
			defer closeTestMaster(t, h)
			r1, r2 := useProductionProbe(t, h)
			productionHeldTick(t, h, r2, silent, func() {
				r1.set(currentStatus(true))
				time.Sleep(5 * time.Millisecond)
				excluded := restarting
				excluded.ready = true
				tc.mut(&excluded)
				ingestRF3Incarnations(t, h, excluded, readySlot("r2-a"), silent)
			})
			expectFailoverToR2(t, h, "recovered frontend but "+tc.name+" heartbeat")
		})
	}
}
