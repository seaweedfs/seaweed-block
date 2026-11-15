package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// #128 boot grace through the full lifecycle product tick, on the
// unsupported-volume promotion path (review of #130): the third
// server's observation has gone stale, so the volume is unsupported,
// but the current primary's own fresh heartbeat says it is a restarted,
// placeable, not-yet-ready process.

type rf3Slot struct {
	skip        bool // the server sends nothing (its last fact ages out)
	incarnation string
	reachable   bool
	eligible    bool
	ready       bool
}

func ingestRF3Incarnations(t *testing.T, h *Host, r1, r2, r3 rf3Slot) {
	t.Helper()
	now := time.Now().UTC()
	for i, s := range []struct {
		server, replica string
		slot            rf3Slot
	}{
		{"node-a", "r1", r1},
		{"node-b", "r2", r2},
		{"node-c", "r3", r3},
	} {
		if s.slot.skip {
			continue
		}
		if err := h.ObservationHost().Ingest(authority.Observation{
			ServerID:   s.server,
			ObservedAt: now,
			Server:     authority.ServerFact{Reachable: true, Eligible: true},
			Slots: []authority.SlotFact{{
				VolumeID:        "vol-rf3",
				ReplicaID:       s.replica,
				DataAddr:        "127.0.0.1:19" + string(rune('1'+i)) + "01",
				CtrlAddr:        "127.0.0.1:19" + string(rune('1'+i)) + "02",
				Reachable:       s.slot.reachable,
				ReadyForPrimary: s.slot.ready,
				Eligible:        s.slot.eligible,
				EvidenceScore:   uint64(30 - i),
				Incarnation:     s.slot.incarnation,
			}},
		}); err != nil {
			t.Fatalf("ingest %s: %v", s.server, err)
		}
	}
}

func readySlot(inc string) rf3Slot {
	return rf3Slot{incarnation: inc, reachable: true, eligible: true, ready: true}
}

var (
	silent     = rf3Slot{skip: true}
	restarting = rf3Slot{incarnation: "r1-b", reachable: true, eligible: true}
)

const testFreshness = time.Second

// newBootGraceMaster starts a master (freshness window testFreshness
// unless cfg says otherwise, bootstrap pending grace 1 ms) whose
// promotion probe says the current primary r1 is not ready and r2 is a
// ready survivor, and publishes r1 from an all-ready first observation.
// It then lets node-c's facts age out, so the volume is unsupported.
func newBootGraceMaster(t *testing.T, cfg Config) *Host {
	t.Helper()
	cfg.AuthorityStoreDir = t.TempDir()
	cfg.LifecycleStoreDir = t.TempDir()
	cfg.Listen = "127.0.0.1:0"
	cfg.ControllerConfig.ExpectedSlotsPerVolume = 3
	if cfg.Freshness.FreshnessWindow == 0 {
		cfg.Freshness.FreshnessWindow = testFreshness
	}
	cfg.Freshness.PendingGrace = time.Millisecond
	h, err := New(cfg)
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	h.Start()
	h.SetPromotionEvidenceProvider(staticPromotionProbe{result: PromotionProbeResult{
		AckProfile:   "sync-quorum",
		CurrentKnown: true,
		Current:      PromotionCandidateEvidence{ReplicaID: "r1", Ready: false},
		Candidates:   []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 70}},
	}})
	seedRF3PlacementForServersWithAddrs(t, h, "vol-rf3", "node-a", "node-b", "node-c")
	ingestRF3Incarnations(t, h, readySlot("r1-a"), readySlot("r2-a"), readySlot("r3-a"))
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("initial product tick: %v", err)
	}
	if line := waitAuthorityLine(t, h.Publisher(), "vol-rf3"); line.ReplicaID != "r1" {
		t.Fatalf("initial line=%+v want r1", line)
	}
	time.Sleep(cfg.Freshness.FreshnessWindow + 100*time.Millisecond) // node-c goes stale
	return h
}

// unsupportedTick ingests fresh r1/r2 facts (node-c stays silent) and
// runs one product tick, requiring that the unsupported-volume gate
// probed.
func unsupportedTick(t *testing.T, h *Host, r1 rf3Slot) {
	t.Helper()
	ingestRF3Incarnations(t, h, r1, readySlot("r2-a"), silent)
	topo, ok := acceptedTopologyFromPlacementIntents(h.Lifecycle().Placements.ListPlacements())
	if !ok {
		t.Fatal("no accepted topology from placements")
	}
	build := authority.BuildSnapshot(h.obs.Store().Snapshot(), topo, h.Publisher())
	if _, unsupported := build.Report.Unsupported["vol-rf3"]; !unsupported {
		t.Fatalf("vol-rf3 is not unsupported (report %+v); the test needs the unsupported-volume path", build.Report)
	}
	res, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if res.PromotionProbes != 1 {
		t.Fatalf("product tick %+v: want one promotion probe", res)
	}
}

func expectLineStaysR1(t *testing.T, h *Host, why string) {
	t.Helper()
	time.Sleep(150 * time.Millisecond)
	line, ok := h.Publisher().VolumeAuthorityLine("vol-rf3")
	if !ok || line.ReplicaID != "r1" || line.Epoch != 1 {
		t.Fatalf("%s: line=%+v want r1@1 unchanged", why, line)
	}
}

func expectFailoverToR2(t *testing.T, h *Host, why string) {
	t.Helper()
	if line := waitAuthorityReplica(t, h.Publisher(), "vol-rf3", "r2"); line.Epoch != 2 {
		t.Fatalf("%s: line=%+v want r2@2", why, line)
	}
}

// Inside the grace: no direct reassign. Still unready after it:
// failover to the ready survivor.
func TestBootGrace_UnsupportedPathHonorsGraceThenFailsOver(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 600 * time.Millisecond}})
	defer closeTestMaster(t, h)

	unsupportedTick(t, h, restarting)
	expectLineStaysR1(t, h, "restarted primary inside its boot grace, third server stale")

	time.Sleep(600 * time.Millisecond)
	unsupportedTick(t, h, restarting)
	expectFailoverToR2(t, h, "unready past its boot grace")
}

// An explicit unhealthy current primary gets no grace on this path.
func TestBootGrace_UnsupportedPathUnhealthyCurrentFailsOverAtOnce(t *testing.T) {
	h := newBootGraceMaster(t, Config{})
	defer closeTestMaster(t, h)

	ineligible := restarting
	ineligible.eligible = false
	unsupportedTick(t, h, ineligible)
	expectFailoverToR2(t, h, "ineligible restarted primary")
}

// A configured grace above the freshness window is capped at it, and a
// genuinely unready primary fails over at that limit.
func TestBootGrace_CappedAtFreshnessWindow(t *testing.T) {
	for _, tc := range []struct {
		configured, want time.Duration
	}{
		{0, 2 * time.Second},
		{5 * time.Minute, 2 * time.Second},
		{time.Second, time.Second},
	} {
		h, err := New(Config{
			AuthorityStoreDir: t.TempDir(),
			LifecycleStoreDir: t.TempDir(),
			Listen:            "127.0.0.1:0",
			Freshness:         authority.FreshnessConfig{FreshnessWindow: 2 * time.Second},
			ControllerConfig:  authority.TopologyControllerConfig{PrimaryBootGrace: tc.configured},
		})
		if err != nil {
			t.Fatalf("master.New: %v", err)
		}
		if got := h.Controller().PrimaryBootGrace(); got != tc.want {
			t.Errorf("configured %s with a 2s freshness window: grace %s, want %s", tc.configured, got, tc.want)
		}
	}

	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 5 * time.Minute}})
	defer closeTestMaster(t, h)
	unsupportedTick(t, h, restarting)
	expectLineStaysR1(t, h, "inside the capped grace")
	time.Sleep(testFreshness)
	unsupportedTick(t, h, restarting)
	expectFailoverToR2(t, h, "unready past the capped grace (the 1s window, not 5m)")
}
