package master

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/storage"
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
	withdrawn   bool
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
				Withdrawn:       s.slot.withdrawn,
			}},
		}); err != nil {
			t.Fatalf("ingest %s: %v", s.server, err)
		}
	}
}

// switchProbe is the promotion probe: r2 is always a ready survivor, r3
// answers as a lagging replica (not ready; its end is r3End, 60 when unset);
// the current primary r1 is ready only while currentReady is set.
type switchProbe struct {
	mu           sync.Mutex
	r3End        uint64
	fenceSilent  bool // no survivor answers the fencing round
	fenceCalls   int
	currentReady bool
	// currentUnknown: the current primary did not answer (status GET
	// refused or timed out), so CurrentKnown is false.
	currentUnknown bool
	// empty: a new volume, never written: every end is zero and no frontier
	// is required.
	empty bool
	// awaitingEpoch: the current primary answers that it is not ready only
	// because its assignment at this epoch awaits replica stores (0: no).
	awaitingEpoch uint64
	// profile is the answered ack profile ("" = sync-quorum).
	profile string
	// mismatch is the round's ProfileMismatch ("" = every answer agrees).
	mismatch string
	// When hold is set, a probe signals entered and then blocks until
	// release is closed.
	hold    bool
	entered chan struct{}
	release chan struct{}
}

func (p *switchProbe) holdNext() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hold, p.entered, p.release = true, make(chan struct{}), make(chan struct{})
}

func (p *switchProbe) setCurrentReady(ready bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.currentReady = ready
}

func (p *switchProbe) ProbePromotionCandidates(volumeID string, current authority.AuthorityBasis, candidates []authority.ReplicaCandidate) (PromotionProbeResult, error) {
	p.mu.Lock()
	if p.hold {
		p.hold = false
		entered, release := p.entered, p.release
		p.mu.Unlock()
		close(entered)
		<-release
		p.mu.Lock()
	}
	defer p.mu.Unlock()
	if p.empty {
		return PromotionProbeResult{
			AckProfile:      p.ackProfile(),
			ProfileMismatch: p.mismatch,
			CurrentKnown:    !p.currentUnknown,
			Current:      PromotionCandidateEvidence{ReplicaID: "r1", Ready: p.currentReady, AwaitingReplicaStoresEpoch: p.awaitingEpoch},
			Candidates:   []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true}, {ReplicaID: "r3"}},
		}, nil
	}
	return PromotionProbeResult{
		AckProfile:      p.ackProfile(),
		ProfileMismatch: p.mismatch,
		// Only r2 is ready, so its promotion needs a known frontier it
		// covers (#181); r3 answers but lags. A fencing round counts both.
		SyncAckLSN:   70,
		CurrentKnown: !p.currentUnknown,
		Current:      PromotionCandidateEvidence{ReplicaID: "r1", Ready: p.currentReady, AwaitingReplicaStoresEpoch: p.awaitingEpoch, DurableLSN: 70},
		Candidates:   []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 70}, {ReplicaID: "r3", DurableLSN: p.lagEnd()}},
	}, nil
}

func (p *switchProbe) ackProfile() string {
	if p.profile == "" {
		return "sync-quorum"
	}
	return p.profile
}

func (p *switchProbe) lagEnd() uint64 {
	if p.r3End == 0 {
		return 60
	}
	return p.r3End
}

func probeOf(t *testing.T, h *Host) *switchProbe {
	t.Helper()
	h.promotionMu.RLock()
	defer h.promotionMu.RUnlock()
	p, ok := h.promotionProber.(*switchProbe)
	if !ok {
		t.Fatalf("promotion prober is %T", h.promotionProber)
	}
	return p
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
// The volume has left its creation wait (its stores are attested): an
// established volume, whose not-ready primary is a failover candidate.
func newBootGraceMaster(t *testing.T, cfg Config) *Host {
	t.Helper()
	h := newBootGraceMasterCreating(t, cfg)
	attestAllStores(t, h)
	establishVolume(t, h, "vol-rf3") // and the primary's first ready answer: an established volume
	return h
}

// newBootGraceMasterCreating is newBootGraceMaster for a volume still in its
// creation wait (no store attested).
func newBootGraceMasterCreating(t *testing.T, cfg Config) *Host {
	t.Helper()
	if cfg.AuthorityStoreDir == "" {
		cfg.AuthorityStoreDir = t.TempDir()
	}
	if cfg.LifecycleStoreDir == "" {
		cfg.LifecycleStoreDir = t.TempDir()
	}
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
	h.SetPromotionEvidenceProvider(&switchProbe{})
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

// attestAllStores records r1's attestation of every replica's store, so the
// volume has left its creation wait (an established volume).
func attestAllStores(t *testing.T, h *Host) {
	t.Helper()
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": testStoreID, "r2": testStoreID, "r3": testStoreID})); err != nil {
		t.Fatal(err)
	}
	if inCreationWait(h) {
		t.Fatal("fixture: still in the creation wait after the attestation")
	}
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

// A current primary that did not answer its status probe is UNKNOWN, not
// failed: the unsupported path never reassigns on it, however ready the
// survivor (an unknown fact is never read as failed). Only a positive
// not-ready report from the primary itself moves the line.
func TestUnsupportedPathNeverReassignsOnAnUnansweredPrimary(t *testing.T) {
	h := newBootGraceMaster(t, Config{})
	defer closeTestMaster(t, h)
	p := probeOf(t, h)

	p.mu.Lock()
	p.currentUnknown = true
	p.mu.Unlock()
	unsupportedTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "status probe of the current primary timed out")
	unsupportedTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "still unanswered on the next tick")
}

// The unsupported path decides a failover on a fencing round too: with the
// round unanswered nothing is published, however ready the survivor looked
// in the plain round; answered, the line carries the fenced epoch.
func TestUnsupportedPathFailsOverOnlyOnAFencingRound(t *testing.T) {
	ineligible := restarting
	ineligible.eligible = false
	t.Run("fencing round unanswered", func(t *testing.T) {
		h := newBootGraceMaster(t, Config{})
		defer closeTestMaster(t, h)
		p := probeOf(t, h)
		p.mu.Lock()
		p.fenceSilent = true
		p.mu.Unlock()
		unsupportedTick(t, h, ineligible)
		expectLineStaysR1(t, h, "the survivors did not answer the fencing round")
	})
	t.Run("fencing round answered", func(t *testing.T) {
		h := newBootGraceMaster(t, Config{})
		defer closeTestMaster(t, h)
		unsupportedTick(t, h, ineligible)
		expectFailoverToR2(t, h, "fenced survivors")
		if info, _ := h.Publisher().LastPublished("vol-rf3", "r2"); info.Primary.FencedEpoch != 2 {
			t.Fatalf("failover line evidence %+v; want fenced at 2", info.Primary)
		}
	})
}

// A primary restarting inside its boot grace is not replaced, so no fencing
// round cuts its replicas off from it; past the grace it is fenced.
func TestBootGrace_NoFencingRoundInsideTheGrace(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 600 * time.Millisecond}})
	defer closeTestMaster(t, h)
	p := probeOf(t, h)
	calls := func() int { p.mu.Lock(); defer p.mu.Unlock(); return p.fenceCalls }

	ingestRF3Incarnations(t, h, restarting, readySlot("r2-a"), readySlot("r3-a"))
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	if n := calls(); n != 0 {
		t.Fatalf("%d fencing rounds against a primary inside its boot grace", n)
	}
	expectLineStaysR1(t, h, "restarted primary inside its boot grace")

	time.Sleep(600 * time.Millisecond)
	ingestRF3Incarnations(t, h, restarting, readySlot("r2-a"), readySlot("r3-a"))
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	if calls() == 0 {
		t.Fatal("no fencing round once the grace has passed")
	}
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
		{-time.Second, 2 * time.Second},
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

	// An omitted freshness window is the store's effective 10s default;
	// an explicit 5m grace is capped at it too.
	omitted, err := New(Config{
		AuthorityStoreDir: t.TempDir(),
		LifecycleStoreDir: t.TempDir(),
		Listen:            "127.0.0.1:0",
		ControllerConfig:  authority.TopologyControllerConfig{PrimaryBootGrace: 5 * time.Minute},
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	if got := omitted.Controller().PrimaryBootGrace(); got != 10*time.Second {
		t.Errorf("omitted freshness window, explicit 5m grace: grace %s, want the effective 10s window", got)
	}

	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 5 * time.Minute}})
	defer closeTestMaster(t, h)
	unsupportedTick(t, h, restarting)
	expectLineStaysR1(t, h, "inside the capped grace")
	time.Sleep(testFreshness)
	unsupportedTick(t, h, restarting)
	expectFailoverToR2(t, h, "unready past the capped grace (the 1s window, not 5m)")
}

// On the unsupported path a READY tick (no promotion needed) must still
// record the fact: once r1-b has reported ready, going unready again in
// the same incarnation gets no second grace (review of #130 r2).
func TestBootGrace_UnsupportedPathReadyTickEndsGrace(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)
	p := probeOf(t, h)

	unsupportedTick(t, h, restarting) // r1-b starts its grace
	expectLineStaysR1(t, h, "restarted primary inside its grace")

	p.setCurrentReady(true)
	readyB := restarting
	readyB.ready = true
	unsupportedTick(t, h, readyB) // ready: no promotion needed, but the fact is recorded
	expectLineStaysR1(t, h, "ready current primary")

	p.setCurrentReady(false)
	unsupportedTick(t, h, restarting) // same incarnation, unready again
	expectFailoverToR2(t, h, "unready again after ready in the same incarnation")
}

// Ready restarts on the unsupported path count toward flap history: a
// third start within the window gets no grace even though the second
// was ready the whole time.
func TestBootGrace_UnsupportedPathReadyStartsCountTowardFlapping(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)
	p := probeOf(t, h)

	p.setCurrentReady(true)
	readyB := rf3Slot{incarnation: "r1-b", reachable: true, eligible: true, ready: true}
	unsupportedTick(t, h, readyB) // start 2 (r1-a was start 1), ready
	expectLineStaysR1(t, h, "ready restarted primary")

	p.setCurrentReady(false)
	third := rf3Slot{incarnation: "r1-c", reachable: true, eligible: true}
	unsupportedTick(t, h, third) // start 3 within the window: flapping
	expectFailoverToR2(t, h, "third start within the flap window")
}

// ingestUnknownClaim makes node-d report a replica the topology does not
// know: UnknownReplicaClaim, a reason outside the promotion allow-list.
func ingestUnknownClaim(t *testing.T, h *Host) {
	t.Helper()
	if err := h.ObservationHost().Ingest(authority.Observation{
		ServerID:   "node-d",
		ObservedAt: time.Now().UTC(),
		Server:     authority.ServerFact{Reachable: true, Eligible: true},
		Slots: []authority.SlotFact{{
			VolumeID: "vol-rf3", ReplicaID: "r9", DataAddr: "127.0.0.1:19901", CtrlAddr: "127.0.0.1:19902",
			Reachable: true, Eligible: true,
		}},
	}); err != nil {
		t.Fatalf("ingest node-d: %v", err)
	}
}

// disallowedTick ingests r1/r2 plus the unknown claim and runs a tick
// while the volume is unsupported for a reason the promotion gate does
// not probe for.
func disallowedTick(t *testing.T, h *Host, r1 rf3Slot) {
	t.Helper()
	ingestRF3Incarnations(t, h, r1, readySlot("r2-a"), silent)
	ingestUnknownClaim(t, h)
	res, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if res.PromotionProbes != 0 {
		t.Fatalf("product tick %+v: the disallowed reason must not probe", res)
	}
}

// A ready report while the volume is unsupported for a disallowed
// reason is still recorded: after the reason clears, the same
// incarnation going unready gets no grace.
func TestBootGrace_DisallowedReasonStillRecordsReady(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)

	readyB := rf3Slot{incarnation: "r1-b", reachable: true, eligible: true, ready: true}
	disallowedTick(t, h, readyB)
	time.Sleep(testFreshness + 100*time.Millisecond) // node-d's claim ages out
	unsupportedTick(t, h, restarting)                // r1-b unready, same incarnation
	expectFailoverToR2(t, h, "unready after a ready report seen under a disallowed reason")
}

// A start seen under a disallowed reason counts toward flapping.
func TestBootGrace_DisallowedReasonStartCountsTowardFlapping(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)

	readyB := rf3Slot{incarnation: "r1-b", reachable: true, eligible: true, ready: true}
	disallowedTick(t, h, readyB) // start 2
	time.Sleep(testFreshness + 100*time.Millisecond)
	third := rf3Slot{incarnation: "r1-c", reachable: true, eligible: true}
	unsupportedTick(t, h, third) // start 3: flapping
	expectFailoverToR2(t, h, "third start, the second seen under a disallowed reason")
}

// heldTick starts a product tick whose promotion probe blocks, runs
// during() while it is held, then releases it and waits for the tick.
func heldTick(t *testing.T, h *Host, r1 rf3Slot, during func()) {
	heldTickAfter(t, h, r1, 0, during)
}

// heldTickAfter is heldTick with a pause between the ingest and the tick,
// so the heartbeat is older than the grace the tick starts.
func heldTickAfter(t *testing.T, h *Host, r1 rf3Slot, pause time.Duration, during func()) {
	t.Helper()
	p := probeOf(t, h)
	ingestRF3Incarnations(t, h, r1, readySlot("r2-a"), silent)
	time.Sleep(pause)
	p.holdNext()
	done := make(chan error, 1)
	go func() {
		_, err := h.RunLifecycleProductTick()
		done <- err
	}()
	select {
	case <-p.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("promotion probe never entered")
	}
	during()
	close(p.release)
	if err := <-done; err != nil {
		t.Fatalf("product tick: %v", err)
	}
}

// The current primary turns explicitly unhealthy while the probe is
// held: the gate must decide from the post-probe fact and fail over.
func TestBootGrace_HeldProbeNewerUnhealthyFailsOver(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)

	heldTick(t, h, restarting, func() {
		ineligible := restarting
		ineligible.eligible = false
		time.Sleep(5 * time.Millisecond) // a strictly newer observation
		ingestRF3Incarnations(t, h, ineligible, readySlot("r2-a"), silent)
	})
	expectFailoverToR2(t, h, "ineligible report arrived during the probe")
}

// The current primary's heartbeat expires while the probe is held, its
// grace (started at the tick) still running: no fresh slot remains at
// the decision point, so there is no grace.
func TestBootGrace_HeldProbeHeartbeatExpiryFailsOver(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)

	// r1's fact is ingested 600 ms before the tick; the grace starts at
	// the tick (capped at the 1 s window). Holding the probe 500 ms ages
	// the fact past 1 s while the grace is about 0.5 s old.
	heldTickAfter(t, h, restarting, 600*time.Millisecond, func() {
		time.Sleep(500 * time.Millisecond)
	})
	expectFailoverToR2(t, h, "heartbeat expired during the probe")
}

// Positive: nothing changes while the probe is held: the grace holds.
func TestBootGrace_HeldProbeUnchangedFreshHolds(t *testing.T) {
	h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: time.Minute}})
	defer closeTestMaster(t, h)

	heldTick(t, h, restarting, func() { time.Sleep(50 * time.Millisecond) })
	expectLineStaysR1(t, h, "unchanged fresh restarted primary across a held probe")
}
