package master

import (
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// supportedCreatingTick ingests r1 (as given) and two ready replicas, checks
// the volume takes the SUPPORTED gate, and runs one product tick.
func supportedCreatingTick(t *testing.T, h *Host, r1 rf3Slot) {
	t.Helper()
	ingestRF3Incarnations(t, h, r1, readySlot("r2-a"), readySlot("r3-a"))
	topo, ok := acceptedTopologyFromPlacementIntents(h.Lifecycle().Placements.ListPlacements())
	if !ok {
		t.Fatal("no accepted topology from placements")
	}
	build := authority.BuildSnapshot(h.obs.Store().Snapshot(), topo, h.Publisher())
	if _, unsupported := build.Report.Unsupported["vol-rf3"]; unsupported {
		t.Fatalf("vol-rf3 is unsupported (%+v); this test needs the supported gate", build.Report.Unsupported["vol-rf3"])
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
}

// Creation is its own phase: a volume whose first line has not reached
// Healthy is never subject to a replacement decision, so no fencing round
// runs and no heal moves the line, however ready the replicas are. The
// schedule from the NET fence-heal run on the SUPPORTED gate: an empty new
// volume, r1 awaiting the replica stores (reachable, not primary-ready),
// r2 and r3 turned ready first.
func TestCreatingVolumeRunsNoFencingRoundOnTheSupportedGate(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(t *testing.T, h *Host)
	}{
		{"the master still awaits the stores", func(*testing.T, *Host) {}},
		{"the master's wait is over, the primary has not got the stores yet", func(t *testing.T, h *Host) { attestAllStores(t, h) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
			defer closeTestMaster(t, h)
			c.setup(t, h)
			p := awaitingProbe(t, h, true)
			p.mu.Lock()
			p.empty = true // a new volume: every end is zero
			p.mu.Unlock()
			time.Sleep(250 * time.Millisecond) // past the boot grace
			awaiting := rf3Slot{incarnation: "r1-a", reachable: true, eligible: true}
			supportedCreatingTick(t, h, awaiting)
			supportedCreatingTick(t, h, awaiting)
			p.mu.Lock()
			fences := p.fenceCalls
			p.mu.Unlock()
			if fences != 0 {
				t.Fatalf("%d fencing rounds against a creating volume", fences)
			}
			expectLineStaysR1(t, h, "a creating volume with ready replicas")
		})
	}
}

// While the master's record still awaits the stores, the volume is creating
// whatever the primary answers: a creating primary that stops answering is
// not replaced either (the volume waits, visibly).
func TestCreatingVolumeIsNotReplacedWhenItsPrimaryIsSilent(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	if !inCreationWait(h) {
		t.Fatal("fixture: the volume is not creating")
	}
	p := probeOf(t, h)
	p.mu.Lock()
	p.empty, p.currentUnknown = true, true
	p.mu.Unlock()
	time.Sleep(250 * time.Millisecond)
	for i := 0; i < 2; i++ {
		ingestRF3Incarnations(t, h, silent, readySlot("r2-a"), readySlot("r3-a"))
		if _, err := h.RunLifecycleProductTick(); err != nil {
			t.Fatal(err)
		}
	}
	p.mu.Lock()
	fences := p.fenceCalls
	p.mu.Unlock()
	if fences != 0 {
		t.Fatalf("%d fencing rounds against a creating volume with a silent primary", fences)
	}
	expectLineStaysR1(t, h, "a creating volume whose primary is silent")
}

// establishVolume records the current primary's attestation of every
// replica's store, as a primary's first heartbeat after its bind does, so
// the volume has left the creation phase (an established volume). Fixtures
// that exercise promotion on an established volume call it after the first
// line is published.
func establishVolume(t *testing.T, h *Host, volumeID string) {
	t.Helper()
	line := waitAuthorityLine(t, h.Publisher(), volumeID)
	p, ok := h.Lifecycle().Placements.GetPlacement(volumeID)
	if !ok {
		t.Fatalf("establish %s: no placement", volumeID)
	}
	stores := map[string]storage.StoreID{}
	for _, s := range p.Slots {
		stores[s.ReplicaID] = storeOf(s.ReplicaID)
	}
	published, _ := h.Publisher().PublishedAuthorityLine(volumeID)
	server := h.workspaceLineOwner(published, volumeID, line.ReplicaID, line.Epoch)
	if err := h.ingestStoreAttestation(server, attestation(volumeID, line.ReplicaID, line.Epoch, stores)); err != nil {
		t.Fatalf("establish %s: %v", volumeID, err)
	}
	h.attest.awaited(volumeID, h.replicaSlotsFor(volumeID))   // the wait closes
	if _, err := h.attest.noteFirstReady(volumeID); err != nil { // creation ended (the primary's first ready answer)
		t.Fatalf("establish %s: %v", volumeID, err)
	}
	if line, _ := h.Publisher().VolumeAuthorityLine(volumeID); h.inCreationPhase(volumeID, line, nil) != "" {
		t.Fatalf("establish %s: still creating after the attestation (line %+v, server %q)", volumeID, line, server)
	}
}

// The operator's entries read the same phase (model row
// mut_today_operator_entry): a failover or planned move of a creating volume
// is refused before any probe or fencing round, whatever the answers say.
func TestCreatingVolumeRefusesTheOperatorsMove(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{})
	defer closeTestMaster(t, h)
	p := probeOf(t, h)
	p.mu.Lock()
	p.currentReady = true // a perfect answer: ready, nothing awaited
	p.mu.Unlock()
	if !inCreationWait(h) {
		t.Fatal("fixture: the volume is not creating")
	}
	if _, err := h.plannedMoveEvidence("vol-rf3", "r2"); err != ErrMoveWhileCreating {
		t.Fatalf("err=%v; want ErrMoveWhileCreating", err)
	}
	p.mu.Lock()
	fences := p.fenceCalls
	p.mu.Unlock()
	if fences != 0 {
		t.Fatalf("%d fencing rounds for a refused move of a creating volume", fences)
	}
}

// The phase is the master's durable record, not memory (model row
// mut_mark_in_memory): a master restarted while a volume creates still
// treats it as creating, and one restarted after the stores were attested
// still fails a dead primary over (the mark is never lost).
func TestCreationPhaseSurvivesAMasterRestart(t *testing.T) {
	for _, established := range []bool{false, true} {
		t.Run(map[bool]string{false: "still creating", true: "established"}[established], func(t *testing.T) {
			cfg := Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(),
				ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}}
			h := newBootGraceMasterCreating(t, cfg)
			if established {
				attestAllStores(t, h)
				h.attest.awaited("vol-rf3", h.replicaSlotsFor("vol-rf3"))
				if _, err := h.attest.noteFirstReady("vol-rf3"); err != nil {
					t.Fatal(err)
				}
			}
			closeTestMaster(t, h)

			cfg.Listen = "127.0.0.1:0"
			cfg.ControllerConfig.ExpectedSlotsPerVolume = 3
			cfg.Freshness.FreshnessWindow = testFreshness
			cfg.Freshness.PendingGrace = time.Millisecond
			h2, err := New(cfg)
			if err != nil {
				t.Fatalf("master restart: %v", err)
			}
			h2.Start()
			defer closeTestMaster(t, h2)
			p := &switchProbe{} // r1 not ready (not awaiting), r2 ready with evidence
			h2.SetPromotionEvidenceProvider(p)
			if got := h2.inCreationPhase("vol-rf3", waitAuthorityLine(t, h2.Publisher(), "vol-rf3"), nil) != ""; got == established {
				t.Fatalf("after the restart creating=%t; want %t", got, !established)
			}
			time.Sleep(250 * time.Millisecond) // past the boot grace
			ineligible := restarting
			ineligible.eligible = false
			if established {
				unsupportedTick(t, h2, ineligible)
				expectFailoverToR2(t, h2, "an established volume's unhealthy primary after a master restart")
				return
			}
			creatingTick(t, h2, ineligible)
			expectLineStaysR1(t, h2, "a creating volume after a master restart")
		})
	}
}

// Creation does not end at the last store attestation: a primary restarted
// after it, before it adopts the complete assignment (it answers not ready,
// and no longer says it awaits the stores), with no write on any survivor,
// is still creating: no fencing round, no reassign. Creation ends at a
// ready answer or a survivor's write (TestCreationEndsAtTheFirstReadyAnswerOrASurvivorsWrite).
func TestCreationOutlivesTheLastStoreAttestation(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	attestAllStores(t, h) // every store recorded: the master's wait is over
	p := probeOf(t, h)
	p.mu.Lock()
	p.empty = true // r1 answers not ready (restarted), awaiting nothing
	p.mu.Unlock()
	time.Sleep(250 * time.Millisecond) // past the boot grace
	restarted := rf3Slot{incarnation: "r1-b", reachable: true, eligible: true}
	supportedCreatingTick(t, h, restarted)
	supportedCreatingTick(t, h, restarted)
	p.mu.Lock()
	fences := p.fenceCalls
	p.mu.Unlock()
	if fences != 0 {
		t.Fatalf("%d fencing rounds before the primary's first ready answer", fences)
	}
	expectLineStaysR1(t, h, "a restarted primary before its first ready answer")
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if h.inCreationPhase("vol-rf3", line, nil) == "" {
		t.Fatal("creation ended with no write on any survivor")
	}
}

// recordFirstReady records the end of creation (after the stores were
// attested), as the primary's first ready answer does: fixtures whose
// primary was healthy before the scenario's failure.
func recordFirstReady(t *testing.T, h *Host, volumeID string) {
	t.Helper()
	h.attest.awaited(volumeID, h.replicaSlotsFor(volumeID))
	if _, err := h.attest.noteFirstReady(volumeID); err != nil {
		t.Fatal(err)
	}
}

// The operator's failover runs through the same first check (the operator
// gate, plannedMoveEvidenceAt): on a creating RF2 volume whose primary is
// reachable, a planned move would otherwise be allowed (its target is in
// every acknowledgement set), so without the check the line would move.
// It is refused, and the line stays.
func TestOperatorFailoverOfACreatingVolumeIsRefused(t *testing.T) {
	h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 2})
	defer closeTestMaster(t, h)
	h.SetPromotionEvidenceProvider(staticPromotionProbe{result: PromotionProbeResult{
		AckProfile:   "sync_quorum",
		CurrentKnown: true,
		Current:      PromotionCandidateEvidence{ReplicaID: "r1", Ready: true},
		Candidates:   []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 1}},
	}})
	seedRF2Placement(t, h)
	ingestRF2Observation(t, h, true, true)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf2")
	if h.inCreationPhase("vol-rf2", line, nil) == "" {
		t.Fatal("fixture: the volume is not creating")
	}
	_, err := h.FailbackAuthorityRuntime().ExecuteFailback(t.Context(), authority.FailbackRuntimeRequest{
		VolumeID: "vol-rf2", ReplicaID: "r2", TargetDataAddr: "127.0.0.1:19201", TargetCtrlAddr: "127.0.0.1:19202",
		ExpectedCurrentReplicaID: line.ReplicaID, ExpectedCurrentEpoch: line.Epoch,
		AckEligible: true, FrontendFencedBeforeFailback: true, DurableFrontierCovered: true, NoCrossVolumeIdentityChange: true,
	})
	if !errors.Is(err, ErrMoveWhileCreating) {
		t.Fatalf("operator failover of a creating volume: %v; want ErrMoveWhileCreating", err)
	}
	if after, _ := h.Publisher().VolumeAuthorityLine("vol-rf2"); after != line {
		t.Fatalf("line=%+v; want %+v unchanged", after, line)
	}
}
