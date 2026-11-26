package master

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// restartCreatingMaster closes h and starts a master on the same stores,
// with a fresh switchProbe (r1 not ready, r2 ready holding DurableLSN 70).
func restartCreatingMaster(t *testing.T, h *Host, cfg Config) *Host {
	t.Helper()
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
	h2.SetPromotionEvidenceProvider(&switchProbe{})
	return h2
}

// operatorProbe is a switchProbe with the sync-quorum profile the
// operator's move reads from its gate.
type operatorProbe struct{ *switchProbe }

func (operatorProbe) AckProfile() string { return "sync_quorum" }

// withOperatorProbe installs p behind the operator's ack profile.
func withOperatorProbe(h *Host, p *switchProbe) { h.SetPromotionEvidenceProvider(operatorProbe{p}) }

// The gap the creation phase must not open: every store is attested, the
// primary serves at once and a write is acknowledged by the quorum (r2
// holds it: DurableLSN 70), and the primary dies before any gate tick could
// read a ready answer from it. The volume served, so it is not creating:
// the operator's failover moves it (decided on the fencing round). Without
// a write on any survivor (it served nothing), it is still creating and
// the failover is refused.
func TestAVolumeWhosePrimaryDiedAfterAQuorumWriteFailsOver(t *testing.T) {
	for _, wrote := range []bool{true, false} {
		t.Run(map[bool]string{true: "a survivor holds the write", false: "no write anywhere"}[wrote], func(t *testing.T) {
			h := newBootGraceMasterCreating(t, Config{})
			defer closeTestMaster(t, h)
			attestAllStores(t, h) // stage 2: the primary may serve now
			p := probeOf(t, h)
			p.mu.Lock()
			p.currentUnknown = true // the primary is gone: it never answers
			p.empty = !wrote
			p.mu.Unlock()
			withOperatorProbe(h, p)
			ingestRF3Incarnations(t, h, silent, readySlot("r2-a"), readySlot("r3-a"))
			_, err := h.plannedMoveEvidence("vol-rf3", "r2")
			if wrote {
				if err != nil {
					t.Fatalf("the operator's failover after a quorum-acknowledged write: %v", err)
				}
				if h.attest.awaitingFirstReady("vol-rf3") {
					t.Fatal("the survivor's write did not end creation durably")
				}
				return
			}
			if err != ErrMoveWhileCreating {
				t.Fatalf("a volume no survivor holds a write of: err=%v; want ErrMoveWhileCreating", err)
			}
		})
	}
}

// Creation ends at the first of the primary's ready answer and a
// survivor's answer that it holds a write of the current line; until then
// (stores attested, a not-ready primary, empty survivors) it is creating.
// Ending is one-way and durable.
func TestCreationEndsAtTheFirstReadyAnswerOrASurvivorsWrite(t *testing.T) {
	for _, by := range []string{"the primary's ready answer", "a survivor's write"} {
		t.Run(by, func(t *testing.T) {
			cfg := Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir()}
			h := newBootGraceMasterCreating(t, cfg)
			attestAllStores(t, h)
			p := probeOf(t, h)
			p.mu.Lock()
			p.empty = true // a not-ready primary, nothing on any survivor
			p.mu.Unlock()
			line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
			if h.inCreationPhase("vol-rf3", line, nil) == "" {
				t.Fatal("creation ended with a not-ready primary and empty survivors")
			}
			// Neither a survivor's row at another epoch nor the primary's own
			// DurableLSN ends it.
			for _, c := range []PromotionCandidateEvidence{{ReplicaID: "r2", DurableLSN: 9, LastDataEpoch: line.Epoch + 1}, {ReplicaID: "r1", DurableLSN: 9}} {
				if h.inCreationPhase("vol-rf3", line, &PromotionProbeResult{Candidates: []PromotionCandidateEvidence{c}}) == "" {
					t.Fatalf("creation ended on %+v", c)
				}
			}
			p.mu.Lock()
			if by == "a survivor's write" {
				p.empty = false // r2 answers DurableLSN 70; r1 still not ready
			} else {
				p.currentReady = true // a Healthy volume that never took a write
			}
			p.mu.Unlock()
			if why := h.inCreationPhase("vol-rf3", line, nil); why != "" {
				t.Fatalf("after %s: still creating (%s)", by, why)
			}
			p.mu.Lock()
			p.empty, p.currentReady = true, false // later answers reopen nothing
			p.mu.Unlock()
			if why := h.inCreationPhase("vol-rf3", line, nil); why != "" {
				t.Fatalf("creation reopened after it ended (%s)", why)
			}
			h2 := restartCreatingMaster(t, h, cfg)
			defer closeTestMaster(t, h2)
			pr := probeOf(t, h2)
			pr.mu.Lock()
			pr.empty = true
			pr.mu.Unlock()
			if why := h2.inCreationPhase("vol-rf3", waitAuthorityLine(t, h2.Publisher(), "vol-rf3"), nil); why != "" {
				t.Fatalf("after a restart the ended creation is back (%s)", why)
			}
		})
	}
}

// When the write that ends the store wait fails, the volume stays held (as
// waiting) and nothing replaces it: no probe, no fencing round, no move,
// also after a restart; once the write succeeds the normal stages follow.
func TestAFailedEndOfTheStoreWaitKeepsTheVolumeHeld(t *testing.T) {
	cfg := Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(),
		ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}}
	h := newBootGraceMasterCreating(t, cfg)
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	// Every store recorded, but the record's next write fails: a directory
	// stands at its temp path.
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": testStoreID, "r2": testStoreID, "r3": testStoreID})); err != nil {
		t.Fatal(err)
	}
	block := filepath.Join(cfg.AuthorityStoreDir, storeAttestationFile+".tmp")
	if err := os.Mkdir(block, 0o755); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond) // past the boot grace
	dead := restarting
	dead.eligible = false
	for i := 0; i < 2; i++ {
		if why := h.inCreationPhase("vol-rf3", line, nil); why == "" {
			t.Fatal("a failed write of the end of the store wait released the volume")
		}
		creatingTick(t, h, dead)
		expectLineStaysR1(t, h, "the end of the store wait not recorded")
		if _, err := h.plannedMoveEvidence("vol-rf3", "r2"); err != ErrMoveWhileCreating {
			t.Fatalf("operator move: err=%v; want ErrMoveWhileCreating", err)
		}
	}
	if h.attest.awaitingFirstReady("vol-rf3") {
		t.Fatal("memory moved to stage 2 without the write")
	}
	h = restartCreatingMaster(t, h, cfg)
	if why := h.inCreationPhase("vol-rf3", line, nil); why == "" {
		t.Fatal("after a restart the unrecorded end released the volume")
	}
	if err := os.Remove(block); err != nil {
		t.Fatal(err)
	}
	// The write succeeds: stage 2, ended at once by r2's write (DurableLSN 70).
	if why := h.inCreationPhase("vol-rf3", line, nil); why != "" {
		t.Fatalf("after the write succeeded: still creating (%s)", why)
	}
	closeTestMaster(t, h)
}

// The primary's own answer alone holds a volume whose master record holds
// nothing (an established volume): its assignment at the current epoch
// still awaits the stores. The matching epoch holds on both gates and the
// operator's fenced path; a stale epoch holds nothing (the controls that
// the fixture would move). Removing the answer guard fails the holds.
func TestThePrimarysAwaitingAnswerAloneHoldsAnEstablishedVolume(t *testing.T) {
	for _, matching := range []bool{true, false} {
		name := map[bool]string{true: "matching epoch", false: "stale epoch"}[matching]
		setup := func(t *testing.T) (*Host, *switchProbe) {
			h := newBootGraceMaster(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
			if h.attest.awaitingFirstReady("vol-rf3") || inCreationWait(h) {
				t.Fatal("fixture: the record still holds the volume")
			}
			p := probeOf(t, h)
			epoch := waitAuthorityLine(t, h.Publisher(), "vol-rf3").Epoch
			if !matching {
				epoch++
			}
			p.mu.Lock()
			p.awaitingEpoch = epoch
			p.mu.Unlock()
			time.Sleep(250 * time.Millisecond) // past the boot grace
			return h, p
		}
		t.Run("unsupported gate/"+name, func(t *testing.T) {
			h, _ := setup(t)
			defer closeTestMaster(t, h)
			ineligible := restarting
			ineligible.eligible = false
			unsupportedTick(t, h, ineligible)
			if matching {
				expectLineStaysR1(t, h, "the primary's awaiting answer")
				return
			}
			expectFailoverToR2(t, h, "a stale awaiting answer")
		})
		t.Run("supported gate/"+name, func(t *testing.T) {
			h, p := setup(t)
			defer closeTestMaster(t, h)
			supportedCreatingTick(t, h, rf3Slot{incarnation: "r1-a", reachable: true, eligible: true})
			p.mu.Lock()
			fences := p.fenceCalls
			p.mu.Unlock()
			line, _ := h.Publisher().VolumeAuthorityLine("vol-rf3")
			moved := fences > 0 || line.ReplicaID != "r1"
			if matching && moved {
				t.Fatalf("the primary's awaiting answer: fences=%d line=%+v", fences, line)
			}
			if !matching && !moved {
				t.Fatal("control: a stale awaiting answer took no replacement decision")
			}
		})
		t.Run("operator's fenced path/"+name, func(t *testing.T) {
			h, p := setup(t)
			defer closeTestMaster(t, h)
			withOperatorProbe(h, p)
			ingestRF3Incarnations(t, h, silent, readySlot("r2-a"), readySlot("r3-a")) // the primary unreachable: the fenced path
			_, err := h.plannedMoveEvidence("vol-rf3", "r2")
			if matching && err != ErrMoveWhileCreating {
				t.Fatalf("err=%v; want ErrMoveWhileCreating", err)
			}
			if !matching && err == ErrMoveWhileCreating {
				t.Fatal("control: a stale awaiting answer refused the move as creating")
			}
		})
	}
}
