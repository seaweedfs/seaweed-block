package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// inCreationWait reports whether the master's store record still awaits a
// replica store of vol-rf3 (test fixtures only: the gate itself reads the
// primary's own answer).
func inCreationWait(h *Host) bool {
	waiting, _ := h.attest.awaited("vol-rf3", h.replicaSlotsFor("vol-rf3"))
	return waiting > 0
}

// awaitingProbe makes the current primary answer that it is not ready only
// because its assignment at the current line's epoch awaits replica stores
// (awaiting false: it does not say so).
func awaitingProbe(t *testing.T, h *Host, awaiting bool) *switchProbe {
	t.Helper()
	p := probeOf(t, h)
	var epoch uint64
	if awaiting {
		epoch = waitAuthorityLine(t, h.Publisher(), "vol-rf3").Epoch
	}
	p.mu.Lock()
	p.awaitingEpoch = epoch
	p.mu.Unlock()
	return p
}

// A new volume's primary serves nothing until its assignment carries every
// replica's store, and its answer says so ("awaiting replica stores"). The
// master must not take that for a failed primary: past its boot grace, with
// a ready survivor, the line stays.
func TestPrimaryAwaitingReplicaStoresIsNotReplacedForNotReady(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	awaitingProbe(t, h, true)
	time.Sleep(250 * time.Millisecond) // past the boot grace
	unsupportedTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "a primary awaiting replica stores answered not ready")
	unsupportedTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "still awaiting on the next tick")
}

// The real-path schedule: the master has recorded every store (its wait is
// over) but the primary has not received the re-sent assignment yet, so it
// still answers "awaiting replica stores"; the master restarts in that gap.
// The excuse is the primary's own answer, so nothing the master forgets on a
// restart reopens the gap: the line stays.
func TestMasterRestartInTheGapDoesNotReassign(t *testing.T) {
	cfg := Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(),
		ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}}
	h := newBootGraceMasterCreating(t, cfg)
	attestAllStores(t, h) // the master's wait is over
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
	h2.SetPromotionEvidenceProvider(&switchProbe{awaitingEpoch: waitAuthorityLine(t, h2.Publisher(), "vol-rf3").Epoch})
	if inCreationWait(h2) {
		t.Fatal("fixture: the restarted master still waits for stores")
	}
	time.Sleep(250 * time.Millisecond) // past the boot grace
	unsupportedTick(t, h2, readySlot("r1-a"))
	expectLineStaysR1(t, h2, "restarted master, primary still awaiting the re-sent stores")
}

// A primary that is not ready for another reason (it does not say it awaits
// replica stores) is replaced as before, also on a new volume.
func TestPrimaryNotReadyForAnotherReasonIsReplaced(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	awaitingProbe(t, h, false)
	if !inCreationWait(h) {
		t.Fatal("fixture: the new volume is not in its creation wait")
	}
	time.Sleep(250 * time.Millisecond) // past the boot grace
	unsupportedTick(t, h, readySlot("r1-a"))
	expectFailoverToR2(t, h, "a primary not ready without awaiting replica stores")
}

// The answer names its line: one that says it awaits replica stores for
// another epoch (a stale answer) excuses nothing, and the primary is
// replaced as one not ready for another reason.
func TestAwaitingAnswerForAnotherLineExcusesNothing(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	p := probeOf(t, h)
	p.mu.Lock()
	p.awaitingEpoch = waitAuthorityLine(t, h.Publisher(), "vol-rf3").Epoch + 1
	p.mu.Unlock()
	time.Sleep(250 * time.Millisecond) // past the boot grace
	unsupportedTick(t, h, readySlot("r1-a"))
	expectFailoverToR2(t, h, "an awaiting answer naming another line")
}

// The answer never shelters a failed primary: one whose own fresh slot is
// not acceptable (an unhealthy heartbeat makes it ineligible) is replaced
// through the fencing round, as for an established volume.
func TestPrimaryAwaitingReplicaStoresIsReplacedWhenItsSlotIsNotAcceptable(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{})
	defer closeTestMaster(t, h)
	awaitingProbe(t, h, true)
	ineligible := restarting
	ineligible.eligible = false
	unsupportedTick(t, h, ineligible)
	expectFailoverToR2(t, h, "an ineligible primary awaiting replica stores")
}

// attestSomeStores records r1 and r2 but not r3: the volume is still in its
// creation wait.
func attestSomeStores(t *testing.T, h *Host) {
	t.Helper()
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": testStoreID, "r2": testStoreID})); err != nil {
		t.Fatal(err)
	}
	if !inCreationWait(h) {
		t.Fatal("fixture: a partial attestation ended the creation wait")
	}
}

// Partial attestation (still creating), the primary answering that it awaits
// replica stores: a primary whose own slot is not acceptable is replaced;
// one that does not answer the probe is unknown and never replaced.
func TestCreationWaitPartialAttestationControls(t *testing.T) {
	t.Run("unhealthy primary (ineligible) is replaced", func(t *testing.T) {
		h := newBootGraceMasterCreating(t, Config{})
		defer closeTestMaster(t, h)
		attestSomeStores(t, h)
		awaitingProbe(t, h, true)
		ineligible := restarting
		ineligible.eligible = false
		unsupportedTick(t, h, ineligible)
		expectFailoverToR2(t, h, "an unhealthy primary inside the creation wait")
	})
	t.Run("expired primary (no fresh slot) is replaced", func(t *testing.T) {
		h := newBootGraceMasterCreating(t, Config{})
		defer closeTestMaster(t, h)
		attestSomeStores(t, h)
		awaitingProbe(t, h, true)
		unsupportedTick(t, h, silent)
		expectFailoverToR2(t, h, "an expired primary inside the creation wait")
	})
	t.Run("unanswered primary is never replaced", func(t *testing.T) {
		h := newBootGraceMasterCreating(t, Config{})
		defer closeTestMaster(t, h)
		attestSomeStores(t, h)
		p := awaitingProbe(t, h, true)
		p.mu.Lock()
		p.currentUnknown = true
		p.mu.Unlock()
		unsupportedTick(t, h, readySlot("r1-a"))
		expectLineStaysR1(t, h, "an unanswered primary inside the creation wait")
	})
}

// The realistic new volume: never written, so every end is zero and no
// frontier is known, with only r1 and r2 attested (still creating). What the
// code does, for each primary:
//   - unhealthy (ineligible) or expired: NOT promoted. With the required
//     frontier unknown, the RF3 quorum rule needs two fenced survivors and
//     only r2 is ready (r3 answers end 0, not ready), so nothing is
//     published: the line stays and the volume stays in its visible creation
//     wait (status: creating: waiting for replica attestations). The
//     operator recreates it (it has served nothing).
//   - unanswered: unknown, never failed: the line stays, in the same wait.
func TestCreationWaitEmptyVolumeOutcomes(t *testing.T) {
	ineligible := restarting
	ineligible.eligible = false
	for _, c := range []struct {
		name     string
		r1       rf3Slot
		unknown  bool
		promoted bool
	}{
		{"unhealthy primary: stays in the visible wait", ineligible, false, false},
		{"expired primary: stays in the visible wait", silent, false, false},
		{"unanswered primary: stays in the visible wait", readySlot("r1-a"), true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newBootGraceMasterCreating(t, Config{})
			defer closeTestMaster(t, h)
			attestSomeStores(t, h)
			p := awaitingProbe(t, h, true)
			p.mu.Lock()
			p.empty, p.currentUnknown = true, c.unknown
			p.mu.Unlock()
			unsupportedTick(t, h, c.r1)
			if c.promoted {
				expectFailoverToR2(t, h, c.name)
				return
			}
			expectLineStaysR1(t, h, c.name)
			if !inCreationWait(h) {
				t.Fatal("the volume left its creation wait without the missing store")
			}
		})
	}
}
