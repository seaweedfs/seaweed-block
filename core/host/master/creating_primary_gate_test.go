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
	creatingTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "a primary awaiting replica stores answered not ready")
	creatingTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "still awaiting on the next tick")
}

// The real-path schedule: the master has recorded every store (its wait is
// over) but the primary has not received the re-sent assignment yet, so it
// still answers "awaiting replica stores"; the master restarts in that gap.
// Creation ends only at the primary's first ready answer, recorded durably,
// so the restarted master still holds the volume: no decision, the line
// stays.
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
	// The primary still awaits the stores: it served nothing, so no survivor
	// holds a write (every end is zero).
	h2.SetPromotionEvidenceProvider(&switchProbe{awaitingEpoch: waitAuthorityLine(t, h2.Publisher(), "vol-rf3").Epoch, empty: true})
	if inCreationWait(h2) {
		t.Fatal("fixture: the restarted master still waits for stores")
	}
	time.Sleep(250 * time.Millisecond) // past the boot grace
	creatingTick(t, h2, readySlot("r1-a"))
	expectLineStaysR1(t, h2, "restarted master, primary still awaiting the re-sent stores")
}

// Creation is its own phase (decided on the master's record): a creating
// volume's primary is not replaced even when it is not ready for another
// reason (it does not say it awaits replica stores). No decision is made at
// all: the gate does not even probe.
func TestCreatingPrimaryNotReadyForAnotherReasonIsNotReplaced(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	awaitingProbe(t, h, false)
	if !inCreationWait(h) {
		t.Fatal("fixture: the new volume is not in its creation wait")
	}
	time.Sleep(250 * time.Millisecond) // past the boot grace
	creatingTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "a primary not ready without awaiting replica stores")
}

// The phase is the master's record, not the answer: a stale awaiting answer
// (naming another epoch) changes nothing while the record still awaits.
func TestCreatingVolumeWithAStaleAwaitingAnswerIsNotReplaced(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	p := probeOf(t, h)
	p.mu.Lock()
	p.awaitingEpoch = waitAuthorityLine(t, h.Publisher(), "vol-rf3").Epoch + 1
	p.mu.Unlock()
	time.Sleep(250 * time.Millisecond) // past the boot grace
	creatingTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "an awaiting answer naming another line")
}

// A creating primary whose own slot is not acceptable (ineligible) is not
// replaced either: a creating volume that is really broken stays in its
// visible wait, and the operator deletes and recreates it (it served
// nothing).
func TestCreatingPrimaryWithAnUnacceptableSlotIsNotReplaced(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{})
	defer closeTestMaster(t, h)
	awaitingProbe(t, h, true)
	ineligible := restarting
	ineligible.eligible = false
	creatingTick(t, h, ineligible)
	expectLineStaysR1(t, h, "an ineligible primary awaiting replica stores")
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

// Partial attestation (still creating): whatever the primary's state
// (ineligible, expired, unanswered), no replacement decision is made.
func TestCreationWaitPartialAttestationControls(t *testing.T) {
	t.Run("unhealthy primary (ineligible) is not replaced", func(t *testing.T) {
		h := newBootGraceMasterCreating(t, Config{})
		defer closeTestMaster(t, h)
		attestSomeStores(t, h)
		awaitingProbe(t, h, true)
		ineligible := restarting
		ineligible.eligible = false
		creatingTick(t, h, ineligible)
		expectLineStaysR1(t, h, "an unhealthy primary inside the creation wait")
	})
	t.Run("expired primary (no fresh slot) is not replaced", func(t *testing.T) {
		h := newBootGraceMasterCreating(t, Config{})
		defer closeTestMaster(t, h)
		attestSomeStores(t, h)
		awaitingProbe(t, h, true)
		creatingTick(t, h, silent)
		expectLineStaysR1(t, h, "an expired primary inside the creation wait")
	})
	t.Run("unanswered primary is never replaced", func(t *testing.T) {
		h := newBootGraceMasterCreating(t, Config{})
		defer closeTestMaster(t, h)
		attestSomeStores(t, h)
		p := awaitingProbe(t, h, true)
		p.mu.Lock()
		p.currentUnknown = true
		p.mu.Unlock()
		creatingTick(t, h, readySlot("r1-a"))
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
			creatingTick(t, h, c.r1)
			if c.promoted {
				expectLineStaysR1(t, h, c.name)
				return
			}
			expectLineStaysR1(t, h, c.name)
			if !inCreationWait(h) {
				t.Fatal("the volume left its creation wait without the missing store")
			}
		})
	}
}

// creatingTick ingests fresh r1/r2 facts (node-c silent), runs one product
// tick on the unsupported-volume path, and requires that no promotion
// decision was even probed: a creating volume is outside every replacement
// decision.
func creatingTick(t *testing.T, h *Host, r1 rf3Slot) {
	t.Helper()
	ingestRF3Incarnations(t, h, r1, readySlot("r2-a"), silent)
	res, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if res.PromotionProbes != 0 {
		t.Fatalf("product tick %+v: a creating volume was probed for a promotion decision", res)
	}
}
