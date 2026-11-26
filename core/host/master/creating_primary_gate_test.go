package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A new volume's primary hides its frontend until its replicas' stores are
// recorded (the creation wait), so it answers the promotion probe "not
// primary-ready" by design. The master must not take that for a failed
// primary: past its boot grace, with a ready survivor, the line stays.
// (Real-path symptom: r1 adopted at epoch 1, the master moved the primary
// to r2 at epoch 2 while r1 was still learning its replicas' stores.)
func TestPrimaryInTheCreationWaitIsNotReplacedForNotReady(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	if waiting, _ := h.attest.awaited("vol-rf3", h.replicaSlotsFor("vol-rf3")); waiting == 0 {
		t.Fatal("fixture: the new volume is not in its creation wait")
	}
	time.Sleep(250 * time.Millisecond) // past the boot grace
	unsupportedTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "a primary in the creation wait answered not ready")
	unsupportedTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "still in the creation wait on the next tick")
}

// The creation wait never shelters a failed primary: one whose own fresh
// slot is not acceptable (an unhealthy heartbeat makes it ineligible) is
// replaced through the fencing round, as for an established volume.
func TestPrimaryInTheCreationWaitIsReplacedWhenItsSlotIsNotAcceptable(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{})
	defer closeTestMaster(t, h)
	if !h.inCreationWait("vol-rf3") {
		t.Fatal("fixture: the new volume is not in its creation wait")
	}
	ineligible := restarting
	ineligible.eligible = false
	unsupportedTick(t, h, ineligible)
	expectFailoverToR2(t, h, "an ineligible primary inside the creation wait")
}

// The wait clears while the probe is held (the master records the last
// store meanwhile): the primary's not-ready answer was given inside the
// wait and is judged as the wait, so this round does not fence it.
func TestCreationWaitClearingDuringTheProbeDoesNotFence(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	p := probeOf(t, h)
	time.Sleep(250 * time.Millisecond) // past the boot grace
	heldTick(t, h, readySlot("r1-a"), func() {
		attestAllStores(t, h)
		// r1 keeps heartbeating while the probe runs: its slot stays
		// acceptable, so only the wait reading decides this round.
		ingestRF3Incarnations(t, h, readySlot("r1-a"), readySlot("r2-a"), silent)
	})
	if n := func() int { p.mu.Lock(); defer p.mu.Unlock(); return p.fenceCalls }(); n != 0 {
		t.Fatalf("%d fencing rounds on a not-ready answer given inside the creation wait", n)
	}
	expectLineStaysR1(t, h, "the wait cleared during the probe")
}

// attestSomeStores records r1 and r2 but not r3: the volume is still in its
// creation wait.
func attestSomeStores(t *testing.T, h *Host) {
	t.Helper()
	line := waitAuthorityLine(t, h.Publisher(), "vol-rf3")
	if err := h.ingestStoreAttestation("node-a", attestation("vol-rf3", "r1", line.Epoch, map[string]storage.StoreID{"r1": testStoreID, "r2": testStoreID})); err != nil {
		t.Fatal(err)
	}
	if !h.inCreationWait("vol-rf3") {
		t.Fatal("fixture: a partial attestation ended the creation wait")
	}
}

// Partial attestation (still creating): a primary whose own slot is not
// acceptable is replaced; one that does not answer the probe is unknown and
// never replaced.
func TestCreationWaitPartialAttestationControls(t *testing.T) {
	t.Run("unhealthy primary (ineligible) is replaced", func(t *testing.T) {
		h := newBootGraceMasterCreating(t, Config{})
		defer closeTestMaster(t, h)
		attestSomeStores(t, h)
		ineligible := restarting
		ineligible.eligible = false
		unsupportedTick(t, h, ineligible)
		expectFailoverToR2(t, h, "an unhealthy primary inside the creation wait")
	})
	t.Run("expired primary (no fresh slot) is replaced", func(t *testing.T) {
		h := newBootGraceMasterCreating(t, Config{})
		defer closeTestMaster(t, h)
		attestSomeStores(t, h)
		unsupportedTick(t, h, silent)
		expectFailoverToR2(t, h, "an expired primary inside the creation wait")
	})
	t.Run("unanswered primary is never replaced", func(t *testing.T) {
		h := newBootGraceMasterCreating(t, Config{})
		defer closeTestMaster(t, h)
		attestSomeStores(t, h)
		p := probeOf(t, h)
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
			p := probeOf(t, h)
			p.mu.Lock()
			p.empty, p.currentUnknown = true, c.unknown
			p.mu.Unlock()
			unsupportedTick(t, h, c.r1)
			if c.promoted {
				expectFailoverToR2(t, h, c.name)
				return
			}
			expectLineStaysR1(t, h, c.name)
			if !h.inCreationWait("vol-rf3") {
				t.Fatal("the volume left its creation wait without the missing store")
			}
		})
	}
}

// The NET bring-up schedule: the wait clears (every store recorded) a
// moment before the primary opens its frontend (it opens only when the
// re-sent assignment reaches it). On a line that began in the wait, its
// not-ready answer is still the start, not a failure, until it has answered
// ready once on the line; after that a not-ready primary is replaced as
// usual. The primary's own slot reports ready for primary (its engine is
// ready), so the boot grace does not hold here.
func TestCreationLineIsExcusedUntilItsPrimaryAnsweredReadyOnce(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	p := probeOf(t, h)
	time.Sleep(250 * time.Millisecond) // past the boot grace
	unsupportedTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "inside the creation wait")
	attestAllStores(t, h) // the wait clears
	unsupportedTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "the wait cleared and the primary has not opened its frontend yet")
	p.setCurrentReady(true)
	unsupportedTick(t, h, readySlot("r1-a"))
	expectLineStaysR1(t, h, "the primary answered ready")
	p.setCurrentReady(false)
	unsupportedTick(t, h, readySlot("r1-a"))
	expectFailoverToR2(t, h, "an established primary that served on its line, now not ready")
}
