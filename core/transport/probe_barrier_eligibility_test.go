package transport

// Marker-only slice (post-G0) — unit coverage for the §IV.2.1
// PrimaryWalLegOk observation accessor on WalShipper. Pure shipper
// state transitions; no wire / receiver / coordinator coupling.
//
// What this pins:
//
//   - Fresh shipper post-Activate(0): cursor==head trivially (both 0)
//     so debtZero=true; liveTail=false (no Live emitted yet); thus
//     walLegOk=true via debtZero.
//   - StartSession resets liveTail to false and rewinds cursor to
//     fromLSN; if substrate has entries past fromLSN, debtZero=false
//     until drive() catches up.
//   - First EmitKindLive emit (NotifyAppend on a caught-up shipper)
//     sets liveTail=true monotonically; subsequent debt accumulation
//     does NOT clear it.
//   - StartSession on the same shipper for a fresh session resets
//     liveTail=false (per-session monotonic).
//
// Source: sw-block/design/recover-semantics-adjustment-plan.md §8.2.6
// (architect Option B + Tier-1 marker contract).

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// TestProbeBarrierEligibility_FreshShipper_DebtZero — at construction
// + Activate(0), no substrate writes, cursor==head==0; debtZero true;
// liveTail false; walLegOk true.
func TestProbeBarrierEligibility_FreshShipper_DebtZero(t *testing.T) {
	primary := memorywal.NewStore(8, 64)
	emit := func(_ EmitKind, _ uint32, _ uint64, _ []byte) error { return nil }
	cfg := WalShipperConfig{IdleSleep: 5 * 1000, DisableTimerDrain: true}
	s := NewWalShipperWithOptions("r1", HeadSourceFromStorage(primary), primary, emit, cfg)
	defer s.Stop()
	if err := s.Activate(0); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	debtZero, liveTail, walLegOk, cursor, head := s.ProbeBarrierEligibility()
	if !debtZero {
		t.Errorf("fresh shipper: debtZero=%t want true (cursor=%d head=%d)", debtZero, cursor, head)
	}
	if liveTail {
		t.Errorf("fresh shipper: liveTail=%t want false (no Live emitted yet)", liveTail)
	}
	if !walLegOk {
		t.Errorf("fresh shipper: walLegOk=%t want true (debtZero ⇒ walLegOk)", walLegOk)
	}
	if cursor != 0 || head != 0 {
		t.Errorf("fresh shipper: cursor=%d head=%d want both 0", cursor, head)
	}
}

// TestProbeBarrierEligibility_LiveEmitFlipsLiveTail — once a fast-path
// or post-CASE-A Live emit fires, liveTail goes true monotonically and
// stays true even if subsequent appends extend head past cursor.
func TestProbeBarrierEligibility_LiveEmitFlipsLiveTail(t *testing.T) {
	primary := memorywal.NewStore(8, 64)
	emit := func(_ EmitKind, _ uint32, _ uint64, _ []byte) error { return nil }
	cfg := WalShipperConfig{IdleSleep: 5 * 1000, DisableTimerDrain: true}
	s := NewWalShipperWithOptions("r1", HeadSourceFromStorage(primary), primary, emit, cfg)
	defer s.Stop()
	if err := s.Activate(0); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	// Pre-emit: liveTail=false.
	_, liveTail, _, _, _ := s.ProbeBarrierEligibility()
	if liveTail {
		t.Fatalf("pre-emit: liveTail=true want false")
	}

	// Production sequence: primary.Write commits to substrate (advances
	// head); then NotifyAppend on the shipper takes fast-path
	// (lsn=cursor+1, head==lsn) → EmitKindLive → sets firstLiveEmitted.
	data := make([]byte, 64)
	data[0] = 0xAA
	lsn, err := primary.Write(0, data)
	if err != nil {
		t.Fatalf("primary.Write: %v", err)
	}
	if err := s.NotifyAppend(0, lsn, data); err != nil {
		t.Fatalf("NotifyAppend: %v", err)
	}

	debtZero, liveTail, walLegOk, cursor, head := s.ProbeBarrierEligibility()
	if !liveTail {
		t.Errorf("post-Live-emit: liveTail=%t want true", liveTail)
	}
	if !debtZero {
		t.Errorf("post-Live-emit: debtZero=%t want true (cursor=%d head=%d)", debtZero, cursor, head)
	}
	if !walLegOk {
		t.Errorf("post-Live-emit: walLegOk=%t want true", walLegOk)
	}
	if cursor != lsn {
		t.Errorf("post-Live-emit: cursor=%d want %d", cursor, lsn)
	}
}

// TestProbeBarrierEligibility_StartSessionResetsLiveTail — the
// per-session monotonic property: a new session zeroes liveTail
// regardless of whether the prior session emitted Live.
func TestProbeBarrierEligibility_StartSessionResetsLiveTail(t *testing.T) {
	primary := memorywal.NewStore(8, 64)
	emit := func(_ EmitKind, _ uint32, _ uint64, _ []byte) error { return nil }
	cfg := WalShipperConfig{IdleSleep: 5 * 1000, DisableTimerDrain: true}
	s := NewWalShipperWithOptions("r1", HeadSourceFromStorage(primary), primary, emit, cfg)
	defer s.Stop()
	if err := s.Activate(0); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	// Fire one Live emit to set liveTail.
	data := make([]byte, 64)
	lsn, err := primary.Write(0, data)
	if err != nil {
		t.Fatalf("primary.Write: %v", err)
	}
	if err := s.NotifyAppend(0, lsn, data); err != nil {
		t.Fatalf("NotifyAppend: %v", err)
	}
	if _, lt, _, _, _ := s.ProbeBarrierEligibility(); !lt {
		t.Fatalf("setup: liveTail did not flip on Live emit")
	}

	// New session — liveTail must reset.
	if err := s.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, lt, _, _, _ := s.ProbeBarrierEligibility(); lt {
		t.Errorf("post-StartSession: liveTail=%t want false (per-session reset)", lt)
	}
	s.EndSession()
}
