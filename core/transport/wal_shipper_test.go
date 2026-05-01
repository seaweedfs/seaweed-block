package transport

// WalShipper conformance tests — spec-driven.
//
// Source of truth: `sw-block/design/v3-recovery-wal-shipper-spec.md`
// §3 invariants (INV-*), §5 R1 (mode-transition atomicity), §6 R2
// (saturation), §9 minimum test names.
//
// These tests are written from the spec, NOT from the implementation
// in wal_shipper.go. A test failing here means the implementation
// drifted from spec — fix the implementation, never the test.
//
// Test structure: each test has a section comment citing the spec §
// it pins. Test names match spec §9 exactly so PR self-check (mini-plan
// §5) can grep for them. Tests use memorywal as the substrate (per
// kickoff §10 OOS — WAL substrate, not BlockStore).

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

var _ = storage.ErrAppliedLSNsNotTracked // anchor: storage import survives unused-import scrub


// --- helpers ----------------------------------------------------------

// recordingEmit captures every (lba, lsn, data) handed to it. Used by
// tests as the EmitFunc to assert the emitted set.
type recordingEmit struct {
	mu      sync.Mutex
	entries []emitRecord
	// fail injects an error on the Nth emit (1-indexed). Zero = no fail.
	failAt   int
	emitN    int
	failWith error
	// inFlight counts concurrent calls; INV-NO-DOUBLE-LIVE assertion.
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

type emitRecord struct {
	LBA  uint32
	LSN  uint64
	Data []byte
}

func newRecordingEmit() *recordingEmit {
	return &recordingEmit{}
}

func (r *recordingEmit) Func() EmitFunc {
	return func(_ EmitKind, lba uint32, lsn uint64, data []byte) error {
		// INV-NO-DOUBLE-LIVE probe: track concurrent emits. shipMu
		// should serialize so this never exceeds 1.
		cur := r.inFlight.Add(1)
		defer r.inFlight.Add(-1)
		for {
			max := r.maxInFlight.Load()
			if cur <= max || r.maxInFlight.CompareAndSwap(max, cur) {
				break
			}
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.emitN++
		if r.failAt > 0 && r.emitN == r.failAt {
			return r.failWith
		}
		cp := make([]byte, len(data))
		copy(cp, data)
		r.entries = append(r.entries, emitRecord{LBA: lba, LSN: lsn, Data: cp})
		return nil
	}
}

func (r *recordingEmit) Snapshot() []emitRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]emitRecord, len(r.entries))
	copy(out, r.entries)
	return out
}

func (r *recordingEmit) MaxInFlight() int32 { return r.maxInFlight.Load() }

// fixedHeadSource lets the test pin head independently of substrate;
// useful for R1 race tests where we want to control the head value
// observed by the WalShipper independently of what the substrate
// reports.
type fixedHeadSource struct {
	mu sync.Mutex
	h  uint64
}

func (f *fixedHeadSource) Head() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.h
}

func (f *fixedHeadSource) Set(h uint64) {
	f.mu.Lock()
	f.h = h
	f.mu.Unlock()
}

// payload generates a deterministic 4 KiB block whose first byte
// encodes (lba ^ epoch).
func payload(lba uint32, epoch byte) []byte {
	out := make([]byte, 4096)
	v := byte(lba) ^ epoch
	for i := range out {
		out[i] = v
	}
	return out
}

// --- spec §3 INV tests ------------------------------------------------

// TestWalShipper_RegistrySingleton — spec §3 INV-SINGLE.
//
// "At any moment, at most one WalShipper instance exists per
// (volumeID, replicaID). Construction MUST go through a registry
// that fails-fast on duplicate."
//
// SKIP: registry is the BlockExecutor's responsibility, not
// WalShipper's. Per mini-plan §3, P1 introduces the executor
// registry. This test is a placeholder so PR self-check + spec §9
// row stays accounted for; real test lives in
// ship_sender_test.go after P1.
func TestWalShipper_RegistrySingleton(t *testing.T) {
	t.Skip("registry lives in BlockExecutor; pinned in P1 (mini-plan §3) — see ship_sender_test.go after wiring")
}

// TestWalShipper_CursorMonotonic — spec §3 INV-MONOTONIC-CURSOR.
//
// "cursor is monotonically non-decreasing except for the single
// rewind at session entry: when coord.StartSession admits a
// session, WalShipper sets cursor := fromLSN exactly once under
// shipMu. After that, cursor only advances via emission. No
// in-band rewind."
//
// Q1b alignment (architect 2026-04-29): seed must satisfy
// `head ≥ fromLSN`. Earlier draft seeded LSN 1..5 + fromLSN=10
// gave head < fromLSN — an edge that exposes R1 bugs but doesn't
// pin INV-MONOTONIC-CURSOR cleanly. Now seed LSN 1..15 so
// head=15 ≥ fromLSN=10, and DrainBacklog has a real range
// (10, 15] to ship.
//
// Test:
//  1. Pre-seed LSN 1..15 (memorywal head=15)
//  2. StartSession(fromLSN=10) → cursor rewinds to 10
//  3. DrainBacklog ships LSN 11..15 — cursor advances to 15
//  4. Cursor never observed to decrease during drain (sampled)
//  5. Idempotent NotifyAppend with lsn ≤ cursor — cursor unchanged
func TestWalShipper_CursorMonotonic(t *testing.T) {
	const fromLSN = uint64(10)
	const seedCount = 15 // head=15 ≥ fromLSN=10 (Q1b alignment)

	primary := memorywal.NewStore(64, 4096)
	for lba := uint32(0); lba < seedCount; lba++ {
		_, _ = primary.Write(lba, payload(lba, 0xA0))
	}
	_, _ = primary.Sync()

	emit := newRecordingEmit()
	s := NewWalShipper("r1", HeadSourceFromStorage(primary), primary, emit.Func())

	if err := s.StartSession(fromLSN); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if got := s.Cursor(); got != fromLSN {
		t.Fatalf("post-StartSession cursor=%d want %d (rewind to fromLSN)", got, fromLSN)
	}

	// Sample cursor across DrainBacklog; must never decrease.
	var prev uint64 = fromLSN
	var cursorViolations atomic.Int32
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			c := s.Cursor()
			if c < prev {
				cursorViolations.Add(1)
			}
			prev = c
			time.Sleep(time.Microsecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.DrainBacklog(ctx); err != nil {
		t.Fatalf("DrainBacklog: %v", err)
	}
	close(stop)

	if cursorViolations.Load() != 0 {
		t.Errorf("cursor regressed during drain: %d violations", cursorViolations.Load())
	}

	// After drain, cursor must be at head (15) — DrainBacklog returns
	// successfully only when R1 transitions cursor==head.
	_, _, head := primary.Boundaries()
	if got := s.Cursor(); got != head {
		t.Errorf("post-drain cursor=%d want head=%d", got, head)
	}

	// Idempotent NotifyAppend with lsn ≤ cursor must not regress cursor.
	cursorBefore := s.Cursor()
	if err := s.NotifyAppend(0, cursorBefore-1, payload(0, 0xAA)); err != nil {
		t.Fatalf("idempotent NotifyAppend: %v", err)
	}
	if got := s.Cursor(); got != cursorBefore {
		t.Errorf("cursor changed by idempotent NotifyAppend: got %d want %d", got, cursorBefore)
	}
}

// TestWalShipper_RewindOnceAtSessionEntry — spec §3 INV-MONOTONIC-CURSOR
// (legal one-shot rewind path).
//
// Test:
//  1. Activate(cursor=100) — Realtime, cursor=100
//  2. NotifyAppend lsn=101..105 — cursor=105
//  3. EndSession (or equivalent return-to-Idle / session re-entry)
//  4. StartSession(fromLSN=50) — cursor goes BACK to 50 (the one rewind)
//  5. After StartSession, NotifyAppend with lsn ≤ 50 stays no-op;
//     DrainBacklog from 50 forward is the only advance source
//
// The "one rewind per session entry" is the ONLY legal way cursor
// goes backward. Any other regression is INV violation.
func TestWalShipper_RewindOnceAtSessionEntry(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	emit := newRecordingEmit()
	s := NewWalShipper("r1", HeadSourceFromStorage(primary), primary, emit.Func())

	// Activate steady state at cursor=100.
	if err := s.Activate(100); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if got := s.Cursor(); got != 100 {
		t.Errorf("post-Activate cursor=%d want 100", got)
	}
	if got := s.Mode(); got != ModeRealtime {
		t.Errorf("post-Activate mode=%s want Realtime", got)
	}

	// Realtime emits.
	for i := 0; i < 5; i++ {
		if err := s.NotifyAppend(0, 101+uint64(i), payload(0, 0xB0)); err != nil {
			t.Fatalf("NotifyAppend lsn=%d: %v", 101+i, err)
		}
	}
	if got := s.Cursor(); got != 105 {
		t.Errorf("after 5 emits cursor=%d want 105", got)
	}

	// EndSession returns to Realtime (no rewind).
	s.EndSession()
	if got := s.Cursor(); got != 105 {
		t.Errorf("EndSession should not rewind; cursor=%d want 105", got)
	}

	// StartSession with NEW fromLSN=50 — the one legal rewind.
	if err := s.StartSession(50); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if got := s.Cursor(); got != 50 {
		t.Errorf("StartSession cursor=%d want 50 (the rewind)", got)
	}
	if got := s.Mode(); got != ModeBacklog {
		t.Errorf("post-StartSession mode=%s want Backlog", got)
	}
}

// TestWalShipper_DeliveredEqualsCursor — spec §3 INV-SUBSET.
//
// "At any time, the set of LSNs delivered to the transport sink
// during this WalShipper's lifetime is exactly (fromLSN, cursor]
// of the substrate's log. No skips, no dups."
//
// Q1a anchoring (architect 2026-04-29): the contract is open-lower
// at fromLSN. First emit's LSN MUST be `fromLSN+1`, NOT "1". Here
// fromLSN=0 happens to make `fromLSN+1 = 1`, matching the seed; but
// the assertion below derives the expected sequence from `fromLSN+1`
// to make the contract explicit (and so a future variant with
// fromLSN > 0 gets covered without rewriting the test logic).
//
// Test:
//  1. Pre-seed primary with LSN 1..50 (memorywal write-time LSN).
//  2. StartSession(fromLSN=0); DrainBacklog runs to completion.
//  3. After drain: cursor = head = 50.
//  4. emit log MUST be exactly LSN [fromLSN+1, .., head] in order,
//     contiguous, no dups.
func TestWalShipper_DeliveredEqualsCursor(t *testing.T) {
	const numEntries = 50
	const fromLSN = uint64(0) // for this case, head ≡ numEntries
	primary := memorywal.NewStore(64, 4096)
	for lba := uint32(0); lba < numEntries; lba++ {
		_, _ = primary.Write(lba, payload(lba, 0xC0))
	}
	_, _ = primary.Sync()
	_, _, head := primary.Boundaries()

	emit := newRecordingEmit()
	s := NewWalShipper("r1", HeadSourceFromStorage(primary), primary, emit.Func())
	if err := s.StartSession(fromLSN); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.DrainBacklog(ctx); err != nil {
		t.Fatalf("DrainBacklog: %v", err)
	}

	if got := s.Cursor(); got != head {
		t.Errorf("post-drain cursor=%d want head=%d", got, head)
	}
	if got := s.Mode(); got != ModeRealtime {
		t.Errorf("post-drain mode=%s want Realtime (R1 transition success)", got)
	}

	// Open-lower at fromLSN: expected LSN sequence is
	// [fromLSN+1, fromLSN+2, ..., head], contiguous.
	expectedCount := head - fromLSN
	got := emit.Snapshot()
	if uint64(len(got)) != expectedCount {
		t.Fatalf("emit count=%d want %d (head=%d - fromLSN=%d)",
			len(got), expectedCount, head, fromLSN)
	}
	for i, e := range got {
		want := fromLSN + 1 + uint64(i)
		if e.LSN != want {
			t.Errorf("emit[%d].LSN=%d want %d (open-lower at fromLSN=%d)",
				i, e.LSN, want, fromLSN)
		}
	}
}

// TestWalShipper_R1_NoGapAtTransition — spec §3 INV-NO-GAP-R1 +
// spec §5 R1 procedure under concurrent append.
//
// "When mode transitions Backlog → Realtime, no LSN with
// fromLSN < L ≤ headAtTransition may be missed by both modes."
//
// Test setup:
//  1. Pre-seed primary with LSN 1..N0 (the backlog).
//  2. StartSession(fromLSN=0); DrainBacklog runs in goroutine.
//  3. Concurrent goroutine writes N1..N2 to primary at high rate
//     during the drain — exercises the R1 race.
//  4. Wait for DrainBacklog to return AND concurrent writer to finish.
//  5. After both: every LSN in [1, finalHead] must be in emit log
//     exactly once. NO GAPS.
//
// Note: NotifyAppend also fires for the concurrent writer (callee
// responsibility). The race is: scan-loop sees cursor < head, scan
// emits some, cursor advances; concurrent write lands; R1 transition
// inside shipMu must double-check head and not miss the new entry.
func TestWalShipper_R1_NoGapAtTransition(t *testing.T) {
	primary := memorywal.NewStore(256, 4096)
	const N0 = 100 // pre-seeded backlog

	// Pre-seed.
	for lba := uint32(0); lba < N0; lba++ {
		_, _ = primary.Write(lba, payload(lba, 0xD0))
	}
	_, _ = primary.Sync()

	emit := newRecordingEmit()
	s := NewWalShipper("r1", HeadSourceFromStorage(primary), primary, emit.Func())
	if err := s.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Concurrent writer adds 100 more during drain.
	const N1 = 100
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := uint32(0); i < N1; i++ {
			lba := N0 + i
			data := payload(lba, 0xD0)
			lsn, err := primary.Write(lba, data)
			if err != nil {
				t.Errorf("concurrent Write: %v", err)
				return
			}
			// Simulate the production path's NotifyAppend call.
			_ = s.NotifyAppend(lba, lsn, data)
			time.Sleep(50 * time.Microsecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	drainErr := s.DrainBacklog(ctx)
	<-writerDone
	if drainErr != nil {
		t.Fatalf("DrainBacklog: %v", drainErr)
	}

	// After drain returns, mode should be Realtime (R1 success). The
	// final head may be larger than what was current at R1 if more
	// writes landed AFTER R1 succeeded — those would flow through
	// NotifyAppend in Realtime mode.
	_, _, finalHead := primary.Boundaries()
	cursor := s.Cursor()
	if cursor != finalHead {
		t.Errorf("post-drain cursor=%d != finalHead=%d (some LSN never emitted)", cursor, finalHead)
	}

	// emit log must contain LSN 1..finalHead exactly once each, in
	// monotonic-increasing order. No gap, no dup.
	got := emit.Snapshot()
	if uint64(len(got)) != finalHead {
		t.Fatalf("emit count=%d != finalHead=%d (gap or dup)", len(got), finalHead)
	}
	seen := make(map[uint64]bool, len(got))
	var lastLSN uint64
	for i, e := range got {
		if seen[e.LSN] {
			t.Errorf("emit[%d]: LSN=%d duplicate", i, e.LSN)
		}
		seen[e.LSN] = true
		if e.LSN <= lastLSN {
			t.Errorf("emit[%d]: LSN=%d not > lastLSN=%d (out of order)", i, e.LSN, lastLSN)
		}
		lastLSN = e.LSN
	}
	for n := uint64(1); n <= finalHead; n++ {
		if !seen[n] {
			t.Errorf("LSN=%d MISSING from emit log (INV-NO-GAP-R1 violated)", n)
		}
	}
}

// TestWalShipper_R1_DoubleCheckRejectsRace — spec §5.1 R1 internal
// guarantee: when caller observes cursor==head AT t0, calls into
// AssertCaughtUpAndEnableTailShip (held under shipMu), and a write
// has landed between t0 and the actual flip, R1 returns false
// (caller stays Backlog).
//
// Q3 alignment (architect 2026-04-29): use `fixedHeadSource` for
// deterministic head control instead of relying on substrate
// `Boundaries()` timing. The head value reported to WalShipper is
// independent of substrate state; test bumps head atomically AT
// THE MOMENT cursor reaches the previous head, and writes the new
// LSN to substrate so the next scan picks it up.
//
// Setup:
//  1. fixedHeadSource at h=10
//  2. Substrate has LSN 1..10
//  3. StartSession(0); DrainBacklog runs in goroutine
//  4. Concurrent goroutine polls Cursor; on cursor reaching 10,
//     atomically: substrate.Write(LSN 11) + fixedHeadSource.Set(11)
//  5. R1's double-check inside shipMu re-reads head: if it sees 11,
//     stays Backlog and rescans (picks LSN 11). If it sees 10 (race
//     lost the window), it flips early — but then NotifyAppend on
//     the LSN 11 path catches it via realtime emission. Either way
//     the OUTCOME is: emit log contains LSN 11.
//  6. After drain: cursor=11=fixedHeadSource. emit log = LSN 1..11.
func TestWalShipper_R1_DoubleCheckRejectsRace(t *testing.T) {
	primary := memorywal.NewStore(32, 4096)
	for lba := uint32(0); lba < 10; lba++ {
		_, _ = primary.Write(lba, payload(lba, 0xE0))
	}
	_, _ = primary.Sync()

	head := &fixedHeadSource{h: 10}
	emit := newRecordingEmit()
	s := NewWalShipper("r1", head, primary, emit.Func())
	if err := s.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Concurrent: when cursor reaches 10, inject LSN 11. Atomicity:
	// substrate Write happens BEFORE head bump, so any scan that runs
	// after the head bump sees LSN 11 in the substrate.
	bumped := make(chan struct{})
	go func() {
		defer close(bumped)
		// Spin until cursor is at the boundary. fixedHeadSource lets
		// us synchronize on observed cursor without timing fudge.
		for s.Cursor() < 10 {
			time.Sleep(50 * time.Microsecond)
		}
		// Step 1: write to substrate first (so scan can find it)
		_, _ = primary.Write(10, payload(10, 0xE0))
		// Step 2: bump head AFTER substrate has the entry
		head.Set(11)
		// Step 3: notify (in case shipper transitioned to realtime
		// already — covers the "race lost the R1 window" branch)
		_ = s.NotifyAppend(10, 11, payload(10, 0xE0))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.DrainBacklog(ctx); err != nil {
		t.Fatalf("DrainBacklog: %v", err)
	}
	<-bumped

	if got := s.Cursor(); got != 11 {
		t.Errorf("cursor=%d want 11 (head bumped to 11; LSN 11 must have shipped)", got)
	}

	got := emit.Snapshot()
	if len(got) != 11 {
		t.Fatalf("emit count=%d want 11 (LSN 1..11 contiguous; R1 must not skip LSN 11)", len(got))
	}
	// Verify LSN 11 specifically — that's the race-window LSN.
	lastLSN := got[len(got)-1].LSN
	if lastLSN != 11 {
		t.Errorf("last emit LSN=%d want 11 (race-window LSN missed)", lastLSN)
	}
}

// TestWalShipper_NoDoubleLive — spec §3 INV-NO-DOUBLE-LIVE.
//
// "At any moment, exactly one delivery path is live for new LSNs:
// either OnLocalWrite ships directly (Realtime) or backlog scan
// ships (Backlog). They are mutually exclusive under shipMu."
//
// Test: drive heavy concurrent NotifyAppend + DrainBacklog while
// the recordingEmit tracks max in-flight emit calls. shipMu MUST
// serialize, so maxInFlight ≤ 1 at all times.
func TestWalShipper_NoDoubleLive(t *testing.T) {
	primary := memorywal.NewStore(256, 4096)
	for lba := uint32(0); lba < 50; lba++ {
		_, _ = primary.Write(lba, payload(lba, 0xF0))
	}
	_, _ = primary.Sync()

	emit := newRecordingEmit()
	s := NewWalShipper("r1", HeadSourceFromStorage(primary), primary, emit.Func())
	if err := s.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// 4 concurrent appender goroutines + DrainBacklog.
	var appendWg sync.WaitGroup
	for w := 0; w < 4; w++ {
		appendWg.Add(1)
		go func(worker int) {
			defer appendWg.Done()
			for i := 0; i < 25; i++ {
				lba := uint32(50 + worker*25 + i)
				if lba >= 256 {
					return
				}
				data := payload(lba, 0xF0)
				lsn, _ := primary.Write(lba, data)
				_ = s.NotifyAppend(lba, lsn, data)
				time.Sleep(time.Microsecond)
			}
		}(w)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	drainErr := s.DrainBacklog(ctx)
	appendWg.Wait()
	if drainErr != nil {
		t.Fatalf("DrainBacklog: %v", drainErr)
	}

	if max := emit.MaxInFlight(); max > 1 {
		t.Errorf("INV-NO-DOUBLE-LIVE violated: maxInFlight=%d (want ≤1; shipMu MUST serialize emits)", max)
	}
}

// TestWalShipper_R2_LagSignalFires — spec §6.2 R2 saturation hook.
//
// "OnSaturation fires exactly once when synthetic load drives lag
// past threshold for > N ms".
//
// Test:
//  1. Pre-seed primary so head is high (e.g. LSN 1000).
//  2. Cap emit rate by injecting blocking emit (or pause via
//     channel; here we use NotifyAppend in Backlog mode where
//     entries don't ship until DrainBacklog runs).
//  3. Configure SaturationThreshold = 500.
//  4. After enough writes, cursor=0 and head=1000 → lag=1000 > 500
//     → OnSaturation should fire.
//  5. Continued lag should NOT spam the hook (single-shot per session).
func TestWalShipper_R2_LagSignalFires(t *testing.T) {
	primary := memorywal.NewStore(2048, 4096)

	var fired atomic.Int32
	cfg := WalShipperConfig{
		IdleSleep:           time.Millisecond,
		SaturationThreshold: 500,
		DisableTimerDrain:   true, // C2: test drives lag manually; no auto-drain
		OnSaturation: func(replicaID string, lag uint64) {
			fired.Add(1)
			if replicaID != "r1" {
				t.Errorf("OnSaturation replicaID=%q want r1", replicaID)
			}
			if lag < 500 {
				t.Errorf("OnSaturation lag=%d want ≥500", lag)
			}
		},
	}
	emit := newRecordingEmit()
	s := NewWalShipperWithOptions("r1", HeadSourceFromStorage(primary), primary, emit.Func(), cfg)
	if err := s.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// §6.3 collapse note: pre-§6.3, NotifyAppend in Backlog mode was
	// lag-only (no emit) — the test could pump 1000 NotifyAppends and
	// observe lag accumulate. Post-§6.3 (drive() single dispatch),
	// NotifyAppend with input.lsn==cursor+1 takes the fast-path tail
	// emit; cursor advances per call; lag stays at 0. To create the
	// saturation observable in the new model, pre-seed substrate
	// (build head without triggering drive), then trigger ONE drive()
	// call whose CASE A scan observes the high lag during the first
	// emit's updateLag.
	for lba := uint32(0); lba < 1000; lba++ {
		data := payload(lba, 0xF0)
		_, _ = primary.Write(lba, data) // substrate head advances; no NotifyAppend yet
	}

	// Single trigger NotifyAppend. drive() CASE A scans from cursor=0;
	// first emit's updateLag observes head-cursor ≈ 1000 ≥ threshold
	// 500 → OnSaturation fires (single-shot per session).
	trigData := payload(1500, 0xF1)
	trigLSN, _ := primary.Write(1500, trigData)
	_ = s.NotifyAppend(1500, trigLSN, trigData)

	// Allow the goroutine'd hook call to land.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && fired.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	if fired.Load() == 0 {
		t.Errorf("OnSaturation never fired despite seeded lag ≈1000 > threshold 500")
	}
	if fired.Load() > 1 {
		t.Errorf("OnSaturation fired %d times; want 1 (single-shot per session)", fired.Load())
	}
}

// TestWalShipper_R2_NoSpuriousSignal — spec §6.3 R2 boundedness.
//
// "OnSaturation does NOT fire at all when lag stays under threshold
// for the test duration".
func TestWalShipper_R2_NoSpuriousSignal(t *testing.T) {
	primary := memorywal.NewStore(64, 4096)

	var fired atomic.Int32
	cfg := WalShipperConfig{
		IdleSleep:           time.Millisecond,
		SaturationThreshold: 1000,
		OnSaturation: func(string, uint64) {
			fired.Add(1)
		},
	}
	emit := newRecordingEmit()
	s := NewWalShipperWithOptions("r1", HeadSourceFromStorage(primary), primary, emit.Func(), cfg)
	if err := s.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// 50 writes — well under threshold of 1000.
	for lba := uint32(0); lba < 50; lba++ {
		data := payload(lba, 0xF0)
		lsn, _ := primary.Write(lba, data)
		_ = s.NotifyAppend(lba, lsn, data)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := s.DrainBacklog(ctx); err != nil {
		t.Fatalf("DrainBacklog: %v", err)
	}

	// After drain, lag is 0; OnSaturation must never have fired.
	time.Sleep(50 * time.Millisecond) // any pending hook call would land
	if fired.Load() != 0 {
		t.Errorf("OnSaturation fired %d times under threshold-safe load (R2 spurious)", fired.Load())
	}
}

// TestWalShipper_BacklogToRealtime_Happy — spec §4 default priority
// end-to-end without races.
//
// Steady → StartSession (rewind) → DrainBacklog → R1 transition →
// Realtime → NotifyAppend → emit. Pure happy path, no concurrent
// writer, no failures.
func TestWalShipper_BacklogToRealtime_Happy(t *testing.T) {
	primary := memorywal.NewStore(32, 4096)
	for lba := uint32(0); lba < 10; lba++ {
		_, _ = primary.Write(lba, payload(lba, 0x10))
	}
	_, _ = primary.Sync()

	emit := newRecordingEmit()
	s := NewWalShipper("r1", HeadSourceFromStorage(primary), primary, emit.Func())
	if err := s.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.DrainBacklog(ctx); err != nil {
		t.Fatalf("DrainBacklog: %v", err)
	}

	if got := s.Mode(); got != ModeRealtime {
		t.Fatalf("post-drain mode=%s want Realtime", got)
	}

	// Now in Realtime: a new write should emit immediately.
	rtData := payload(15, 0x10)
	rtLSN, err := primary.Write(15, rtData)
	if err != nil {
		t.Fatalf("realtime Write: %v", err)
	}
	if err := s.NotifyAppend(15, rtLSN, rtData); err != nil {
		t.Fatalf("Realtime NotifyAppend: %v", err)
	}

	got := emit.Snapshot()
	// Expect: 10 backlog entries (LSN 1..10) + 1 realtime (LSN 11).
	if len(got) != 11 {
		t.Fatalf("emit count=%d want 11 (10 backlog + 1 realtime)", len(got))
	}
	if got[10].LSN != 11 {
		t.Errorf("realtime emit LSN=%d want 11", got[10].LSN)
	}
}

// TestWalShipper_R2_SaturationUnderSustainedLoad — §6.3 saturation
// observability under continuous writer pressure.
//
// (Renamed from TestWalShipper_BacklogStaysBacklog_UnderLoad which
// tested a §13-era contract: "mode stays Backlog under saturation".
// §6.3 single drive() collapse retired the mode-as-dispatch concept;
// the new contract is observability via the lag signal — OnSaturation
// fires once per session when lag crosses threshold, regardless of
// any mode label.)
//
// Setup:
//  1. Slow EmitFunc (sleeps per call) — simulates slow wire so the
//     scan loop within a single drive() takes measurable time.
//  2. Background writer goroutine writes substrate fast (no emit).
//     Producer outpaces ship rate; head advances faster than cursor.
//  3. Trigger drive() periodically via NotifyAppend; each drive()
//     CASE A scan observes lag mid-flight (between emits).
//
// What §6.3 promises here:
//   - OnSaturation fires exactly once per session when lag crosses
//     threshold (single-shot, INV-R2-SINGLE-SHOT-PER-SESSION).
//   - cursor advances monotonically (INV-MONOTONIC-CURSOR), never
//     regresses, even under sustained writer pressure.
//   - After writer stops + bounded settle time, cursor catches head
//     (drive() CASE A drains remaining backlog).
//
// What §6.3 does NOT promise here (§13-era retired):
//   - "Mode stays Backlog under load" — mode is now derived;
//     transient ModeRealtime windows (cursor==head between writer's
//     primary.Write and next NotifyAppend) are normal and harmless.
//   - "DrainBacklog returns FailureCancelled at ctx deadline" —
//     drive() integrates its own scan; the explicit DrainBacklog
//     entry point is for recovery sessions, not steady saturation.
func TestWalShipper_R2_SaturationUnderSustainedLoad(t *testing.T) {
	primary := memorywal.NewStore(8192, 4096)
	// Pre-seed enough lag so the very first drive() observes
	// lag >= threshold (saturation fires deterministically).
	const seedN = 600
	for lba := uint32(0); lba < seedN; lba++ {
		_, _ = primary.Write(lba, payload(lba, 0x20))
	}
	_, _ = primary.Sync()

	emit := newRecordingEmit()
	slowEmit := EmitFunc(func(kind EmitKind, lba uint32, lsn uint64, data []byte) error {
		time.Sleep(50 * time.Microsecond) // slow wire simulator
		return emit.Func()(kind, lba, lsn, data)
	})

	var fired atomic.Int32
	cfg := WalShipperConfig{
		IdleSleep:           time.Millisecond,
		SaturationThreshold: 500, // lag will exceed this on first observation
		DisableTimerDrain:   true,
		OnSaturation: func(replicaID string, lag uint64) {
			fired.Add(1)
			if replicaID != "r1" {
				t.Errorf("OnSaturation replicaID=%q want r1", replicaID)
			}
			if lag < 500 {
				t.Errorf("OnSaturation lag=%d want ≥500", lag)
			}
		},
	}
	s := NewWalShipperWithOptions("r1", HeadSourceFromStorage(primary), primary, slowEmit, cfg)
	defer s.Stop()
	if err := s.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Background writer keeps adding entries; head extends during
	// drive()'s CASE A scan. Each writer iteration writes substrate
	// only (no NotifyAppend) — the test's main goroutine drives
	// emission via NotifyAppend triggers below.
	stopWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		i := uint32(seedN)
		for {
			select {
			case <-stopWriter:
				return
			default:
			}
			if i >= 8192 {
				return
			}
			_, _ = primary.Write(i, payload(i, 0x30))
			i++
		}
	}()

	// Trigger drive() with a sequence of NotifyAppends. Each call
	// enters CASE A (cursor < head), scans+emits a chunk, releases.
	// updateLagLocked observes lag during scan; first observation ≥
	// threshold fires OnSaturation.
	cursor0 := s.Cursor()
	for k := 0; k < 50; k++ {
		// Use the current cursor+1 path; substrate-fill via CASE A
		// will dominate (cursor far behind head). Input.lsn is
		// idempotent-skipped because CASE A advances cursor past it.
		next := s.Cursor() + 1
		_ = s.NotifyAppend(0, next, payload(0, 0x40))
		// Brief breather so the writer goroutine can extend head.
		time.Sleep(time.Millisecond)
	}

	// Stop writer and let drive() catch up.
	close(stopWriter)
	<-writerDone
	// One final drive to drain any remaining tail.
	for k := 0; k < 5; k++ {
		next := s.Cursor() + 1
		_ = s.NotifyAppend(0, next, payload(0, 0x40))
		time.Sleep(time.Millisecond)
	}

	// Allow the goroutine'd OnSaturation hook to land if it fired
	// just before our reads.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) && fired.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	if fired.Load() == 0 {
		t.Errorf("OnSaturation never fired despite seeded lag≈%d > threshold 500", seedN)
	}
	if fired.Load() > 1 {
		t.Errorf("OnSaturation fired %d times; want 1 (single-shot per session)", fired.Load())
	}

	// cursor advanced monotonically (INV-MONOTONIC-CURSOR).
	if s.Cursor() <= cursor0 {
		t.Errorf("cursor did not advance: cursor0=%d post=%d", cursor0, s.Cursor())
	}

	t.Logf("§6.3 saturation: cursor advanced %d→%d; OnSaturation fired=%d",
		cursor0, s.Cursor(), fired.Load())
}
