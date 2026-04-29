package recovery

import (
	"bytes"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

const blockSize = 4096

func newSession(t *testing.T, numBlocks uint32, targetLSN uint64) (*RebuildSession, *storage.BlockStore) {
	t.Helper()
	store := storage.NewBlockStore(numBlocks, blockSize)
	return NewRebuildSession(store, targetLSN), store
}

func makeBlock(marker byte) []byte {
	buf := make([]byte, blockSize)
	for i := range buf {
		buf[i] = marker
	}
	return buf
}

// INV-DUAL-LANE-WAL-WINS-BASE: WAL lane applies first, then base lane
// for the same LBA — base must skip. Substrate retains WAL data.
func TestRebuildSession_WALWinsThenBaseSkips(t *testing.T) {
	s, store := newSession(t, 64, 100)
	walData := makeBlock(0xAA)
	baseData := makeBlock(0xBB)

	if err := s.ApplyWALEntry(WALKindBacklog, 5, walData, 50); err != nil {
		t.Fatalf("ApplyWALEntry: %v", err)
	}
	skipped, err := s.ApplyBaseBlock(5, baseData)
	if err != nil {
		t.Fatalf("ApplyBaseBlock: %v", err)
	}
	if !skipped {
		t.Fatal("base lane should have been skipped after WAL won this LBA")
	}
	got, _ := store.Read(5)
	if !bytes.Equal(got, walData) {
		t.Fatalf("substrate lba 5: got[0]=%02x want %02x (WAL data)", got[0], walData[0])
	}
}

// INV-DUAL-LANE-WAL-WINS-BASE (other ordering): base applies first,
// then WAL applies for the same LBA — WAL data must overwrite base.
// (Substrate ApplyEntry is last-writer-wins by LSN; WAL lane carries
// real LSN, base carries targetLSN.)
func TestRebuildSession_BaseFirstThenWALOverwrites(t *testing.T) {
	s, store := newSession(t, 64, 100)
	baseData := makeBlock(0xBB)
	walData := makeBlock(0xCC)

	skipped, err := s.ApplyBaseBlock(7, baseData)
	if err != nil || skipped {
		t.Fatalf("ApplyBaseBlock: skipped=%v err=%v", skipped, err)
	}
	if err := s.ApplyWALEntry(WALKindBacklog, 7, walData, 75); err != nil {
		t.Fatalf("ApplyWALEntry: %v", err)
	}
	got, _ := store.Read(7)
	if !bytes.Equal(got, walData) {
		t.Fatalf("substrate lba 7: got[0]=%02x want %02x (WAL data)", got[0], walData[0])
	}
}

// INV-SESSION-COMPLETE-ON-CONJUNCTION-LAYER1: TryComplete = baseDone ∧
// walApplied ≥ targetLSN. Three negative cases + positive case.
func TestRebuildSession_TryComplete_Conjunction(t *testing.T) {
	s, _ := newSession(t, 16, 100)

	// Initial: nothing done.
	if _, done := s.TryComplete(); done {
		t.Fatal("fresh session: TryComplete should not be done")
	}

	// baseDone but WAL not yet at target.
	s.MarkBaseComplete()
	_ = s.ApplyWALEntry(WALKindBacklog, 0, makeBlock(0x01), 50)
	if _, done := s.TryComplete(); done {
		t.Fatal("baseDone=true, walApplied=50 < target=100: should not be done")
	}

	// WAL at target but baseDone reset wouldn't be possible — instead
	// check the symmetric case in a separate session.
	s2, _ := newSession(t, 16, 100)
	_ = s2.ApplyWALEntry(WALKindBacklog, 0, makeBlock(0x01), 100)
	if _, done := s2.TryComplete(); done {
		t.Fatal("walApplied=target but !baseDone: should not be done")
	}

	// Both: should complete.
	_ = s.ApplyWALEntry(WALKindBacklog, 0, makeBlock(0x02), 100)
	achieved, done := s.TryComplete()
	if !done {
		t.Fatal("baseDone ∧ walApplied >= target: TryComplete should be done")
	}
	if achieved < 100 {
		t.Fatalf("achievedLSN=%d want >= 100", achieved)
	}
}

func TestRebuildSession_TryComplete_LatchesCompleted(t *testing.T) {
	s, _ := newSession(t, 16, 50)
	s.MarkBaseComplete()
	_ = s.ApplyWALEntry(WALKindBacklog, 0, makeBlock(0x01), 50)
	if _, done := s.TryComplete(); !done {
		t.Fatal("expected done")
	}
	// Subsequent calls return done=true (latched).
	if _, done := s.TryComplete(); !done {
		t.Fatal("TryComplete should latch completed=true")
	}
	// Apply* after completion should error.
	if _, err := s.ApplyBaseBlock(1, makeBlock(0x99)); err == nil {
		t.Fatal("ApplyBaseBlock after completed: want error")
	}
	if err := s.ApplyWALEntry(WALKindBacklog, 1, makeBlock(0x99), 100); err == nil {
		t.Fatal("ApplyWALEntry after completed: want error")
	}
}

// Concurrency: many goroutines on each lane writing different LBAs.
// Run with -race to verify single-mutex serialization is sound.
func TestRebuildSession_ConcurrentLanes(t *testing.T) {
	const numBlocks = 1024
	const targetLSN = 5000
	s, store := newSession(t, numBlocks, targetLSN)

	var wg sync.WaitGroup

	// Base lane writers: 4 goroutines covering different LBA stripes.
	for stripe := 0; stripe < 4; stripe++ {
		wg.Add(1)
		go func(stripe int) {
			defer wg.Done()
			for lba := uint32(stripe); lba < numBlocks; lba += 4 {
				data := makeBlock(byte(0x80 | lba))
				_, _ = s.ApplyBaseBlock(lba, data)
			}
		}(stripe)
	}

	// WAL lane writers: 4 goroutines, different LSN ranges, different LBAs.
	for stripe := 0; stripe < 4; stripe++ {
		wg.Add(1)
		go func(stripe int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				lba := uint32((stripe*200 + i) % numBlocks)
				lsn := uint64(stripe*1000 + i + 1)
				data := makeBlock(byte(0xC0 | i))
				_ = s.ApplyWALEntry(WALKindBacklog, lba, data, lsn)
			}
		}(stripe)
	}

	wg.Wait()

	// Sanity: no panic, no race (latter requires -race). Substrate
	// reads succeed; bitmap count should be ≤ numBlocks.
	st := s.Status()
	if st.BitmapApplied > int(numBlocks) {
		t.Fatalf("BitmapApplied=%d exceeds numBlocks=%d", st.BitmapApplied, numBlocks)
	}
	if st.WALApplied == 0 {
		t.Fatal("WALApplied stayed 0 after concurrent WAL writes")
	}
	// All written LBAs should be readable (no corruption).
	for lba := uint32(0); lba < numBlocks; lba++ {
		if _, err := store.Read(lba); err != nil {
			t.Fatalf("Read lba=%d: %v", lba, err)
		}
	}
}

func TestRebuildSession_BaseBatchAcked_Monotonic(t *testing.T) {
	s, _ := newSession(t, 1024, 100)

	if got := s.Status().BaseAckedPrefix; got != 0 {
		t.Fatalf("fresh: BaseAckedPrefix=%d want 0", got)
	}
	s.BaseBatchAcked(100)
	if got := s.Status().BaseAckedPrefix; got != 100 {
		t.Fatalf("after ack 100: BaseAckedPrefix=%d want 100", got)
	}
	// Lower ack must NOT regress prefix (monotonic).
	s.BaseBatchAcked(50)
	if got := s.Status().BaseAckedPrefix; got != 100 {
		t.Fatalf("after stale ack 50: BaseAckedPrefix=%d want 100 (monotonic)", got)
	}
	s.BaseBatchAcked(500)
	if got := s.Status().BaseAckedPrefix; got != 500 {
		t.Fatalf("after ack 500: BaseAckedPrefix=%d want 500", got)
	}
}

// Status snapshot exposes layer-2-relevant fields.
func TestRebuildSession_Status_Snapshot(t *testing.T) {
	s, _ := newSession(t, 16, 200)
	s.MarkBaseComplete()
	_ = s.ApplyWALEntry(WALKindBacklog, 0, makeBlock(0x01), 50)
	_ = s.ApplyWALEntry(WALKindBacklog, 1, makeBlock(0x02), 100)
	s.BaseBatchAcked(8)

	st := s.Status()
	if st.TargetLSN != 200 {
		t.Errorf("TargetLSN=%d want 200", st.TargetLSN)
	}
	if st.WALApplied != 100 {
		t.Errorf("WALApplied=%d want 100", st.WALApplied)
	}
	if !st.BaseDone {
		t.Error("BaseDone want true")
	}
	if st.Completed {
		t.Error("Completed want false (walApplied < target)")
	}
	if st.BaseAckedPrefix != 8 {
		t.Errorf("BaseAckedPrefix=%d want 8", st.BaseAckedPrefix)
	}
	if st.BitmapApplied != 2 {
		t.Errorf("BitmapApplied=%d want 2", st.BitmapApplied)
	}
}
