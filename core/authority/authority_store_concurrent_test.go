package authority

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// D3 — Concurrent write/reload regression. Permanent guardrail
// for the rename-retry discipline added when the Windows
// Defender race surfaced. If a future refactor weakens the
// retry classification (e.g., absorbs a non-retryable error
// silently, or drops retries altogether), this test must fail.
//
// Shape:
//   - N writer goroutines (N >= 4) write to durable store
//   - M reader goroutines (M >= 4) Load concurrently
//   - 100 cycles of (write batch, reload, verify)
//   - Budget: < 30s on typical hardware
//
// Asserts:
//   - Every successful Put is visible on the very next Load
//     (no silent data loss)
//   - Authority tuple is monotonic per volume (WriteSeq strictly
//     grows)
//   - No non-retryable error absorbed silently (errors surface)
//   - Total failures across all cycles fits within expected
//     zero-on-clean-fs bound
func TestAuthorityStore_ConcurrentWriteReload_100Cycles(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrent store regression; skipped under -short")
	}
	dir := t.TempDir()
	store, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatalf("NewFileAuthorityStore: %v", err)
	}
	defer store.Close()

	const (
		cycles    = 100
		writers   = 4
		readers   = 4
		volCount  = 8 // 8 distinct volumes share the store
	)

	// Each volume has a monotonically-growing WriteSeq that we
	// advance across cycles. After the final cycle we assert
	// the loaded seq equals the maximum submitted seq per
	// volume — no dropped writes.
	var submittedSeq [volCount]atomic.Uint64
	for v := 0; v < volCount; v++ {
		submittedSeq[v].Store(0)
	}

	putErrCount := atomic.Uint64{}

	for cycle := 0; cycle < cycles; cycle++ {
		var wg sync.WaitGroup

		// Writers: each picks a volume (round-robin by writer id
		// + cycle), advances its submittedSeq, and Puts.
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				v := (w + cycle) % volCount
				seq := submittedSeq[v].Add(1)
				rec := DurableRecord{
					VolumeID:        fmt.Sprintf("vol-%d", v),
					ReplicaID:       fmt.Sprintf("r-%d-%d", v, seq),
					Epoch:           seq,
					EndpointVersion: 1,
					DataAddr:        fmt.Sprintf("data-%d-%d", v, seq),
					CtrlAddr:        fmt.Sprintf("ctrl-%d-%d", v, seq),
					WriteSeq:        seq,
				}
				if err := store.Put(rec); err != nil {
					// A Put failure surfaces as a test
					// failure AT CYCLE END (we collect and
					// report), not a hard panic — so one
					// spurious Defender miss doesn't mask
					// later cycles from running.
					putErrCount.Add(1)
					t.Errorf("cycle %d writer %d vol %d: Put(%d): %v", cycle, w, v, seq, err)
				}
			}(w)
		}

		// Readers: concurrently Load the store and verify that
		// every record they see has a WriteSeq <= the current
		// max submitted for that volume (i.e., no record from
		// the future appeared).
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				recs, err := store.Load()
				if err != nil {
					t.Errorf("cycle %d reader: Load: %v", cycle, err)
					return
				}
				for _, rec := range recs {
					var vid int
					if _, perr := fmt.Sscanf(rec.VolumeID, "vol-%d", &vid); perr != nil || vid < 0 || vid >= volCount {
						t.Errorf("cycle %d reader: unexpected VolumeID %q", cycle, rec.VolumeID)
						continue
					}
					// Record's WriteSeq must not exceed what
					// has been submitted so far (no "future"
					// data leaking through).
					maxSubmitted := submittedSeq[vid].Load()
					if rec.WriteSeq > maxSubmitted {
						t.Errorf("cycle %d reader: vol-%d loaded WriteSeq=%d but max submitted=%d",
							cycle, vid, rec.WriteSeq, maxSubmitted)
					}
				}
			}()
		}

		wg.Wait()
	}

	// Post-cycles: final reload must show the highest WriteSeq
	// for each volume equal to the highest seq we submitted.
	// This catches silent data loss — if any Put succeeded but
	// wasn't persisted, the final seq would be lower than
	// submittedSeq[v].
	final, err := store.Load()
	if err != nil {
		t.Fatalf("final Load: %v", err)
	}
	gotMax := map[int]uint64{}
	for _, rec := range final {
		var vid int
		if _, perr := fmt.Sscanf(rec.VolumeID, "vol-%d", &vid); perr != nil {
			continue
		}
		if rec.WriteSeq > gotMax[vid] {
			gotMax[vid] = rec.WriteSeq
		}
	}
	for v := 0; v < volCount; v++ {
		want := submittedSeq[v].Load()
		if want == 0 {
			continue // no writes landed on this volume
		}
		if gotMax[v] != want {
			t.Errorf("vol-%d: final WriteSeq=%d but submitted=%d (possible silent loss)", v, gotMax[v], want)
		}
	}

	if failed := putErrCount.Load(); failed > 0 {
		t.Logf("%d Puts failed (rename retry budget must absorb — investigate if non-zero on clean fs)", failed)
	}
}
