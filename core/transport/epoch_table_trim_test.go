package transport

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

// A primary adopted at epoch 2 at next LSN 10, holding an applied but
// unsynced tail 6..9 of epoch 1; it crashes, recovers to 5 (next LSN 6) and
// adopts epoch 2 again. Its new epoch-2 writes start at 6. Without the trim
// the row stays (2, 10) and the table calls 6..9 epoch 1, so an old primary's
// never-acknowledged epoch-1 tail ending at 8 would reconcile as shared.
func TestEpochTable_TrimAboveTheRecoveredEndKeepsTheNewWritesInTheirEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.epoch-table")
	tb, _ := LoadEpochTable(path, "v1")
	_ = tb.Note(1, 1)
	_ = tb.Note(2, 10)

	// Restart: load, trim at the recovered store's next LSN, adopt again.
	again, err := LoadEpochTable(path, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := again.TrimAbove(6); err != nil {
		t.Fatal(err)
	}
	if err := again.Note(2, 6); err != nil {
		t.Fatal(err)
	}
	if _, shared := again.SharedEnd(1, 8); shared {
		t.Fatalf("an epoch-1 tail ending at 8 reconciles as shared with rows %v; want diverged (6..8 are epoch 2 here)", again.Rows())
	}
	if _, shared := again.SharedEnd(1, 5); !shared {
		t.Fatalf("the shared epoch-1 prefix to 5 is not shared with rows %v", again.Rows())
	}
	reloaded, _ := LoadEpochTable(path, "v1")
	if rows := reloaded.Rows(); len(rows) != 2 || rows[1] != (EpochRow{Epoch: 2, FirstLSN: 6}) {
		t.Fatalf("durable rows after the trim and the note: %v; want [{1 1} {2 6}]", rows)
	}
	// A row at exactly the next LSN (adopted, nothing written) is kept.
	if err := reloaded.TrimAbove(6); err != nil || len(reloaded.Rows()) != 2 {
		t.Fatalf("TrimAbove(next) dropped a row with no entry yet: %v %v", reloaded.Rows(), err)
	}
}

// Every write keeps the newest EpochTableMaxRows rows, so the record always
// reloads: the boundary is written with the widest values and read back.
func TestEpochTable_BoundedSoItAlwaysReloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.epoch-table")
	tb, _ := LoadEpochTable(path, "v1")
	wide := make([]EpochRow, EpochTableMaxRows+100)
	for i := range wide {
		wide[i] = EpochRow{Epoch: math.MaxUint64 - uint64(len(wide)-i), FirstLSN: math.MaxUint64 - uint64(len(wide)-i)}
	}
	if err := tb.Replace(wide); err != nil {
		t.Fatal(err)
	}
	if n := len(tb.Rows()); n != EpochTableMaxRows {
		t.Fatalf("%d rows kept; want the newest %d", n, EpochTableMaxRows)
	}
	if st, _ := os.Stat(path); st.Size() > epochTableMaxBytes {
		t.Fatalf("record is %d bytes, over the %d read bound", st.Size(), epochTableMaxBytes)
	}
	back, err := LoadEpochTable(path, "v1")
	if err != nil || len(back.Rows()) != EpochTableMaxRows || back.Rows()[EpochTableMaxRows-1] != wide[len(wide)-1] {
		t.Fatalf("reload at the boundary: %d rows %v", len(back.Rows()), err)
	}
	if err := back.Note(math.MaxUint64, math.MaxUint64); err != nil || len(back.Rows()) != EpochTableMaxRows {
		t.Fatalf("a note past the bound: %d rows %v", len(back.Rows()), err)
	}
	if _, err := DecodeEpochRows(EncodeEpochRows(wide)); err == nil {
		t.Fatalf("a rebuild-done row list of %d rows was accepted", len(wide))
	}
}
