package transport

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestEpochTable_NoteIsDurableAndOrdered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.epoch-table")
	tb, err := LoadEpochTable(path, "v1")
	if err != nil || tb.LastEpoch() != 0 {
		t.Fatalf("missing record: %v last=%d; want an empty table", err, tb.LastEpoch())
	}
	for _, s := range []struct{ e, lsn uint64 }{{1, 1}, {1, 7}, {3, 10}} {
		if err := tb.Note(s.e, s.lsn); err != nil {
			t.Fatalf("Note(%d, %d): %v", s.e, s.lsn, err)
		}
	}
	if err := tb.Note(2, 20); err == nil {
		t.Fatal("a lower epoch was accepted")
	}
	if err := tb.Note(4, 9); err == nil {
		t.Fatal("a new epoch starting below the last row was accepted")
	}
	again, err := LoadEpochTable(path, "v1")
	if err != nil {
		t.Fatal(err)
	}
	want := []EpochRow{{1, 1}, {3, 10}}
	if got := again.Rows(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("reloaded rows %v; want %v", got, want)
	}
	if _, err := LoadEpochTable(path, "v2"); err == nil {
		t.Fatal("a record of another volume was accepted")
	}
}

// Every persist step is interrupted once: the call fails, and the table in
// memory and on disk keeps the previous rows (the entry must not be acked).
func TestEpochTable_InterruptEveryPersistStep(t *testing.T) {
	boom := errors.New("injected")
	for _, step := range []string{"write", "rename", "syncDir"} {
		t.Run(step, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "v1.epoch-table")
			tb, _ := LoadEpochTable(path, "v1")
			if err := tb.Note(1, 1); err != nil {
				t.Fatal(err)
			}
			saved := floorFS
			t.Cleanup(func() { floorFS = saved })
			ops := osFloorOps
			switch step {
			case "write":
				ops.writeSynced = func(string, []byte) error { return boom }
			case "rename":
				ops.rename = func(string, string) error { return boom }
			case "syncDir":
				ops.syncDir = func(string) error { return boom }
			}
			floorFS = ops
			if err := tb.Note(2, 5); !errors.Is(err, boom) {
				t.Fatalf("Note with %s failing: %v; want the injected error", step, err)
			}
			if tb.LastEpoch() != 1 {
				t.Fatalf("in memory the failed row was kept: last epoch %d", tb.LastEpoch())
			}
			floorFS = saved
			if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) && step != "syncDir" {
				t.Fatalf("temp file left after a failed %s", step)
			}
		})
	}
}

// A row made durable before its first entry, with a crash between: the row
// is not the last data epoch until the durable end reaches its first LSN.
func TestEpochTable_LastDataEpochAtTheDurableEnd(t *testing.T) {
	tb, _ := LoadEpochTable("", "v1")
	_ = tb.Note(1, 1)
	_ = tb.Note(2, 50)
	for _, c := range []struct{ end, want uint64 }{{0, 0}, {1, 1}, {49, 1}, {50, 2}, {80, 2}} {
		if got := tb.LastDataEpochAt(c.end); got != c.want {
			t.Errorf("LastDataEpochAt(%d) = %d; want %d", c.end, got, c.want)
		}
	}
}

// SharedEnd's outcomes, one per guard.
func TestEpochTable_SharedEnd(t *testing.T) {
	tb, _ := LoadEpochTable("", "v1")
	_ = tb.Note(1, 1)
	_ = tb.Note(2, 50) // the new primary's epoch starts at 50
	for _, c := range []struct {
		name       string
		epoch, end uint64
		wantEnd    uint64
		wantShared bool
	}{
		{"no data", 0, 0, 0, true},
		{"data but no table", 0, 40, 0, false},
		{"current epoch", 2, 70, 70, true},
		{"old epoch, prefix", 1, 49, 49, true},
		{"old epoch, equal length, divergent", 1, 50, 49, false},
		{"old epoch, above, divergent", 1, 60, 49, false},
		{"unknown epoch", 3, 10, 0, false},
	} {
		end, shared := tb.SharedEnd(c.epoch, c.end)
		if end != c.wantEnd || shared != c.wantShared {
			t.Errorf("%s: SharedEnd(%d, %d) = (%d, %t); want (%d, %t)", c.name, c.epoch, c.end, end, shared, c.wantEnd, c.wantShared)
		}
	}
}
