package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateWALStoreRefusesOversizeBeforeFilesystem(t *testing.T) {
	// An invalid parent prevents even the old implementation from creating a
	// huge sparse image; the capacity error must precede all filesystem work.
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := CreateWALStore(filepath.Join(parent, "volume"), ^uint32(0), 8192)
	if s != nil {
		s.Close()
		t.Fatal("oversized store created")
	}
	if err == nil || !strings.Contains(err.Error(), "16 TiB - 4 KiB") {
		t.Fatalf("want capacity refusal before I/O, got %v", err)
	}
	got, e := os.ReadFile(parent)
	if e != nil || string(got) != "untouched" {
		t.Fatal("parent changed")
	}
}
