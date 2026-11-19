package parallelwal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateCapacityBeforeFilesystem(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := CreateStore(filepath.Join(parent, "volume"), ^uint32(0), 8192)
	if s != nil {
		s.Close()
		t.Fatal("created oversized image")
	}
	if err == nil || !strings.Contains(err.Error(), "16 TiB - 4 KiB") {
		t.Fatalf("capacity must precede filesystem: %v", err)
	}
}
