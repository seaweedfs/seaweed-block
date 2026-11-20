package storage

import (
	"os"

	"testing"
)

// diskSuperblockForGapTest reads the on-disk superblock with a plain file
// read: no store, no flusher or committer.
func diskSuperblockForGapTest(t *testing.T, path string) superblock {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sb, err := readSuperblock(f)
	if err != nil {
		t.Fatal(err)
	}
	return sb
}
