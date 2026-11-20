package storage

import (
	"os"

	"testing"
)

// The WAL writer may only overwrite bytes that the PERSISTED superblock tail
// has released (the torn-wrap finding). Otherwise a torn new record can
// land inside the persisted
// [tail, head) and stop recovery's scan before a synced record behind it.

func diskSuperblockForTest(t *testing.T, path string) superblock {
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
