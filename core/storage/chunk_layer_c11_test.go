package storage

// C11 incremental snapshots through a persisted dirty-since-HEAD chunk
// bitmap (chunk_seam.go: chunkBitmapStore, chunkBitmapHeader, chunkDelta,
// BeforeCheckpointAdvance, BeginBaseInstall). The bitmap stays off the
// write hot path: the dirty set is the persisted bitmap ∪ the chunks of WAL
// entries above the checkpoint. The bitmap is durable before ANY checkpoint
// or WAL-tail advance and before any BASE extent write (REBUILDING header).
// It may over-report, never under-report. RED until the layer exists.

import (
	"testing"
)

// extentBytes reads one block straight from the local extent region of the
// volume file, bypassing the dirty map and the layer.
func extentBytes(t *testing.T, s *WALStore, lba uint32) []byte {
	t.Helper()
	b := make([]byte, ctBlockSize)
	if _, err := s.fd.ReadAt(b, int64(s.extentBase+uint64(lba)*uint64(ctBlockSize))); err != nil {
		t.Fatalf("read extent LBA %d: %v", lba, err)
	}
	return b
}

// walLogicalTail is the native retained cursor.
func walLogicalTail(s *WALStore) uint64 { return s.v3.pub.current().RetainedTail }

func flushWithin(t *testing.T, s *WALStore, what string) error {
	t.Helper()
	var ferr error
	within(t, what, func() error { ferr = s.flusher.flushOnce(); return nil })
	return ferr
}
