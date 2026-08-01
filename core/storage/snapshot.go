package storage

import "context"

// SnapshotCut describes one durability-fenced point-in-time capture.
// Frontier belongs to the source volume; a restored volume receives its own
// LSN sequence when the archive is applied through its normal write path.
type SnapshotCut struct {
	Frontier   uint64
	NumBlocks  uint32
	BlockSize  int
	BlockCount uint64
	DataBytes  uint64
}

// SnapshotBlockSink consumes one non-zero block while the source's snapshot
// mutation barrier is held. Implementations emit LBAs in ascending order. The
// data slice is owned by the caller only for the duration of the callback.
type SnapshotBlockSink func(lba uint32, data []byte) error

// SnapshotSource is the point-in-time data-lifecycle capability. It is
// intentionally separate from LogicalStorage.AllBlocks: rebuild enumeration
// may converge with a later WAL lane, while a standalone snapshot must be one
// atomic cut.
//
// CaptureSnapshot must fence and durably sync every mutation admitted before
// the cut, hold later mutations until sink returns, and return the synced
// source frontier. A failed sink fails the capture; callers must not publish a
// ready snapshot from partial output.
type SnapshotSource interface {
	CaptureSnapshot(ctx context.Context, sink SnapshotBlockSink) (SnapshotCut, error)
}

func blockIsZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}
