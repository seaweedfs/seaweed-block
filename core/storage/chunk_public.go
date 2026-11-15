package storage

// Exported surface of the chunk layer for callers outside this package:
// the durable provider and remote implementations. The aliases keep the
// contract types single-sourced in chunk_seam.go.

type (
	ChunkRemote       = chunkRemote
	ChunkHead         = chunkHead
	ChunkHeadVersion  = chunkHeadVersion
	ChunkRef          = chunkRef
	ChunkBitmapStore  = chunkBitmapStore
	ChunkBitmapHeader = chunkBitmapHeader
	ChunkBaseState    = chunkBaseState
	ChunkDelta        = chunkDelta
	ChunkLayerConfig  = chunkLayerConfig
	ChunkLayer        = chunkLayer
)

const (
	ChunkBaseNone                 = chunkBaseNone
	ChunkBaseInstalling           = chunkBaseInstalling
	ChunkBaseInstalledAwaitingCut = chunkBaseInstalledAwaitingCut
)

var (
	ErrChunkKeyExists        = errChunkKeyExists
	ErrChunkHeadPrecondition = errChunkHeadPrecondition
	ErrChunkLayerFenced      = errChunkLayerFenced
	ErrChunkEvictRefused     = errChunkEvictRefused
	ErrChunkCaptureRefused   = errChunkCaptureRefused
)

// AttachChunkLayer attaches a chunk layer to an open, recovered WALStore.
// Attach after Recover: the dirty set and the restart rules read the
// replayed dirty map. The store owns the layer from then on and closes it
// in WALStore.Close.
func AttachChunkLayer(s *WALStore, remote ChunkRemote, cfg ChunkLayerConfig) (ChunkLayer, error) {
	return attachChunkLayer(s, remote, cfg)
}
