package storage

// Exported surface of the chunk layer for callers outside this package:
// the durable provider and remote implementations. The aliases keep the
// contract types single-sourced in chunk_seam.go.

type (
	ChunkRemote        = chunkRemote
	ChunkCommitChecker = chunkCommitChecker
	ChunkHead          = chunkHead
	ChunkHeadVersion   = chunkHeadVersion
	ChunkRef           = chunkRef
	ChunkBitmapStore   = chunkBitmapStore
	ChunkBitmapHeader  = chunkBitmapHeader
	ChunkBaseState     = chunkBaseState
	ChunkDelta         = chunkDelta
	ChunkLayerConfig   = chunkLayerConfig
	ChunkLayer         = chunkLayer
	ChunkCutResult     = chunkCutResult
	ChunkCutStats      = chunkCutStats
	ChunkLayerStats    = chunkLayerStats
	ChunkDetachedBy    = chunkDetachedBy
	DetachSettlement   = detachSettlement
)

// How a failed Detach ended (ChunkLayer.SettleDetach).
const (
	DetachNotPublished = detachNotPublished
	DetachApplied      = detachApplied
	DetachFenced       = detachFenced
)

// HEAD states.
const (
	ChunkHeadActive   = chunkHeadActive
	ChunkHeadDetached = chunkHeadDetached
	ChunkHeadAborted  = chunkHeadAborted
	ChunkHeadDeleting = chunkHeadDeleting
	ChunkHeadPurging  = chunkHeadPurging
	ChunkHeadDeleted  = chunkHeadDeleted
)

const (
	ChunkBaseNone                 = chunkBaseNone
	ChunkBaseInstalling           = chunkBaseInstalling
	ChunkBaseInstalledAwaitingCut = chunkBaseInstalledAwaitingCut
)

var (
	ErrCutCOWAborted          = errCutCOWAborted
	ErrChunkKeyExists         = errChunkKeyExists
	ErrChunkHeadPrecondition  = errChunkHeadPrecondition
	ErrChunkHeadDeleted       = errChunkHeadDeleted
	ErrChunkLayerFenced       = errChunkLayerFenced
	ErrChunkEvictRefused      = errChunkEvictRefused
	ErrChunkCaptureRefused    = errChunkCaptureRefused
	ErrChunkReconciling       = errChunkReconciling
	ErrChunkKeyInvalid        = errChunkKeyInvalid
	ErrChunkLayerDetached     = errChunkLayerDetached
	ErrChunkCASOutcomeUnknown = errChunkCASOutcomeUnknown
	ErrChunkLayerClosed       = errChunkLayerClosed
	ErrChunkBaseInstalling    = errChunkBaseInstalling
	ErrChunkRemoteUnavailable = errChunkRemoteUnavailable
	ErrChunkContentMismatch   = errChunkContentMismatch
	ErrChunkLSNBehind         = errChunkLSNBehind
)

// ValidChunkKey enforces the chunk key domain (non-empty valid UTF-8); a
// remote implementation calls it in Put.
func ValidChunkKey(key string) error { return validChunkKey(key) }

// AttachChunkLayer attaches a chunk layer to an open, recovered WALStore.
// Attach after Recover: the dirty set and the restart rules read the
// replayed dirty map. The store owns the layer from then on and closes it
// in WALStore.Close.
func AttachChunkLayer(s *WALStore, remote ChunkRemote, cfg ChunkLayerConfig) (ChunkLayer, error) {
	return attachChunkLayer(s, remote, cfg)
}
