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
	ChunkCutResult    = chunkCutResult
	ChunkCutStats     = chunkCutStats
	ChunkIntent       = chunkIntent
	ChunkDetachedBy   = chunkDetachedBy
)

// HEAD states.
const (
	ChunkHeadActive   = chunkHeadActive
	ChunkHeadDetached = chunkHeadDetached
)

// NewChunkIntentID returns a fresh intent id "<gen>-<nonce>" for a remote
// implementation's PutIntent; ValidChunkIntentID accepts exactly those ids.
func NewChunkIntentID(gen uint64) (string, error) { return newChunkIntentID(gen) }

func ValidChunkIntentID(id string) bool { return validChunkIntentID(id) }

const (
	ChunkBaseNone                 = chunkBaseNone
	ChunkBaseInstalling           = chunkBaseInstalling
	ChunkBaseInstalledAwaitingCut = chunkBaseInstalledAwaitingCut
)

var (
	ErrChunkKeyExists         = errChunkKeyExists
	ErrChunkHeadPrecondition  = errChunkHeadPrecondition
	ErrChunkLayerFenced       = errChunkLayerFenced
	ErrChunkEvictRefused      = errChunkEvictRefused
	ErrChunkCaptureRefused    = errChunkCaptureRefused
	ErrChunkKeyInvalid        = errChunkKeyInvalid
	ErrChunkLayerDetached     = errChunkLayerDetached
	ErrChunkCASOutcomeUnknown = errChunkCASOutcomeUnknown
	ErrChunkLayerClosed       = errChunkLayerClosed
	ErrChunkBaseInstalling    = errChunkBaseInstalling
	ErrChunkRemoteUnavailable = errChunkRemoteUnavailable
	ErrChunkLSNBehind         = errChunkLSNBehind
)

// ValidChunkKey enforces the chunk key domain (non-empty valid UTF-8); a
// remote implementation calls it in Put and, for every key, in PutIntent.
func ValidChunkKey(key string) error { return validChunkKey(key) }

// AttachChunkLayer attaches a chunk layer to an open, recovered WALStore.
// Attach after Recover: the dirty set and the restart rules read the
// replayed dirty map. The store owns the layer from then on and closes it
// in WALStore.Close.
func AttachChunkLayer(s *WALStore, remote ChunkRemote, cfg ChunkLayerConfig) (ChunkLayer, error) {
	return attachChunkLayer(s, remote, cfg)
}
