package storage

// Chunk layer seam: an asynchronous remote copy of a walstore volume.
//
// The chunk layer sits under the flusher. The local WAL and extent stay
// authoritative: Sync still means local durability (L1). The remote copy is
// an immutable-chunk image published by a compare-and-swap of a single HEAD
// record (L2). The contract the layer must meet is C1-C11 in
// chunk_layer_contract_test.go and chunk_layer_c11_test.go.
//
// Capture is a quiesced cut, not a flusher checkpoint boundary. A checkpoint
// is not a prefix image: superseded dirty entries are skipped, so the extent
// can already hold blocks newer than the checkpoint, and old block versions
// are not kept. Capture therefore:
//  1. blocks new writes at the store and waits for in-flight ones;
//  2. Syncs; H is then the synced LSN (nextLSN-1) and no data newer than H
//     exists in the WAL or the extent. The flusher is not drained;
//  3. under that barrier reads every chunk dirtied since the last HEAD
//     through the store's normal read path (dirty map/WAL entry if present,
//     else extent) into immutable buffers; that is exactly the image at H.
//     A chunk counts as dirty since HEAD whether its newer blocks are still
//     in the WAL or already in the extent;
//  4. releases the barrier (pause = Sync + copy of the dirty chunks);
//  5. uploads the buffers, outside every walstore lock;
//  6. CASes HEAD {gen, epoch} with RemoteLSN = H.
//
// This file declares the seam only. The layer is not implemented yet:
// attachChunkLayer returns errChunkLayerNotImplemented, so the contract tests
// fail until it exists.

import (
	"context"
	"errors"
	"time"
)

// chunkSink receives walstore events. The store calls:
//   - OnExtentWrite from the single extent write path (flusher) and from
//     WriteExtentDirect (rebuild BASE), with the block's LSN. LSN 0 means a
//     BASE block: a rebuild replaces the image, so it marks every chunk
//     dirty (no cut runs during a rebuild; see CaptureAllowed).
//   - BeforeCheckpoint from the flusher after its extent fsync and before
//     persistCheckpoint advances the checkpoint and the WAL tail;
//   - OnReadMiss from readFromExtent, after releasing the extent lock.
//
// The dirty bitmap stays off the write hot path. WAL entries carry their
// LBA, so the dirty set is always the persisted bitmap ∪ the chunks of WAL
// entries above the checkpoint. OnExtentWrite runs under walstore extent or
// publication locks and must be O(1), never block, do no I/O and take no
// lock beyond the sink's own bit-set: it only marks chunks in memory.
// BeforeCheckpoint persists the in-memory dirty set (chunkBitmapStore.Save)
// before the WAL entries it covers can be recycled; an error aborts that
// checkpoint, so the WAL keeps them. The persisted bitmap may over-report,
// never under-report.
//
// OnReadMiss returns nil data when the block is present locally (the store
// then reads its extent). Otherwise it fetches the block's chunk as
// published in HEAD, bounded by chunkLayerConfig.ReadMissTimeout, and
// returns the block. It installs fetched bytes into the extent only for
// blocks that have no newer local version (dirty map, WAL or extent write
// since the fetch began); a local version always wins.
type chunkSink interface {
	OnExtentWrite(lba uint64, lsn uint64)
	BeforeCheckpoint(lsn uint64) error
	OnReadMiss(lba uint64) ([]byte, error)
}

// chunkBitmapHeader ties a persisted bitmap to the HEAD period it covers:
// BaseLSN is the cut H of that HEAD, HeadGen and Epoch its CAS guard.
type chunkBitmapHeader struct {
	BaseLSN uint64
	HeadGen uint64
	Epoch   uint64
}

// chunkDelta archives one closed HEAD period, for snapshot diff,
// replication and audit: the cuts it spans, the chunks dirtied in it, and
// the immutable keys the new HEAD published for them.
type chunkDelta struct {
	FromLSN uint64
	ToLSN   uint64
	Gen     uint64
	Dirty   []uint64
	NewKeys map[uint64]string
}

// chunkBitmapStore persists the dirty-since-HEAD chunk bitmap (one bit per
// chunk) under a chunkBitmapHeader. It lives on the local host next to the
// volume.
//   - Load returns the header and dirty chunks; ok is false when nothing
//     usable is stored (lost or corrupt).
//   - Save makes the current period's dirty set durable (header unchanged).
//   - Reset runs after a successful HEAD CAS at cut H: it archives the
//     closed period as delta and starts a new bitmap with header
//     {BaseLSN: H, HeadGen, Epoch} in which only stillDirty (chunks written
//     after the cut) are set. Durable on return.
//
// Restart or failover: a header equal to HEAD {RemoteLSN, Gen, Epoch} means
// incremental, dirty = persisted bitmap ∪ chunks of WAL entries above the
// checkpoint. A lost bitmap or any header mismatch falls back to a
// per-chunk SHA-256 compare against HEAD's chunk refs; only differing
// chunks are uploaded. Manifests stay logically full per HEAD, so restore
// never chains deltas.
type chunkBitmapStore interface {
	Load() (hdr chunkBitmapHeader, dirty []uint64, ok bool, err error)
	Save(dirty []uint64) error
	Reset(hdr chunkBitmapHeader, stillDirty []uint64, delta chunkDelta) error
}

// chunkRef names one published chunk: its immutable key and the SHA-256 of
// its ChunkBlocks*BlockSize bytes.
type chunkRef struct {
	Key    string
	SHA256 [32]byte
}

// chunkHeadVersion is the CAS guard of HEAD.
type chunkHeadVersion struct {
	Gen   uint64
	Epoch uint64
}

// chunkHead is the published remote state. Chunks maps a chunk index to the
// chunk holding its bytes (block i of the chunk at offset i*BlockSize). A
// chunk index missing from the map is ZERO: every block in it reads as zeros.
type chunkHead struct {
	Gen       uint64
	Epoch     uint64
	Holder    string
	RemoteLSN uint64
	Chunks    map[uint64]chunkRef
}

// chunkRemote is the remote store. Keys are immutable: Put of a key that
// exists, or once existed, fails with errChunkKeyExists and never replaces
// stored bytes. CASHead installs next only when the stored HEAD matches
// expected and returns the new generation; a mismatch fails with
// errChunkHeadPrecondition. Any other CASHead error leaves the outcome
// unknown: the caller resolves it by re-reading HEAD, never by a blind
// retry, and reports no success or frontier advance while it is unresolved.
type chunkRemote interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	GetHead(ctx context.Context) (chunkHead, error)
	CASHead(ctx context.Context, expected chunkHeadVersion, next chunkHead) (uint64, error)
	Delete(ctx context.Context, key string) error
}

// chunkLayerConfig configures one attachment.
type chunkLayerConfig struct {
	// ChunkBlocks is the number of consecutive blocks per chunk (1024 for
	// 4 KiB blocks and 4 MiB chunks).
	ChunkBlocks uint32
	// Epoch is the epoch the block master assigned to this node; every HEAD
	// CAS carries it. It is the manifest's own epoch, kept separate from the
	// frontend Identity().Epoch, which is checked against the master
	// projection on every I/O.
	Epoch  uint64
	Holder string
	// FreshFromHead says the local volume is new and empty and starts from
	// HEAD (every block absent, read lazily). False means an existing local
	// volume: everything local is present and authoritative. Never inferred
	// from the local contents.
	FreshFromHead bool
	// CaptureAllowed is supplied by the store owner. Capture is refused while
	// it returns an error (BASE install or rebuild in progress, not a healthy
	// primary). It is checked again right before the HEAD CAS, so a primary
	// demoted mid-capture never publishes. Nil allows capture.
	CaptureAllowed func() error
	// ReadMissTimeout bounds one OnReadMiss remote fetch.
	ReadMissTimeout time.Duration
	// Bitmap persists the dirty-since-HEAD bitmap. Required.
	Bitmap chunkBitmapStore
}

// chunkLayer is what the store owner drives.
type chunkLayer interface {
	chunkSink
	// Capture takes a quiesced cut at H (the last written LSN), uploads the
	// chunks dirtied since the last HEAD and CASes HEAD with RemoteLSN = H.
	// It returns H once HEAD is installed.
	Capture(ctx context.Context) (uint64, error)
	// MakeRemoteDurable is the durability point for snapshot, detach and
	// migrate: a Capture whose H >= t. Only a nil return makes t remote
	// durable; it requires the local Sync to cover t first, and fails
	// (without waiting) when t was never written.
	MakeRemoteDurable(ctx context.Context, t uint64) error
	// EvictChunk makes a chunk absent locally. Refused while the chunk is
	// dirty since HEAD.
	EvictChunk(idx uint64) error
	RemoteLSN() uint64
	Fenced() bool
	// Close stops and joins every worker the layer owns and cancels its
	// remote calls. WALStore.Close calls it before closing the store.
	Close() error
}

var (
	errChunkLayerNotImplemented = errors.New("storage: chunk layer not implemented")
	errChunkKeyExists           = errors.New("storage: chunk key already exists")
	errChunkHeadPrecondition    = errors.New("storage: chunk HEAD precondition failed")
	errChunkLayerFenced         = errors.New("storage: chunk layer fenced")
	errChunkEvictRefused        = errors.New("storage: chunk eviction refused")
	errChunkCaptureRefused      = errors.New("storage: chunk capture refused")
)

// attachChunkLayer attaches a chunk layer to an open store.
func attachChunkLayer(s *WALStore, remote chunkRemote, cfg chunkLayerConfig) (chunkLayer, error) {
	return nil, errChunkLayerNotImplemented
}
