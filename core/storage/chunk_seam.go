package storage

// Chunk layer seam: an asynchronous remote copy of a walstore volume.
//
// The chunk layer sits under the flusher. The local WAL and extent stay
// authoritative: Sync still means local durability (L1). The remote copy is
// an immutable-chunk image of some LSN prefix (L2), published by a
// compare-and-swap of a single HEAD record. The contract the layer must meet
// is C1-C10 in chunk_layer_contract_test.go.
//
// This file declares the seam only. The layer itself is not implemented
// yet: attachChunkLayer returns errChunkLayerNotImplemented, so the contract
// tests fail until it exists.

import (
	"context"
	"errors"
)

// chunkSink receives the walstore events the chunk layer needs. The store
// calls it from three places only:
//   - OnExtentWrite: the single extent write path (flusher) and
//     WriteExtentDirect (rebuild BASE), with the LSN of the written block
//     (0 for a BASE block, which carries no LSN).
//   - OnCheckpoint: after persistCheckpoint made a new checkpoint durable.
//   - OnReadMiss: from readFromExtent, before reading the local extent.
//
// The sink owns the per-block presence bitmap. OnReadMiss returns nil data
// when the block is present locally (the store then reads its extent);
// otherwise it returns the block's content as published in HEAD, and the
// sink has filled the local extent with it.
type chunkSink interface {
	OnExtentWrite(lba uint64, lsn uint64)
	OnCheckpoint(lsn uint64)
	OnReadMiss(lba uint64) ([]byte, error)
}

// chunkHeadVersion is the CAS guard of HEAD.
type chunkHeadVersion struct {
	Gen   uint64
	Epoch uint64
}

// chunkHead is the published remote state. Chunks maps a chunk index to the
// immutable key holding that chunk's bytes (ChunkBlocks*BlockSize bytes,
// block i of the chunk at offset i*BlockSize). A chunk index missing from
// the map is ZERO: every block in it reads as zeros.
type chunkHead struct {
	Gen       uint64
	Epoch     uint64
	Holder    string
	RemoteLSN uint64
	Chunks    map[uint64]string
}

// chunkRemote is the remote store. Keys are immutable: Put of an existing
// key fails with errChunkKeyExists and never replaces the stored bytes.
// CASHead installs next only when the stored HEAD matches expected, and
// returns the new generation; a mismatch fails with errChunkHeadPrecondition.
// A CASHead error other than errChunkHeadPrecondition leaves the outcome
// unknown: the caller resolves it by re-reading HEAD, never by a blind retry.
type chunkRemote interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	GetHead(ctx context.Context) (chunkHead, error)
	CASHead(ctx context.Context, expected chunkHeadVersion, next chunkHead) (uint64, error)
	Delete(ctx context.Context, key string) error
}

// chunkLayerConfig configures one attachment. ChunkBlocks is the number of
// consecutive blocks per chunk (1024 for 4 KiB blocks and 4 MiB chunks).
type chunkLayerConfig struct {
	ChunkBlocks uint32
	Epoch       uint64
	Holder      string
}

// chunkLayer is what the store drives and what callers use for the
// remote-durability operations.
type chunkLayer interface {
	chunkSink
	// RemoteCheckpoint uploads every dirty chunk as of the current local
	// checkpoint C and then CASes HEAD with RemoteLSN = C. It returns the
	// published RemoteLSN.
	RemoteCheckpoint(ctx context.Context) (uint64, error)
	// MakeRemoteDurable is the durability point used by snapshot, detach and
	// migrate: Sync, checkpoint >= t, upload until RemoteLSN >= t, CAS HEAD.
	// Only when it returns nil is t remote durable.
	MakeRemoteDurable(ctx context.Context, t uint64) error
	// Evict makes a block absent locally. It refuses while the block is
	// dirty or its last written LSN is above HEAD's RemoteLSN.
	Evict(lba uint64) error
	RemoteLSN() uint64
	Fenced() bool
}

var (
	errChunkLayerNotImplemented = errors.New("storage: chunk layer not implemented")
	errChunkKeyExists           = errors.New("storage: chunk key already exists")
	errChunkHeadPrecondition    = errors.New("storage: chunk HEAD precondition failed")
	errChunkLayerFenced         = errors.New("storage: chunk layer fenced")
	errChunkEvictRefused        = errors.New("storage: chunk eviction refused")
)

// attachChunkLayer attaches a chunk layer to an open store. With an empty
// local volume and a non-empty HEAD it starts from HEAD and reads lazily.
func attachChunkLayer(s *WALStore, remote chunkRemote, cfg chunkLayerConfig) (chunkLayer, error) {
	return nil, errChunkLayerNotImplemented
}
