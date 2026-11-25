package storage

import "context"

// ChunkSegment names an immutable range manifest. Owner is the namespace in
// which that manifest was encoded; forks preserve it without rewriting data.
type ChunkSegment struct {
	Key         string   `json:"key"`
	SHA256      [32]byte `json:"sha256"`
	Owner       string   `json:"owner"`
	Count       int      `json:"count"`
	MaxBirthLSN uint64   `json:"max_birth_lsn"`
	SegFormat   int      `json:"seg_format"`
}

// ChunkManifestBuilder prepares immutable segments before the existing root
// CAS, in the publishing caller's session.
type ChunkManifestBuilder interface {
	PrepareManifest(context.Context, ChunkHead, ChunkHead, uint64) (ChunkHead, error)
}
