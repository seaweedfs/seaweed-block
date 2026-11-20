package storage

import "context"

// ChunkSegment names an immutable range manifest. Owner is the namespace in
// which that manifest was encoded; forks preserve it without rewriting data.
type ChunkSegment struct {
	Key    string   `json:"key"`
	SHA256 [32]byte `json:"sha256"`
	Owner  string   `json:"owner"`
	Count  int      `json:"count"`
}

// ChunkManifestBuilder prepares immutable segments before the existing root
// CAS. Returned intents belong to the cut and follow its settlement rules.
type ChunkManifestBuilder interface {
	PrepareManifest(context.Context, ChunkHead, ChunkHead, uint64) (ChunkHead, []string, error)
}
