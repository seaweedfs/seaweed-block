package storage

// ChunkSegment names an immutable range manifest. Owner is the namespace in
// which that manifest was encoded; forks preserve it without rewriting data.
type ChunkSegment struct {
	Key       string   `json:"key"`
	SHA256    [32]byte `json:"sha256"`
	Owner     string   `json:"owner"`
	Count     int      `json:"count"`
	SegFormat int      `json:"seg_format"`
}
