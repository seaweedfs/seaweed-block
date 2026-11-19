package chunkstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Metadata-only scaling probe: one chunk per 1-GiB segment, all unchanged.
// No volume data allocation or remote I/O is included. Report ns/op and allocs;
// the old per-group scan visits groups*groups base references.
func BenchmarkPrepareManifestUnchangedGroups(b *testing.B) {
	for _, n := range []int{256, 1024, 4096} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			r, err := NewChunkRemote(newFakeFiler(), newFakeBlobs(), "/chunks/v1")
			if err != nil {
				b.Fatal(err)
			}
			base := storage.ChunkHead{Chunks: map[uint64]storage.ChunkRef{}, Segments: map[uint64]storage.ChunkSegment{}, ChunkBytes: 1 << 30, ManifestChunkCount: n}
			for i := 0; i < n; i++ {
				base.Chunks[uint64(i)] = storage.ChunkRef{Key: fmt.Sprintf("@v1/chunk/%d", i)}
				base.Segments[uint64(i)] = storage.ChunkSegment{Key: fmt.Sprintf("@v1/seg/%d", i), Owner: "v1", Count: 1}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				next, ids, err := r.PrepareManifest(context.Background(), base, base, 1<<30)
				if err != nil || len(ids) != 0 || len(next.Segments) != n {
					b.Fatalf("unchanged segment reuse: %v, intents=%v", err, ids)
				}
			}
		})
	}
}
