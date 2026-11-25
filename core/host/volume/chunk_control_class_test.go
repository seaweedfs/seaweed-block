package volume

import (
	"context"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
)

// An invalid class is refused before anything cuts or writes, and as a
// client error (400).
func TestChunkControl_InvalidClassRefusedBeforeCut(t *testing.T) {
	ctx := context.Background()
	fp, ctl := adapterFixture(t)
	_, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "r1", Class: "golden"})
	if chunkstore.CodeOf(err) != chunkstore.CodeInvalidClass {
		t.Fatalf("invalid class: %v; want invalid_class", err)
	}
	if fp.cuts != 0 {
		t.Fatalf("an invalid class cut %d times", fp.cuts)
	}
	if got, _, err := ctl.ListSnapshots(ctx, "v1", chunkstore.ListOptions{}); err != nil || len(got) != 0 {
		t.Fatalf("an invalid class left snapshots %v (%v)", got, err)
	}
	for _, code := range []string{chunkstore.CodeInvalidClass, chunkstore.CodeInvalidPolicy} {
		if s := catalogStatus(code); s != http.StatusBadRequest {
			t.Fatalf("%s maps to %d, want 400", code, s)
		}
	}

}
