package chunkstore

import (
	"context"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestHEADRefusesLSNRegressionAcrossMove(t *testing.T) {
	ctx := context.Background()
	r := newTestRemote(t, newFakeFiler(), newFakeBlobs())
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, head(1, "old", 42, nil)); err != nil {
		t.Fatal(err)
	}
	before, _ := r.GetHead(ctx)
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 1}, head(2, "new", 41, nil)); !isPrecondition(err) {
		t.Fatalf("regressing takeover accepted: %v", err)
	}
	after, _ := r.GetHead(ctx)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("refused HEAD changed")
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 1}, head(2, "new", 42, nil)); err != nil {
		t.Fatal(err)
	}
}
