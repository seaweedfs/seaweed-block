package chunkstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type attachLookupProjection struct{}

func (attachLookupProjection) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1, Healthy: true}
}

// V1-SPEC r2.55 gap 10; INV-WS-CLAIM-BEFORE-SERVE-001.
// Real gRPC lookup errors pass through SWFS and a fresh durable provider.
// An existing HEAD must never become a bootstrap claim or an I/O-ready volume.
func TestAttachLookupUnavailableIsNotAnAbsentHead(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.PermissionDenied, codes.Internal} {
		t.Run(code.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			head := storage.ChunkHead{Gen: 7, Epoch: 3, Holder: "node/r1", State: storage.ChunkHeadActive}
			entry, err := headEntry(head)
			if err != nil {
				t.Fatal(err)
			}
			f := &fakeFilerGRPC{lookupResp: &filerpb.LookupDirectoryEntryResponse{Entry: entry}}
			swfs := startFakeFiler(t, f)
			remote, err := NewChunkRemote(swfs, newFakeBlobs(), "/chunks/v1")
			if err != nil {
				t.Fatal(err)
			}
			before, err := remote.GetHead(ctx)
			if err != nil || before.Gen != 7 {
				t.Fatalf("pre-existing HEAD=%+v error=%v", before, err)
			}
			f.mu.Lock()
			// The absence sentinel inside an Unavailable message is still an error.
			f.lookupErr = status.Error(code, "lookup failed: "+filerNotFoundText)
			f.mu.Unlock()
			if h, err := remote.GetHead(ctx); err == nil || errors.Is(err, ErrNotFound) || status.Code(err) != code || !strings.Contains(err.Error(), "lookup failed: "+filerNotFoundText) {
				t.Fatalf("failed lookup became an absent HEAD: head=%+v error=%v", h, err)
			}
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 256, ChunkBlocks: 16,
				ChunkHolder:    "node/r1",
				ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
			}, attachLookupProjection{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Close() })
			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := p.RecoverVolume(ctx, "v1"); status.Code(err) != code || !strings.Contains(err.Error(), "lookup failed: "+filerNotFoundText) {
				t.Fatalf("attach error=%v; want preserved %s", err, code)
			}
			if err := p.AdmitsIO("v1"); err == nil {
				t.Fatal("failed HEAD lookup admitted guest I/O")
			}
			f.mu.Lock()
			created, updated := f.lastCreate, f.lastUpdate
			f.lookupErr = nil
			f.mu.Unlock()
			if created != nil || updated != nil {
				t.Fatalf("failed lookup attempted a HEAD CAS: create=%+v update=%+v", created, updated)
			}
			after, err := remote.GetHead(ctx)
			if err != nil || after.Gen != head.Gen || after.Holder != head.Holder || after.Epoch != head.Epoch {
				t.Fatalf("failed attach changed HEAD: %+v error=%v", after, err)
			}
		})
	}
}
