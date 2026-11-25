package durable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type layoutCrashBlobs struct {
	sync.Mutex
	b map[string][]byte
}

func (b *layoutCrashBlobs) Put(_ context.Context, raw []byte) (string, error) {
	b.Lock()
	defer b.Unlock()
	id := fmt.Sprint(len(b.b) + 1)
	b.b[id] = append([]byte(nil), raw...)
	return id, nil
}
func (b *layoutCrashBlobs) Get(_ context.Context, id string) ([]byte, error) {
	b.Lock()
	defer b.Unlock()
	raw, ok := b.b[id]
	if !ok {
		return nil, chunkstore.ErrBlobNotFound
	}
	return append([]byte(nil), raw...), nil
}

type layoutCrashView struct{ epoch uint64 }

func (v layoutCrashView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: v.epoch, EndpointVersion: 1, Healthy: true}
}

func TestLayoutStartCrashAtEachDurableArrow(t *testing.T) {
	for _, restore := range []bool{false, true} {
		for _, point := range []string{"identity", "claim", "base", "fresh clear"} {
			t.Run(fmt.Sprintf("restore=%v/%s", restore, point), func(t *testing.T) {
				ctx := context.Background()
				f := chunkstore.NewMemFiler()
				cat, err := chunkstore.NewBCatalog(f, &layoutCrashBlobs{b: map[string][]byte{}}, "/crash")
				if err != nil {
					t.Fatal(err)
				}
				id, err := layout.NewVolumeID()
				if err != nil {
					t.Fatal(err)
				}
				ref := chunkstore.VolumeRef{Name: "v", ID: id}
				if _, err = cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
					t.Fatal(err)
				}
				makeP := func(root string, epoch uint64) (*DurableProvider, *storage.WALStore) {
					t.Helper()
					p, err := NewDurableProvider(ProviderConfig{Impl: ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r", ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return cat.Store(ref) }, ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
						return chunkstore.NewLayoutRemote(cat, ref, me)
					}}, layoutCrashView{epoch})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = p.Close() })
					s, err := p.EnsureStorage("v")
					if err != nil {
						t.Fatal(err)
					}
					return p, s.(*storage.WALStore)
				}
				epoch := uint64(1)
				want := make([]byte, 4096)
				if restore {
					source, ws := makeP(t.TempDir(), 1)
					if _, err = source.RecoverVolume(ctx, "v"); err != nil {
						t.Fatal(err)
					}
					want = bytes.Repeat([]byte{'A'}, 4096)
					if _, err = ws.Write(0, want); err != nil {
						t.Fatal(err)
					}
					if _, err = ws.Sync(); err != nil {
						t.Fatal(err)
					}
					if _, err = source.CutVolume(ctx, "v", 0); err != nil {
						t.Fatal(err)
					}
					if err = source.Close(); err != nil {
						t.Fatal(err)
					}
					epoch = 2
				}
				root := t.TempDir()
				p, ws := makeP(root, epoch)
				claim, fresh, ok := ws.LayoutIdentity()
				if !ok || !fresh || claim == "" {
					t.Fatal("identity not durable before Start")
				}
				crash := errors.New("fixture: interrupted at " + point)
				switch point {
				case "identity":
					p.cfg.ChunkLayoutHeadFor = func(string) (layout.StartStore, error) { return nil, crash }
				case "claim":
					p.cfg.ChunkLayoutRemoteFor = func(string, layout.HolderID) (storage.ChunkRemote, error) { return nil, crash }
				case "base":
					p.testBeforeChunkFreshRetire = func() error { return crash }
				case "fresh clear":
					p.testAfterChunkFreshRetire = func() error { return crash }
				}
				if _, err = p.RecoverVolume(ctx, "v"); !errors.Is(err, crash) {
					t.Fatalf("interruption: %v", err)
				}
				if err := p.volumes["v"].backend.readyCheck(); !errors.Is(err, frontend.ErrNotReady) {
					t.Fatalf("interrupted Start admitted guest I/O: %v", err)
				}
				_, fresh, _ = ws.LayoutIdentity()
				if fresh != (point != "fresh clear") {
					t.Fatalf("fresh=%v at %s", fresh, point)
				}
				if fresh {
					if _, err = ws.Write(0, want); !errors.Is(err, storage.ErrLayoutFresh) {
						t.Fatalf("guest write before durable clear: %v", err)
					}
				}
				if err = p.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, ws := makeP(root, epoch)
				if got, _, _ := ws.LayoutIdentity(); got != claim {
					t.Fatal("restart minted a second claim id")
				}
				if _, err = reopened.RecoverVolume(ctx, "v"); err != nil {
					t.Fatal(err)
				}
				if _, fresh, _ = ws.LayoutIdentity(); fresh {
					t.Fatal("Start served before fresh clear")
				}
				got := make([]byte, 4096)
				n, err := reopened.volumes["v"].backend.Read(ctx, 0, got)
				if err != nil || n != len(got) || !bytes.Equal(got, want) {
					t.Fatalf("guest base read: n=%d err=%v bytes match=%v", n, err, bytes.Equal(got, want))
				}
				if n, err := reopened.volumes["v"].backend.Write(ctx, 4096, bytes.Repeat([]byte{'B'}, 4096)); err != nil || n != 4096 {
					t.Fatalf("guest write after Start: n=%d err=%v", n, err)
				}
				if _, err = ws.Sync(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
