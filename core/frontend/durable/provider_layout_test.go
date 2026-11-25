package durable_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type layoutProviderBlobs struct {
	sync.Mutex
	blobs map[string][]byte
}

func (b *layoutProviderBlobs) Put(_ context.Context, raw []byte) (string, error) {
	b.Lock()
	defer b.Unlock()
	id := fmt.Sprint(len(b.blobs) + 1)
	b.blobs[id] = append([]byte(nil), raw...)
	return id, nil
}
func (b *layoutProviderBlobs) Get(_ context.Context, id string) ([]byte, error) {
	b.Lock()
	defer b.Unlock()
	raw, ok := b.blobs[id]
	if !ok {
		return nil, chunkstore.ErrBlobNotFound
	}
	return append([]byte(nil), raw...), nil
}

func TestLayoutProviderRefusesEmptyOwnEpochAndRemountedOldRoot(t *testing.T) {
	ctx := context.Background()
	f := chunkstore.NewMemFiler()
	cat, err := chunkstore.NewBCatalog(f, &layoutProviderBlobs{blobs: map[string][]byte{}}, "/layout-test")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	v := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err = cat.CreateVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	view := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}))
	remoteCalls := 0
	var createdRemote storage.ChunkRemote
	lostReply := false
	makeProvider := func(root string) (*durable.DurableProvider, *storage.WALStore) {
		t.Helper()
		p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r1",
			ChunkLayoutHeadFor: func(string) (layout.HeadStore, error) {
				st, err := cat.Store(v)
				if err != nil {
					return nil, err
				}
				return &layoutLostClaimReply{HeadStore: st, lose: &lostReply}, nil
			},
			ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
				remoteCalls++
				r, err := chunkstore.NewLayoutRemote(cat, v, me)
				createdRemote = r
				return r, err
			},
		}, view)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		s, err := p.EnsureStorage("v1")
		if err != nil {
			t.Fatal(err)
		}
		return p, s.(*storage.WALStore)
	}
	oldRoot := t.TempDir()
	p, ws := makeProvider(oldRoot)
	claim, fresh, ok := ws.LayoutIdentity()
	if !ok || !fresh || claim == "" {
		t.Fatalf("identity not durable before claim: %q %v %v", claim, fresh, ok)
	}
	lostReply = true
	if _, err = p.RecoverVolume(ctx, "v1"); err == nil {
		t.Fatal("claim reply loss not observed")
	}
	if got, _, _ := ws.LayoutIdentity(); got != claim {
		t.Fatal("claim identity changed after lost reply")
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	me, err := p.LayoutHolder(ctx, "v1")
	if err != nil || me.ClaimID != claim || me.Epoch != 1 {
		t.Fatalf("handover=%+v %v", me, err)
	}
	for i := 0; i < 2; i++ {
		if got, err := p.LayoutRemote(ctx, "v1"); err != nil || got != createdRemote {
			t.Fatalf("cached remote changed: %T %v", got, err)
		}
	}
	if _, fresh, _ = ws.LayoutIdentity(); fresh {
		t.Fatal("serving with a fresh descriptor")
	}
	if files, _ := filepath.Glob(filepath.Join(oldRoot, "*.chunkfresh")); len(files) != 0 {
		t.Fatal("created a second identity marker", files)
	}
	if _, err = ws.Write(0, bytes.Repeat([]byte{'A'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err = p.CutVolume(ctx, "v1", 0); err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	// A newly created directory at the same grant cannot invent continuity.
	empty, _ := makeProvider(t.TempDir())
	if _, err = empty.RecoverVolume(ctx, "v1"); !errors.Is(err, layout.ErrMissingLocalRoot) {
		t.Fatalf("empty own epoch: %v", err)
	}
	if _, err = empty.LayoutHolder(ctx, "v1"); err == nil {
		t.Fatal("failed Start exposed cleaning identity")
	}
	if _, err = empty.LayoutRemote(ctx, "v1"); err == nil {
		t.Fatal("failed Start exposed a remote")
	}
	if err = empty.Close(); err != nil {
		t.Fatal(err)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 2}))
	successor, newWS := makeProvider(t.TempDir())
	if _, err = successor.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if got, err := newWS.Read(0); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{'A'}, 4096)) {
		t.Fatalf("restore lost published bytes: %v", err)
	}
	if _, err = newWS.Write(0, bytes.Repeat([]byte{'B'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err = newWS.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err = successor.CutVolume(ctx, "v1", 0); err != nil {
		t.Fatal(err)
	}
	if err = successor.Close(); err != nil {
		t.Fatal(err)
	}
	remounted, _ := makeProvider(oldRoot)
	if _, err = remounted.RecoverVolume(ctx, "v1"); !errors.Is(err, layout.ErrStaleLocalRoot) {
		t.Fatalf("old directory served new lineage: %v", err)
	}
	if remoteCalls != 2 {
		t.Fatalf("remote constructed before Start/duplicated: %d", remoteCalls)
	}
}

type layoutLostClaimReply struct {
	layout.HeadStore
	lose *bool
}

func (s *layoutLostClaimReply) UpdateHead(ctx context.Context, expected uint64, next layout.Head) error {
	if err := s.HeadStore.UpdateHead(ctx, expected, next); err != nil {
		return err
	}
	if *s.lose {
		*s.lose = false
		return errors.New("fixture: committed claim reply lost")
	}
	return nil
}
