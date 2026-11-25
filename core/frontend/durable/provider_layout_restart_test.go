package durable_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"sync"
	"testing"
	"time"
)

type restartStartStore struct {
	layout.StartStore
	failClosure bool
	closureSeen bool
}

func (s *restartStartStore) UpdateHead(ctx context.Context, gen uint64, next layout.Head) error {
	if next.Activity.Kind == layout.ActIdle && s.failClosure {
		s.closureSeen = true
		return errors.New("fixture: conditional Start closure failed")
	}
	if next.Activity.Kind == layout.ActIdle {
		s.closureSeen = true
	}
	return s.StartStore.UpdateHead(ctx, gen, next)
}

type heldRestartDetach struct {
	layout.StartStore
	held, release chan struct{}
}

func (s *heldRestartDetach) UpdateHead(ctx context.Context, gen uint64, next layout.Head) error {
	if next.State == layout.StateDetached {
		close(s.held)
		<-s.release
	}
	return s.StartStore.UpdateHead(ctx, gen, next)
}

// This uses the real provider open/close/reopen and kernel commit paths;
// process SIGKILL/systemd qualification is a separate real-cluster gate.
func TestLayoutRestartClosesActivityBeforeGuestWrites(t *testing.T) {
	for _, abortFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "held detach", true: "Start closure failure"}[abortFails], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), &layoutProviderBlobs{blobs: map[string][]byte{}}, "/restart")
			if err != nil {
				t.Fatal(err)
			}
			id, _ := layout.NewVolumeID()
			ref := chunkstore.VolumeRef{Name: "v1", ID: id}
			g := layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}
			if _, err := cat.CreateVolume(ctx, ref, g); err != nil {
				t.Fatal(err)
			}
			st, _ := cat.Store(ref)
			dir, locks := t.TempDir(), t.TempDir()
			view := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}))
			startStore := &restartStartStore{StartStore: st}
			open := func(root string) (*durable.DurableProvider, *storage.WALStore) {
				p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r1", HostVolumeLock: true, VolumeLockDirTestOnly: locks,
					ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return startStore, nil },
					ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
						return chunkstore.NewLayoutRemote(cat, ref, me)
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
			old, ws := open(dir)
			claim, _, _ := ws.LayoutIdentity()
			me := layout.HolderID{Epoch: 1, Holder: "s/r1", ClaimID: claim}
			if _, err := layout.Start(ctx, st, restartLease{}, me, layout.LocalRoot{Geometry: g, ClaimID: claim, Fresh: true, CanStartFrom: anyStart}); err != nil {
				t.Fatal(err)
			}
			if _, err := layout.RunBegin(ctx, st, me, "old-stop"); err != nil {
				t.Fatal(err)
			}
			root, _, _ := layout.Build(nil)
			rn, raw, _ := layout.NameRoot(root)
			if err := layout.EnsureObject(ctx, st, rn.Path(), raw, "old-stop"); err != nil {
				t.Fatal(err)
			}
			held := &heldRestartDetach{StartStore: st, held: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(held.release) }) }
			defer release()
			done := make(chan error, 1)
			go func() {
				_, err := layout.RunCommit(ctx, held, st, "v1", me, layout.CommitRequest{Session: "old-stop", State: layout.StateDetached})
				done <- err
			}()
			select {
			case <-held.held:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			} // releases local store + host lock
			startStore.closureSeen = false
			startStore.failClosure = abortFails
			p, newWS := open(dir)
			_, err = p.RecoverVolume(ctx, "v1")
			if !startStore.closureSeen {
				t.Fatal("Start did not conditionally close the old activity")
			}
			if abortFails {
				if err == nil {
					t.Fatal("failed Start closure allowed attach")
				}
				if _, err := newWS.Write(0, bytes.Repeat([]byte{'B'}, 4096)); !errors.Is(err, storage.ErrLayoutFresh) {
					t.Fatal("failed attach enabled writes", err)
				}
				release()
				<-done
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := newWS.Write(0, bytes.Repeat([]byte{'B'}, 4096)); err != nil {
				t.Fatal(err)
			}
			if _, err := newWS.Sync(); err != nil {
				t.Fatal(err)
			}
			release()
			if err := <-done; !errors.Is(err, layout.ErrPrecondition) {
				t.Fatal("late detached commit was accepted", err)
			}
			h, found, err := st.ReadHead(ctx)
			if err != nil || !found || h.State != layout.StateLive {
				t.Fatal("HEAD did not stay live", h.State, err)
			}
			if _, _, err := p.DetachVolumePlain(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 2}))
			successor, targetWS := open(t.TempDir())
			if _, err := successor.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			if got, err := targetWS.Read(0); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{'B'}, 4096)) {
				t.Fatal("acknowledged write lost across clean successor", err)
			}
		})
	}
}

type restartLease struct{}

func (restartLease) OldLeaseEnded(context.Context, uint64) error { return nil }
