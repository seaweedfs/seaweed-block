package master

import (
	"bytes"
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type restoreProjection struct{ projection frontend.Projection }

func (v restoreProjection) Projection() frontend.Projection { return v.projection }

func layoutWorkspaceHead(h layout.Head) authority.WorkspaceHead {
	return authority.WorkspaceHead{Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.Position, Detached: h.State == layout.StateDetached}
}

type restoreRemote struct {
	*chunkstore.LayoutRemote
	gets *atomic.Uint64
}

func (r *restoreRemote) Get(ctx context.Context, key string) ([]byte, error) {
	r.gets.Add(1)
	return r.LayoutRemote.Get(ctx, key)
}

// This CPU component loop uses a real master authority runtime, two real
// temporary-file providers and the existing in-memory ChunkRemote boundary.
// Keep the old projection stale to model delayed assignment delivery; a HEAD
// takeover must independently refuse its cut and then stop its frontend I/O.
func TestWorkspaceMoveRestoresLazilyAndFencesOldHost(t *testing.T) {
	// No wall-clock bound (#227): each step completes or the go test timeout
	// reports the hang; a 10 s context failed the HEAD claim under load.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, m)
	seedVerifiedExistingReplicaPlacement(t, m)
	if _, err := m.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	oldLine := waitAuthorityLine(t, m.Publisher(), "vol-a")
	establishVolume(t, m, "vol-a")
	fixture := testutil.NewLayoutFixture(t, "vol-a", 64, 4)
	var gets atomic.Uint64
	readHead := func() layout.Head {
		t.Helper()
		h, found, err := fixture.Catalog.ReadHead(ctx, fixture.Ref)
		if err != nil || !found {
			t.Fatalf("layout HEAD: found=%v err=%v", found, err)
		}
		return h
	}
	open := func(line authority.AuthorityBasis) (*durable.DurableProvider, frontend.Backend) {
		t.Helper()
		view := restoreProjection{frontend.Projection{VolumeID: "vol-a", ReplicaID: line.ReplicaID,
			Epoch: line.Epoch, EndpointVersion: line.EndpointVersion, Healthy: true}}
		cfg := durable.ProviderConfig{
			Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64,
			BlockSize: 4096, ChunkBlocks: 4,
		}
		fixture.Configure(&cfg)
		cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			inner, err := chunkstore.NewLayoutRemote(fixture.Catalog, fixture.Ref, me)
			if err != nil {
				return nil, err
			}
			return &restoreRemote{LayoutRemote: inner, gets: &gets}, nil
		}
		p, err := durable.NewDurableProvider(cfg, view)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		if _, err := p.EnsureStorage("vol-a"); err != nil {
			t.Fatal(err)
		}
		if _, err := p.RecoverVolume(ctx, "vol-a"); err != nil {
			t.Fatal(err)
		}
		backend, err := p.Open(ctx, "vol-a")
		if err != nil {
			t.Fatal(err)
		}
		return p, backend
	}
	oldProvider, oldBackend := open(oldLine)
	want := map[int64][]byte{0: bytes.Repeat([]byte{0xA1}, 4096), 8 * 4096: bytes.Repeat([]byte{0xB2}, 4096)}
	for offset, data := range want {
		if n, err := oldBackend.Write(ctx, offset, data); err != nil || n != len(data) {
			t.Fatal(n, err)
		}
	}
	if err := oldBackend.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	cut, err := oldProvider.CaptureVolume(ctx, "vol-a")
	if err != nil {
		t.Fatal(err)
	}
	headBefore := readHead()
	if headBefore.Position != cut || headBefore.Current == nil {
		t.Fatalf("initial HEAD=%+v", headBefore)
	}

	// The authority runtime, not either provider, allocates the next epoch.
	// Admission evidence is supplied by this fixture, as in the runtime's
	// existing tests; this is not a VM-stop or physical fencing qualification.
	// The planned move decides on the promotion gate (its fixture, too).
	installPlannedMoveGate(m)
	recordFailbackTargetR1(t, m)
	if _, err := m.FailbackAuthorityRuntime().ExecuteFailback(ctx, authority.FailbackRuntimeRequest{
		VolumeID: "vol-a", ReplicaID: "r1", TargetDataAddr: "127.0.0.1:9201", TargetCtrlAddr: "127.0.0.1:9101",
		ExpectedCurrentReplicaID: oldLine.ReplicaID, ExpectedCurrentEpoch: oldLine.Epoch,
		AckEligible: true, FrontendFencedBeforeFailback: true,
		DurableFrontierCovered: true, NoCrossVolumeIdentityChange: true,
	}); err != nil {
		t.Fatal(err)
	}
	newLine, ok := m.Publisher().VolumeAuthorityLine("vol-a")
	if !ok || newLine.ReplicaID != "r1" || newLine.Epoch <= oldLine.Epoch {
		t.Fatalf("master did not move authority: %+v", newLine)
	}
	beforeGets := gets.Load()
	freshProvider, freshBackend := open(newLine) // unrelated empty local directory
	if gets.Load() != beforeGets {
		t.Error("restore eagerly fetched chunk bodies before any read")
	}
	claimed := readHead()
	if claimed.Epoch != newLine.Epoch || claimed.Holder != newLine.ReplicaID || claimed.Position != cut {
		t.Errorf("restore served before HEAD takeover: HEAD epoch=%d holder=%q cut=%d; want master epoch=%d holder=%q cut=%d",
			claimed.Epoch, claimed.Holder, claimed.Position, newLine.Epoch, newLine.ReplicaID, cut)
	}
	for offset, data := range want {
		got := make([]byte, len(data))
		if n, err := freshBackend.Read(ctx, offset, got); err != nil || n != len(got) || !bytes.Equal(got, data) {
			t.Fatalf("fresh exact read offset=%d n=%d err=%v", offset, n, err)
		}
	}
	if gets.Load() <= beforeGets {
		t.Error("fresh reads did not lazily fetch HEAD chunks")
	}
	// With assignment delivery held, the stale host can still have local dirt.
	if n, err := oldBackend.Write(ctx, 0, bytes.Repeat([]byte{0xCC}, 4096)); err != nil || n != 4096 {
		t.Fatal(n, err)
	}
	if _, err := oldProvider.CaptureVolume(ctx, "vol-a"); err == nil {
		t.Error("old host cut succeeded after new host restored")
	}
	if n, err := oldBackend.Read(ctx, 0, make([]byte, 4096)); err == nil || n != 0 {
		t.Errorf("old host kept serving after its refused cut: n=%d err=%v", n, err)
	}
	if n, err := oldBackend.Write(ctx, 0, make([]byte, 4096)); err == nil || n != 0 {
		t.Errorf("old host kept accepting writes after its refused cut: n=%d err=%v", n, err)
	}
	if err := oldBackend.Sync(ctx); err == nil {
		t.Error("old host kept acknowledging Sync after its refused cut")
	}
	afterStale := readHead()
	if !reflect.DeepEqual(afterStale, claimed) {
		t.Errorf("stale host changed claimed HEAD: %+v", afterStale)
	}
	// A new local WAL must continue after HEAD, not restart its LSN at 1.
	want[4*4096] = bytes.Repeat([]byte{0xD4}, 4096)
	if n, err := freshBackend.Write(ctx, 4*4096, want[4*4096]); err != nil || n != 4096 {
		t.Fatal(n, err)
	}
	if err := freshBackend.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	newCut, err := freshProvider.CaptureVolume(ctx, "vol-a")
	if err != nil {
		t.Fatal(err)
	}
	if newCut <= cut {
		t.Errorf("restored WAL did not advance beyond HEAD: old=%d new=%d", cut, newCut)
	}
	newHead := readHead()
	final := readHead()
	if final.Epoch != newLine.Epoch || final.Holder != newLine.ReplicaID || final.Position != newCut || final.Gen != newHead.Gen || !reflect.DeepEqual(final.Current, newHead.Current) {
		t.Errorf("old host changed restored HEAD: %+v", final)
	}
	for offset, data := range want {
		got := make([]byte, len(data))
		if n, err := freshBackend.Read(ctx, offset, got); err != nil || n != len(got) || !bytes.Equal(got, data) {
			t.Fatalf("new host bytes changed after stale cut: offset=%d n=%d err=%v", offset, n, err)
		}
	}
}
