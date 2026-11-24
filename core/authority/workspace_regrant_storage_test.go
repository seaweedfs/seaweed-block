package authority

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type regrantStorageView struct{ epoch uint64 }

func (v regrantStorageView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: v.epoch, Healthy: true}
}

// These component controls obtain the real authority re-grant before opening
// the returning file-backed provider. They do not model a second live process.
func TestWorkspaceLeaseRegrantPreservesLocalLogThroughFirstCut(t *testing.T) {
	testWorkspaceRegrantStorage(t, false, false)
}

func TestWorkspaceLeaseRegrantCannotUnsealSameEpoch(t *testing.T) {
	testWorkspaceRegrantStorage(t, true, false)
}

func TestWorkspaceLeaseRegrantEmptyRootRequiresForcedMove(t *testing.T) {
	testWorkspaceRegrantStorage(t, false, true)
}

func testWorkspaceRegrantStorage(t *testing.T, sealed, fresh bool) {
	t.Helper()
	ctx := context.Background()
	disk, _ := newTempStore(t)
	clock := newTestClock()
	p := newLeasePublisher(t, disk, time.Second, clock)
	bindAndGrant(t, p, "v", "r")
	remote := storage.NewMemChunkRemote()
	root := t.TempDir()
	epoch := uint64(1)
	open := func() *durable.DurableProvider {
		t.Helper()
		provider, err := durable.NewDurableProvider(durable.ProviderConfig{
			Impl: durable.ImplWALStoreChunk, StorageRoot: root, NumBlocks: 64,
			BlockSize: 4096, ChunkBlocks: 4,
			ChunkHolder:    "host/r",
			ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
		}, regrantStorageView{epoch})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = provider.Close() })
		if _, err := provider.EnsureStorage("v"); err != nil {
			t.Fatal(err)
		}
		return provider
	}
	first := open()
	if _, err := first.RecoverVolume(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	backend, err := first.Open(ctx, "v")
	if err != nil {
		t.Fatal(err)
	}
	a, b := bytes.Repeat([]byte{0x31}, 4096), bytes.Repeat([]byte{0x42}, 4096)
	write := func(offset int64, data []byte) {
		t.Helper()
		if n, err := backend.Write(ctx, offset, data); err != nil || n != len(data) {
			t.Fatal(n, err)
		}
		if err := backend.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	write(0, a)
	if _, err := first.CaptureVolume(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	write(4096, b) // completed Sync, deliberately absent from the last HEAD
	if sealed {
		if _, _, err := first.DetachVolumePlain(ctx, "v"); err != nil {
			t.Fatal(err)
		}
	}
	before, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	clock.at(3) // beyond both original expiry/skew endpoints
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal("returning process could not obtain the authority re-grant", err)
	}
	if fresh {
		root = t.TempDir()
	}
	second := open()
	_, err = second.RecoverVolume(ctx, "v")
	if fresh {
		if !errors.Is(err, durable.ErrChunkMissingLocalRoot) {
			t.Fatalf("re-grant silently discarded the local suffix: %v", err)
		}
		if after, err := remote.GetHead(ctx); err != nil || !reflect.DeepEqual(after, before) {
			t.Fatal("refused re-grant changed HEAD", after, err)
		}
		if err := second.Backend("v").AdmitsIO(); err == nil {
			t.Fatal("refused empty root serves")
		}
		if err := second.Close(); err != nil {
			t.Fatal(err)
		}
		q := WorkspaceMoveRequest{RequestID: "recover-here", VolumeID: "v", ExpectedReplicaID: "r", ExpectedHost: "host", TargetHost: "host", ExpectedEpoch: 1, TargetReplicaID: "r", TargetDataAddr: "d", TargetCtrlAddr: "c", Kind: "forced", Force: true}
		if _, err := p.MoveWorkspace(ctx, q, WorkspaceHead{Gen: before.Gen, Epoch: before.Epoch, Holder: before.Holder, RemoteLSN: before.RemoteLSN}); err == nil {
			t.Fatal("same-host force bypassed the renewed lease")
		}
		clock.at(86400)
		grant, moveErr := p.MoveWorkspace(ctx, q, WorkspaceHead{Gen: before.Gen, Epoch: before.Epoch, Holder: before.Holder, RemoteLSN: before.RemoteLSN})
		if moveErr != nil {
			t.Fatal(moveErr)
		}
		lost := p.WorkspaceLostRanges("v")
		if len(lost) != 1 || lost[0].NewEpoch != grant.NewEpoch || lost[0].LostAfterLSN != before.RemoteLSN {
			t.Fatal("forced successor omitted its loss record", lost)
		}
		epoch = grant.NewEpoch
		second = open()
		_, err = second.RecoverVolume(ctx, "v")
		b = make([]byte, 4096) // absent suffix is now explicitly recorded as lost
	}
	if sealed {
		after, readErr := remote.GetHead(ctx)
		if !errors.Is(err, storage.ErrChunkLayerDetached) || readErr != nil || !reflect.DeepEqual(after, before) {
			t.Fatal("same-epoch re-grant reactivated the seal", after, err, readErr)
		}
		blocked, openErr := second.Open(ctx, "v")
		if openErr != nil {
			t.Fatal(openErr)
		}
		if _, readErr := blocked.Read(ctx, 0, make([]byte, 4096)); !errors.Is(readErr, frontend.ErrNotReady) {
			t.Fatal("sealed re-granted process exposed guest I/O", readErr)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	returning, err := second.Open(ctx, "v")
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range [][]byte{a, b} {
		got := make([]byte, 4096)
		if _, err := returning.Read(ctx, int64(i*4096), got); err != nil || !bytes.Equal(got, want) {
			t.Fatal("return lost locally acknowledged bytes", i, err)
		}
	}
	claimed, err := remote.GetHead(ctx)
	if err != nil || claimed.Gen <= before.Gen || claimed.Epoch != epoch || claimed.Holder != before.Holder || claimed.RemoteLSN != before.RemoteLSN {
		t.Fatal("return served without the new generation fence", claimed, err)
	}
	if _, err := second.CaptureVolume(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	final, err := remote.GetHead(ctx)
	if fresh {
		if err != nil || !reflect.DeepEqual(final, claimed) {
			t.Fatal("empty-root return published bytes/frontier absent from HEAD", final, err)
		}
		return
	}
	if err != nil || final.RemoteLSN <= before.RemoteLSN {
		t.Fatal("first returning cut did not cover the local log", final, err)
	}
	payload, err := remote.Get(ctx, final.Chunks[0].Key)
	if err != nil || len(payload) < 8192 || !bytes.Equal(payload[:4096], a) || !bytes.Equal(payload[4096:8192], b) {
		t.Fatal("first cut did not publish the full acknowledged image", err)
	}
}
