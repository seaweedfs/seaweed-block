package main

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// INV-WS-LOCAL-ROOT-IDENTITY-001 / V1-SPEC r2.38: the revocation ACK's
// detached proof is (server, replica, root): root 1 of holder s/r sealed;
// root 2 of the same holder asked to ACK is refused; root 1 is accepted.
func TestRevocationAckProofComparesTheLocalRoot(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	gen, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "s/r", HolderRoot: "root1", State: storage.ChunkHeadActive})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{Gen: gen, Epoch: 1}, storage.ChunkHead{Epoch: 1, Holder: "s/r", HolderRoot: "root1", State: storage.ChunkHeadDetached,
		DetachedBy: &storage.ChunkDetachedBy{Holder: "s/r", HolderRoot: "root1", Epoch: 1, Gen: gen + 1}}); err != nil {
		t.Fatal(err)
	}
	id := &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}
	remoteFor := func(string) (storage.ChunkRemote, error) { return remote, nil }
	if _, err := workspaceDetachedGeneration(ctx, remoteFor, id, "s/r", "root2"); err == nil {
		t.Fatal("another root of the same holder ACKed root 1's seal")
	}
	if got, err := workspaceDetachedGeneration(ctx, remoteFor, id, "s/r", "root1"); err != nil || got != gen+1 {
		t.Fatalf("the sealing root's own ACK: gen=%d err=%v, want gen %d", got, err, gen+1)
	}
}

// The clean drain's real order (V1-SPEC r2.38): the provider seals (plain
// detach), the shutdown closes the provider, and only then is the
// revocation ACK's proof checked. The root nonce is read before Close
// forgets the volume (closeDurableKeepingRoot), so the ACK succeeds with
// this root: a planned hand-over completes.
func TestCleanDrainAckKeepsTheRootAcrossProviderClose(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	view := &claimExitView{}
	view.assigned.Store(true)
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, ChunkHolder: "s/r", StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.EnsureStorage("v"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	sealed, _, err := p.DetachVolumePlain(ctx, "v")
	if err != nil || sealed.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("seal: %+v %v", sealed.Head, err)
	}
	root, err := closeDurableKeepingRoot(p, "v")
	if err != nil || root == "" || root != sealed.Head.HolderRoot {
		t.Fatalf("root across Close: %q err=%v, want the sealing root %q", root, err, sealed.Head.HolderRoot)
	}
	if _, err := p.LocalRootNonce("v"); err == nil {
		t.Fatal("the provider still knows the volume after Close: the order is not exercised")
	}
	id := &control.WorkspaceIdentity{ServerId: "s", VolumeId: "v", ReplicaId: "r", Epoch: sealed.Head.Epoch}
	gen, err := workspaceDetachedGeneration(ctx, func(string) (storage.ChunkRemote, error) { return remote, nil }, id, "s/r", root)
	if err != nil || gen != sealed.Head.Gen {
		t.Fatalf("clean drain ACK proof: gen=%d err=%v, want gen %d", gen, err, sealed.Head.Gen)
	}
}
