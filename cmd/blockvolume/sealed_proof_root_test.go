package main

import (
	"context"
	"testing"

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
