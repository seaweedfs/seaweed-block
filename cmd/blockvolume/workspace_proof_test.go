package main

import (
	"context"
	"errors"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
)

type workspaceProofRemote struct {
	storage.ChunkRemote
	head  storage.ChunkHead
	err   error
	reads int
}

func (r *workspaceProofRemote) GetHead(context.Context) (storage.ChunkHead, error) {
	r.reads++
	return r.head, r.err
}

func TestWorkspaceRevocationUsesExactStoredSeal(t *testing.T) {
	for _, name := range []string{"valid", "zero-epoch", "empty-holder", "active", "missing-proof", "proof-gen", "proof-epoch", "proof-holder", "foreign-holder", "foreign-epoch", "zero-gen", "read-error"} {
		t.Run(name, func(t *testing.T) {
			h := storage.ChunkHead{Gen: 7, Epoch: 2, Holder: "s/r", State: storage.ChunkHeadDetached, DetachedBy: &storage.ChunkDetachedBy{Gen: 7, Epoch: 2, Holder: "s/r"}}
			id := &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 2}
			holder := "s/r"
			r := &workspaceProofRemote{head: h}
			switch name {
			case "zero-epoch":
				id.Epoch = 0
				r.head.Epoch = 0
				r.head.DetachedBy.Epoch = 0
			case "empty-holder":
				holder = ""
				r.head.Holder = ""
				r.head.DetachedBy.Holder = ""
			case "active":
				r.head.State = storage.ChunkHeadActive
			case "missing-proof":
				r.head.DetachedBy = nil
			case "zero-gen":
				r.head.Gen = 0
				r.head.DetachedBy.Gen = 0
			case "proof-epoch":
				r.head.DetachedBy.Epoch = 1
			case "proof-holder":
				r.head.DetachedBy.Holder = "other/r"
			case "foreign-epoch":
				r.head.Epoch = 1
			case "proof-gen":
				r.head.DetachedBy.Gen = 6
			case "foreign-holder":
				r.head.Holder = "other/r"
			case "read-error":
				r.err = errors.New("HEAD answer lost")
			}
			gen, err := workspaceDetachedGeneration(context.Background(), func(string) (storage.ChunkRemote, error) { return r, nil }, id, holder)
			if name == "valid" {
				if err != nil || gen != 7 || r.reads != 1 {
					t.Fatalf("stored seal: gen%d reads%d err%v", gen, r.reads, err)
				}
			} else if err == nil || gen != 0 {
				t.Fatalf("unproven ACK gen%d err%v", gen, err)
			}
		})
	}
}
