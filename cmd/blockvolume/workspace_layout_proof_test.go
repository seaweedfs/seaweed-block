package main

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

type layoutProofStore struct {
	h     layout.Head
	found bool
	err   error
}

func (s layoutProofStore) ReadHead(context.Context) (layout.Head, bool, error) {
	return s.h, s.found, s.err
}
func (s layoutProofStore) UpdateHead(context.Context, uint64, layout.Head) error {
	return errors.New("proof must not write")
}

func TestWorkspaceLayoutDetachedProof(t *testing.T) {
	me := layout.HolderID{Epoch: 2, Holder: "s/r", ClaimID: "k"}
	id := &control.WorkspaceIdentity{VolumeId: "v", ServerId: "s", ReplicaId: "r", Epoch: 2}
	h := layout.Head{Gen: 7, Epoch: 2, Holder: "s/r", ClaimID: "k", State: layout.StateDetached}
	for _, tc := range []struct {
		name string
		st   layoutProofStore
		want bool
	}{
		{"exact", layoutProofStore{h, true, nil}, true},
		{"absent", layoutProofStore{}, false},
		{"unknown", layoutProofStore{err: errors.New("read lost")}, false},
		{"live", layoutProofStore{layout.Head{Gen: 7, Epoch: 2, Holder: "s/r", ClaimID: "k", State: layout.StateLive}, true, nil}, false},
		{"other claim", layoutProofStore{layout.Head{Gen: 7, Epoch: 2, Holder: "s/r", ClaimID: "other", State: layout.StateDetached}, true, nil}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gen, err := workspaceLayoutDetachedGeneration(context.Background(), tc.st, id, me)
			if (err == nil) != tc.want || (tc.want && gen != 7) || (!tc.want && gen != 0) {
				t.Fatalf("gen=%d err=%v", gen, err)
			}
		})
	}
}
