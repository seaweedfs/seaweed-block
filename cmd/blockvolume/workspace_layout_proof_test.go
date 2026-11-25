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
	for _, tc := range []struct {
		name   string
		mutate func(*layoutProofStore, *control.WorkspaceIdentity, *layout.HolderID)
		want   bool
	}{
		{"exact", nil, true},
		{"absent", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) { s.found = false }, false},
		{"unknown", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) {
			s.err = errors.New("read lost")
		}, false},
		{"live", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) {
			s.h.State = layout.StateLive
		}, false},
		{"deleting", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) {
			s.h.State = layout.StateDeleting
		}, false},
		{"deleted", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) {
			s.h.State = layout.StateDeleted
		}, false},
		{"open activity", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) {
			s.h.Activity = layout.Activity{Kind: layout.ActSnap, Session: "old"}
		}, false},
		{"other claim", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) { s.h.ClaimID = "other" }, false},
		{"older HEAD epoch", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) { s.h.Epoch-- }, false},
		{"newer HEAD epoch", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) { s.h.Epoch++ }, false},
		{"other HEAD holder", func(s *layoutProofStore, _ *control.WorkspaceIdentity, _ *layout.HolderID) { s.h.Holder = "other/r" }, false},
		{"zero request epoch", func(_ *layoutProofStore, id *control.WorkspaceIdentity, _ *layout.HolderID) { id.Epoch = 0 }, false},
		{"other request server", func(_ *layoutProofStore, id *control.WorkspaceIdentity, _ *layout.HolderID) { id.ServerId = "other" }, false},
		{"other request replica", func(_ *layoutProofStore, id *control.WorkspaceIdentity, _ *layout.HolderID) { id.ReplicaId = "other" }, false},
		{"empty local claim", func(_ *layoutProofStore, _ *control.WorkspaceIdentity, me *layout.HolderID) { me.ClaimID = "" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			me := layout.HolderID{Epoch: 2, Holder: "s/r", ClaimID: "k"}
			id := &control.WorkspaceIdentity{VolumeId: "v", ServerId: "s", ReplicaId: "r", Epoch: 2}
			st := layoutProofStore{h: layout.Head{Gen: 7, Epoch: 2, Holder: "s/r", ClaimID: "k", State: layout.StateDetached, Activity: layout.Activity{Kind: layout.ActIdle}}, found: true}
			if tc.mutate != nil {
				tc.mutate(&st, id, &me)
			}
			gen, err := workspaceLayoutDetachedGeneration(context.Background(), st, id, me)
			if (err == nil) != tc.want || (tc.want && gen != 7) || (!tc.want && gen != 0) {
				t.Fatalf("gen=%d err=%v", gen, err)
			}
		})
	}
}
