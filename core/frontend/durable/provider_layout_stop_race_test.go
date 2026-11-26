package durable

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
)

type stopRemoteProbe struct {
	outcome string
	err     error
	head    layout.Head
	readErr error
	aborted bool
}

func (r *stopRemoteProbe) AbortOpenSession(context.Context) (string, error) {
	r.aborted = true
	return r.outcome, r.err
}
func (r *stopRemoteProbe) ReadHead(context.Context) (layout.Head, error) {
	if !r.aborted {
		panic("read before session settlement")
	}
	return r.head, r.readErr
}

// Kernel-CAS races are exercised through the real remote by
// TestLayoutRemote_AbortOpenSessionDoesNotWaitForAHeldCommit. This probe
// checks that provider settlement consumes that single decision before resume.
func TestLayoutStopAbortMustConfirmItsOutcome(t *testing.T) {
	me := layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}
	for _, tc := range []struct {
		name, outcome string
		err           error
		state         string
		want          storage.DetachSettlement
		bad           bool
	}{
		{"aborted", "aborted", nil, layout.StateLive, storage.DetachNotPublished, false},
		{"idle", "idle", nil, layout.StateLive, storage.DetachNotPublished, false},
		{"late commit after abort reply", "aborted", nil, layout.StateDetached, storage.DetachApplied, false},
		{"late commit after idle reply", "idle", nil, layout.StateDetached, storage.DetachApplied, false},
		{"detached", "detached", nil, layout.StateDetached, storage.DetachApplied, false},
		{"fenced", "", storage.ErrChunkLayerFenced, "", storage.DetachFenced, false},
		{"absent", "", layout.ErrHeadAbsent, "", 0, true},
		{"unknown abort", "", errors.New("reply lost"), "", 0, true},
		{"unknown outcome", "unexpected", nil, "", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &stopRemoteProbe{outcome: tc.outcome, err: tc.err, head: layout.Head{Gen: 7, Epoch: 1, Holder: "s/r", ClaimID: "k", State: tc.state, Activity: layout.Activity{Kind: layout.ActIdle}, Position: 11}}
			got, result, err := settleLayoutStop(context.Background(), r, me)
			if !r.aborted || (err != nil) != tc.bad || (!tc.bad && got != tc.want) {
				t.Fatalf("got=%v err=%v aborted=%v", got, err, r.aborted)
			}
			if got == storage.DetachApplied && (result.H != 11 || result.Head.Gen != 7) {
				t.Fatalf("detached proof: %+v", result)
			}
		})
	}
	for _, head := range []layout.Head{{Epoch: 2, Holder: "s/r", ClaimID: "new", State: layout.StateDetached}, {Epoch: 1, Holder: "s/r", ClaimID: "k", State: layout.StateLive}} {
		r := &stopRemoteProbe{outcome: "detached", head: head}
		got, _, err := settleLayoutStop(context.Background(), r, me)
		if err == nil && got != storage.DetachFenced {
			t.Fatal("changed HEAD permitted resume", got, err)
		}
	}
}
