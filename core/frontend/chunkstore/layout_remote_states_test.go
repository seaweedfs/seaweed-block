package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The layout remote's in-memory state machine, closed as one table: every
// start state crossed with every call, with the error class, HEAD's
// activity after the call, and whether a pending name was committed.
//
// States (what the remote holds against what HEAD says):
//
//	idle      no session; HEAD idle (the engine has read HEAD)
//	open      session s held; HEAD snap(s)
//	abandoned session s held, the engine's AbortCut notice taken; HEAD snap(s)
//	pending   no session, a snapshot name set; HEAD idle
//	dead      no session, nothing read yet; HEAD snap(d) of this holder (a dead process's)
//	cleaning  this remote's cleaning tick running; HEAD clean(c)
//
// The other in-memory pieces (visible/ownGen/seen/forceView, cleaning,
// snapErr, lastCommit) are covered by their own tests; when the intent
// seam goes with the old store, PutIntent and the abandon notice remain
// (the engine still opens and gives up cuts), so these states remain.
func TestLayoutRemote_StateByCallTable(t *testing.T) {
	ctx := context.Background()
	type outcome struct {
		err     string // "", "precondition"
		act     string // HEAD activity kind after the call
		named   bool   // the pending name is in HEAD after the call
		settled string // AbortOpenSession's answer, when the call is it
	}
	calls := []string{"GetHead", "PutIntent", "Put", "CASHead", "AbortOpenSession", "CommitPendingSnapshot"}
	table := map[string]map[string]outcome{
		"idle": {
			"GetHead": {act: layout.ActIdle}, "PutIntent": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionIdle},
			"CommitPendingSnapshot": {act: layout.ActIdle},
		},
		"open": {
			"GetHead": {act: layout.ActSnap}, "PutIntent": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionAborted},
			"CommitPendingSnapshot": {act: layout.ActSnap},
		},
		"abandoned": {
			"GetHead": {act: layout.ActIdle}, "PutIntent": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionAborted},
			"CommitPendingSnapshot": {act: layout.ActSnap},
		},
		"pending": {
			"GetHead": {act: layout.ActIdle}, "PutIntent": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle, named: true}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionIdle},
			"CommitPendingSnapshot": {act: layout.ActIdle, named: true},
		},
		"cleaning": {
			"Clean":   {err: "busy", act: layout.ActClean},
			"Abort":   {act: layout.ActClean},
			"GetHead": {act: layout.ActClean}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionIdle},
			"CommitPendingSnapshot": {act: layout.ActClean},
		},
		"dead": {
			"GetHead": {act: layout.ActIdle}, "PutIntent": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {err: "precondition", act: layout.ActSnap}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionAborted},
			"CommitPendingSnapshot": {act: layout.ActSnap},
		},
	}
	setup := func(t *testing.T, state string) (*layoutHost, *LayoutRemote) {
		lh := newLayoutHost(t)
		r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
		if err != nil {
			t.Fatal(err)
		}
		if state != "dead" {
			if _, err := r.GetHead(ctx); err != nil {
				t.Fatal(err)
			}
		}
		switch state {
		case "open":
			if _, err := r.PutIntent(ctx, storage.ChunkIntent{}); err != nil {
				t.Fatal(err)
			}
		case "abandoned":
			if _, err := r.PutIntent(ctx, storage.ChunkIntent{}); err != nil {
				t.Fatal(err)
			}
			r.AbortCut()
		case "pending":
			r.SetSnapshot("p", layout.SnapshotMeta{CreatedNs: 1})
		case "cleaning":
			// a cleaning tick that runs until a session's call stops it
			// (or the test ends)
			release, started, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				_ = r.Clean(ctx, func(cctx context.Context, _ layout.HolderID, _ string) error {
					close(started)
					select {
					case <-cctx.Done():
					case <-release:
					}
					return nil
				})
			}()
			<-started
			t.Cleanup(func() { close(release); <-done })
		case "dead":
			st, _ := lh.cat.Store(lh.v)
			if _, err := layout.RunBegin(ctx, st, lh.me, "s-dead"); err != nil {
				t.Fatal(err)
			}
		}
		return lh, r
	}
	data := block(9)
	for state, row := range table {
		for _, call := range calls {
			want, ok := row[call]
			if !ok {
				t.Fatalf("table has no cell %s x %s", state, call)
			}
			t.Run(state+"/"+call, func(t *testing.T) {
				lh, r := setup(t, state)
				var err error
				var settled string
				switch call {
				case "GetHead":
					_, err = r.GetHead(ctx)
				case "PutIntent":
					_, err = r.PutIntent(ctx, storage.ChunkIntent{})
				case "Put":
					err = r.Put(ctx, r.ChunkKey(3, layout.Sum(data)), data)
				case "CASHead":
					err = func() error {
						_, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: r.visible, Epoch: lh.me.Epoch}, storage.ChunkHead{Epoch: lh.me.Epoch, Chunks: map[uint64]storage.ChunkRef{}})
						return err
					}()
				case "AbortOpenSession":
					settled, err = r.AbortOpenSession(ctx)
				case "CommitPendingSnapshot":
					err = r.CommitPendingSnapshot(ctx)
				}
				switch want.err {
				case "":
					if err != nil {
						t.Fatalf("%s x %s: %v", state, call, err)
					}
				case "precondition":
					if !errors.Is(err, storage.ErrChunkHeadPrecondition) {
						t.Fatalf("%s x %s: %v; want a precondition", state, call, err)
					}
				}
				h := lh.head()
				if h.Activity.Kind != want.act {
					t.Fatalf("%s x %s: HEAD %s after the call; want %s", state, call, h.Activity.Kind, want.act)
				}
				if _, named := h.Snapshots["p"]; named != want.named {
					t.Fatalf("%s x %s: pending name in HEAD %t; want %t", state, call, named, want.named)
				}
				if settled != want.settled {
					t.Fatalf("%s x %s: settled %q; want %q", state, call, settled, want.settled)
				}
			})
		}
	}
}
