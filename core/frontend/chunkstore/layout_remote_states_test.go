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
//	abandoned session s held, the engine's Abort notice taken; HEAD snap(s)
//	pending   no session, a snapshot name set; HEAD idle
//	dead      no session, nothing read yet; HEAD snap(d) of this holder (a dead process's)
//
// The other in-memory pieces (visible/ownGen/seen/forceView, cleaning,
// snapErr, lastCommit) are covered by their own tests. The seam's calls
// are the kernel's: Begin opens the cut's session, Abort is the engine's
// notice that it gave a cut up.
func TestLayoutRemote_StateByCallTable(t *testing.T) {
	ctx := context.Background()
	type outcome struct {
		err     string // "", "precondition"
		act     string // HEAD activity kind after the call
		named   bool   // the pending name is in HEAD after the call
		settled string // AbortOpenSession's answer, when the call is it
	}
	// Abort is the engine's non-blocking notice: it writes nothing, so HEAD
	// is what the state left; the next HEAD read ends the session.
	calls := []string{"GetHead", "Begin", "Put", "CASHead", "Abort", "AbortOpenSession", "CommitPendingSnapshot"}
	table := map[string]map[string]outcome{
		"idle": {
			"Abort":   {act: layout.ActIdle},
			"GetHead": {act: layout.ActIdle}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionIdle},
			"CommitPendingSnapshot": {act: layout.ActIdle},
		},
		"open": {
			"Abort":   {act: layout.ActSnap},
			"GetHead": {act: layout.ActSnap}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionAborted},
			"CommitPendingSnapshot": {act: layout.ActSnap},
		},
		"abandoned": {
			"Abort":   {act: layout.ActSnap},
			"GetHead": {act: layout.ActIdle}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionAborted},
			"CommitPendingSnapshot": {act: layout.ActSnap},
		},
		"pending": {
			"Abort":   {act: layout.ActIdle},
			"GetHead": {act: layout.ActIdle}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle, named: true}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionIdle},
			"CommitPendingSnapshot": {act: layout.ActIdle, named: true},
		},
		"dead": {
			"Abort":   {act: layout.ActSnap},
			"GetHead": {act: layout.ActIdle}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
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
			if _, err := r.Begin(ctx); err != nil {
				t.Fatal(err)
			}
		case "abandoned":
			if _, err := r.Begin(ctx); err != nil {
				t.Fatal(err)
			}
			r.Abort()
		case "pending":
			r.SetSnapshot("p", layout.SnapshotMeta{CreatedNs: 1})
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
				case "Begin":
					_, err = r.Begin(ctx)
				case "Put":
					err = r.Put(ctx, r.ChunkKey(3, layout.Sum(data)), data)
				case "CASHead":
					err = func() error {
						_, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: r.visible, Epoch: lh.me.Epoch}, storage.ChunkHead{Epoch: lh.me.Epoch, Chunks: map[uint64]storage.ChunkRef{}})
						return err
					}()
				case "Abort":
					r.Abort()
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
