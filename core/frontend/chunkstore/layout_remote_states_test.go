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
//	births-pending commit reply lost; HEAD idle, adapter still holds the old session and birth refs
//	dead      no session, nothing read yet; HEAD snap(d) of this holder (a dead process's)
//	cleaning  this remote's cleaning tick running; HEAD clean(c)
//
// The other in-memory pieces (visible/ownGen/seen/forceView, cleaning,
// snapErr, lastCommit) are covered by their own tests. The seam's calls
// are the kernel's: Begin opens the cut's session, Abort is the engine's
// notice that it gave a cut up.
func TestLayoutRemote_StateByCallTable(t *testing.T) {
	ctx := context.Background()
	type outcome struct {
		err     string // "", "precondition", "busy"
		act     string // HEAD activity kind after the call
		named   bool   // the pending name is in HEAD after the call
		settled string // AbortOpenSession's answer, when the call is it
	}
	// Abort is the engine's non-blocking notice: it writes nothing, so HEAD
	// is what the state left; the next HEAD read ends the session.
	// Clean is the cleaning (maintenance) tick: it ends a given-up session
	// (abandoned, dead) before it cleans, and is refused while one is held.
	calls := []string{"GetHead", "Begin", "Put", "CASHead", "Abort", "Clean", "AbortOpenSession", "CommitPendingSnapshot"}
	table := map[string]map[string]outcome{
		"idle": {
			"Clean":   {act: layout.ActIdle},
			"Abort":   {act: layout.ActIdle},
			"GetHead": {act: layout.ActIdle}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionIdle},
			"CommitPendingSnapshot": {act: layout.ActIdle},
		},
		"open": {
			"Clean":   {err: "busy", act: layout.ActSnap},
			"Abort":   {act: layout.ActSnap},
			"GetHead": {act: layout.ActSnap}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionAborted},
			"CommitPendingSnapshot": {act: layout.ActSnap},
		},
		"abandoned": {
			"Clean":   {act: layout.ActIdle},
			"Abort":   {act: layout.ActSnap},
			"GetHead": {act: layout.ActIdle}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionAborted},
			"CommitPendingSnapshot": {act: layout.ActSnap},
		},
		"pending": {
			"Clean":   {act: layout.ActIdle},
			"Abort":   {act: layout.ActIdle},
			"GetHead": {act: layout.ActIdle}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle, named: true}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionIdle},
			"CommitPendingSnapshot": {act: layout.ActIdle, named: true},
		},
		"births-pending": {
			"Clean":   {err: "busy", act: layout.ActIdle},
			"Abort":   {act: layout.ActIdle},
			"GetHead": {act: layout.ActIdle}, "Begin": {act: layout.ActIdle}, "Put": {act: layout.ActIdle},
			"CASHead": {err: "precondition", act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionIdle},
			"CommitPendingSnapshot": {act: layout.ActIdle},
		},
		"cleaning": {
			"Clean":   {err: "busy", act: layout.ActClean},
			"Abort":   {act: layout.ActClean},
			"GetHead": {act: layout.ActClean}, "Begin": {act: layout.ActSnap}, "Put": {act: layout.ActSnap},
			"CASHead": {act: layout.ActIdle}, "AbortOpenSession": {act: layout.ActIdle, settled: SessionIdle},
			"CommitPendingSnapshot": {act: layout.ActClean},
		},
		"dead": {
			"Clean":   {act: layout.ActIdle},
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
		case "births-pending":
			if _, err := r.Begin(ctx); err != nil {
				t.Fatal(err)
			}
			data := block(9)
			key := r.ChunkKey(0, layout.Sum(data))
			if err := r.Put(ctx, key, data); err != nil {
				t.Fatal(err)
			}
			lost := &lostReplyStore{sessionStore: r.st, armed: true}
			r.st = lost
			_, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: r.visible, Epoch: lh.me.Epoch}, storage.ChunkHead{
				Epoch: lh.me.Epoch, RemoteLSN: 1, Chunks: map[uint64]storage.ChunkRef{
					0: {Key: key, SHA256: layout.Sum(data), Encoding: "full", BirthLSN: 1},
				},
			})
			if err == nil || lost.armed || len(r.pendingRefs) != 1 {
				t.Fatalf("lost reply did not leave births pending: err=%v armed=%t refs=%d", err, lost.armed, len(r.pendingRefs))
			}
			r.st = lost.sessionStore
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
				case "Clean":
					err = r.Clean(ctx, func(context.Context, layout.HolderID, string) error { return nil })
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
				case "busy":
					if !errors.Is(err, layout.ErrBusy) {
						t.Fatalf("%s x %s: %v; want busy", state, call, err)
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
