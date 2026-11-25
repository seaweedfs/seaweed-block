package chunkstore

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

func headActivity(t *testing.T, lh *layoutHost) layout.Activity {
	t.Helper()
	h, _, err := lh.cat.ReadHead(context.Background(), lh.v)
	if err != nil {
		t.Fatal(err)
	}
	return h.Activity
}

// The engine's Abort never blocks and writes nothing; the session it gave
// up is ended at the remote's next HEAD read (the next cut's, or a
// scheduler tick's), so HEAD is never left in snap(s) for long.
func TestLayoutRemote_AbortEndsTheSessionAtTheNextHeadRead(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act := headActivity(t, lh); act.Kind != layout.ActSnap || act.Session != s {
		t.Fatalf("after Begin: HEAD %+v, want snap(%s)", act, s)
	}
	r.Abort()
	if act := headActivity(t, lh); act.Kind != layout.ActSnap || act.Session != s {
		t.Fatalf("Abort wrote HEAD (%+v): it must only take the notice", act)
	}
	if _, err := r.GetHead(ctx); err != nil {
		t.Fatal(err)
	}
	if act := headActivity(t, lh); act.Kind != layout.ActIdle {
		t.Fatalf("after the next HEAD read: HEAD %+v, want the given-up session ended", act)
	}
}

// Abort, and the HEAD read that follows it, never end a session that is
// not this remote's own (epoch, holder and claim id): a newer holder's
// open session survives a deposed remote's Abort.
func TestLayoutRemote_AbortNeverEndsAnotherHoldersSession(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	old, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := lh.cat.Store(lh.v)
	newer := layout.HolderID{Epoch: 2, Holder: "b", ClaimID: "k-b"}
	if _, err := layout.Start(ctx, st, okClaimLease{}, layout.HolderID{Epoch: 2, Holder: "b"}, layout.LocalRoot{Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 8 * 4096}, ClaimID: "k-b", Fresh: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := layout.RunBegin(ctx, st, newer, "s-b"); err != nil {
		t.Fatal(err)
	}
	old.Abort()
	_, _ = old.GetHead(ctx) // the deposed remote's next read
	if act := headActivity(t, lh); act.Kind != layout.ActSnap || act.Session != "s-b" {
		t.Fatalf("HEAD after the deposed remote's Abort and read: %+v, want the newer holder's snap(s-b)", act)
	}
}

// The bound on a given-up session with no cut after it: the next cleaning
// tick ends it before it cleans (no Begin, no cut in between), so HEAD
// leaves snap(s) at the next cut, cleaning tick or stop, whichever comes
// first.
func TestLayoutRemote_AbortedSessionEndsAtTheNextCleaningTick(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r.Abort()
	if act := headActivity(t, lh); act.Kind != layout.ActSnap || act.Session != s {
		t.Fatalf("after Abort: HEAD %+v, want the given-up snap(%s) still there", act, s)
	}
	ran := false
	if err := r.Clean(ctx, func(context.Context, layout.HolderID, string) error {
		ran = true
		if act := headActivity(t, lh); act.Kind != layout.ActClean {
			t.Fatalf("while cleaning: HEAD %+v, want the session ended and clean open", act)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if act := headActivity(t, lh); !ran || act.Kind != layout.ActIdle {
		t.Fatalf("after the cleaning tick: ran=%t HEAD %+v, want idle", ran, act)
	}
}
