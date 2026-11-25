package storage

import (
	"context"
	"testing"
)

// sessionRemote is a test remote that reports its open session.
type sessionRemote interface {
	chunkRemote
	open() string
}

type memSession struct{ *MemChunkRemote }

func (m memSession) open() string { return m.OpenSession() }

type fakeSession struct{ *fakeChunkRemote }

func (f fakeSession) open() string { return f.openSession() }

// checkSessionContract is the session contract every chunkRemote meets:
// Begin opens a session (a Begin while it is open replays it), Abort ends
// it and is idempotent, and a commit (a successful CASHead) ends it.
func checkSessionContract(t *testing.T, r sessionRemote) {
	t.Helper()
	ctx := context.Background()
	if s := r.open(); s != "" {
		t.Fatalf("a session is open before any Begin: %q", s)
	}
	a, err := r.Begin(ctx)
	if err != nil || a == "" {
		t.Fatalf("Begin: %q %v", a, err)
	}
	if again, err := r.Begin(ctx); err != nil || again != a {
		t.Fatalf("Begin while %q is open: %q %v; want its replay", a, again, err)
	}
	r.Abort()
	if s := r.open(); s != "" {
		t.Fatalf("after Abort: session %q still open", s)
	}
	r.Abort() // nothing open: a no-op
	b, err := r.Begin(ctx)
	if err != nil || b == "" || b == a {
		t.Fatalf("Begin after an Abort: %q %v; want a new session", b, err)
	}
	head, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := copyHead(head)
	next.Epoch, next.Holder = 1, "host-a"
	if _, err := r.CASHead(ctx, chunkHeadVersion{Gen: head.Gen, Epoch: head.Epoch}, next); err != nil {
		t.Fatal(err)
	}
	if s := r.open(); s != "" {
		t.Fatalf("after the commit: session %q still open", s)
	}
}

func TestMemChunkRemote_SessionContract(t *testing.T) {
	checkSessionContract(t, memSession{NewMemChunkRemote()})
}

func TestFakeChunkRemote_SessionContract(t *testing.T) {
	checkSessionContract(t, fakeSession{newFakeChunkRemote()})
}
