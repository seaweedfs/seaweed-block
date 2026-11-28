package layout

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// slowObjects adds a fixed latency to every Stat, as a remote filer lookup
// has. The latency runs outside the lock, so lookups may overlap.
type slowObjects struct {
	*memObjects
	mu    sync.Mutex
	delay time.Duration
	stats int
}

func (s *slowObjects) Stat(ctx context.Context, rel string) (ObjectMeta, bool, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ObjectMeta{}, false, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats++
	return s.memObjects.Stat(ctx, rel)
}

// firstCommitOf prepares a first commit of n new chunks of volume v1.
func firstCommitOf(t *testing.T, n int, session string) (*memObjects, *memHead, CommitRequest) {
	t.Helper()
	f := newMemObjects(volRoot)
	next := map[uint64]Ref{}
	for i := 0; i < n; i++ {
		next[uint64(i)] = ref("v1", uint64(i), fmt.Sprintf("c%d", i))
	}
	prepared(t, f, next, session)
	h := Head{Gen: 1, Epoch: 3, Holder: "srv-a", ClaimID: "k-a", VolumeID: idFor("v1"), Activity: Activity{Kind: ActSnap, Session: session}}
	return f, &memHead{h: &h}, CommitRequest{Session: session, Next: next, Position: 10, Snapshot: "daily"}
}

// A commit's check that every new chunk is this session's must not grow
// with the volume in serial filer round trips: a 1 TiB first snapshot names
// about 262k chunks, and one lookup each, one at a time, outlasted the
// cut's whole budget, on every retry. Here 6000 chunks at 1 ms a lookup
// take 6 s in series and must commit within a 2 s context.
func TestCommitVerifiesManyNewChunksWithinTheBudget(t *testing.T) {
	f, st, req := firstCommitOf(t, 6000, "s1")
	slow := &slowObjects{memObjects: f, delay: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	got, err := RunCommit(ctx, st, slow, "v1", me, req)
	if err != nil {
		t.Fatalf("commit of 6000 new chunks after %v: %v", time.Since(start).Round(time.Millisecond), err)
	}
	if got.Current == nil || got.Position != 10 {
		t.Fatalf("committed HEAD %+v", got)
	}
	if slow.stats < 6000 {
		t.Fatalf("only %d lookups for 6000 new chunks: every chunk must still be checked", slow.stats)
	}
}

// The check stays all-or-nothing: one chunk not written by this session,
// anywhere among many, refuses the commit and HEAD is not written.
func TestCommitRefusesWhenOneOfManyNewChunksIsNotPrepared(t *testing.T) {
	f, st, req := firstCommitOf(t, 3000, "s1")
	missing := req.Next[1777].Chunk.Path()
	delete(f.objs, f.full(missing))
	before := *st.h
	_, err := RunCommit(context.Background(), st, &slowObjects{memObjects: f}, "v1", me, req)
	if !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("commit with chunk 1777 absent: %v; want ErrNotPrepared", err)
	}
	if st.h.Gen != before.Gen || st.h.Current != nil {
		t.Fatalf("HEAD changed by a refused commit: %+v", *st.h)
	}
}

// A detach publishes through the same commit: its detaching commit of a
// large proposal must also land inside the cut's budget, or the volume can
// be neither detached nor sealed for delete. Same scale as above, with the
// commit carrying the detached state.
func TestADetachingCommitOfManyNewChunksCompletesWithinTheBudget(t *testing.T) {
	f, st, req := firstCommitOf(t, 6000, "s1")
	req.State, req.Snapshot = StateDetached, ""
	slow := &slowObjects{memObjects: f, delay: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := RunCommit(ctx, st, slow, "v1", me, req)
	if err != nil {
		t.Fatalf("detaching commit of 6000 new chunks: %v", err)
	}
	if got.State != StateDetached || got.Activity.Kind != ActIdle || got.Current == nil || got.Position != req.Position {
		t.Fatalf("HEAD after the detaching commit: %+v; want detached, idle, at position %d", got, req.Position)
	}
	if slow.stats < 6000 {
		t.Fatalf("only %d lookups for 6000 new chunks", slow.stats)
	}
}
