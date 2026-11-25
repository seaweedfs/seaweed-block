package storage

import (
	"context"
	"errors"
	"testing"
)

// revokingBudget admits every cut and keeps its abort, so the test can
// revoke the cut's host budget the way the limiter does at MaxHold.
type revokingBudget struct{ abort func(error) }

func (b *revokingBudget) Admit(_ context.Context, _ int64, abort func(error)) (uint64, error) {
	b.abort = abort
	return 1, nil
}
func (b *revokingBudget) ReserveCOW(uint64, int64) error { return nil }
func (b *revokingBudget) Release(uint64)                 {}

// revokeAfterRemote revokes the budget at the PUT after `after` completed
// ones. Every PUT attempt is counted before it reaches the fake, so a
// re-send of a completed key is recorded even if the caller then
// reconciles it.
type revokeAfterRemote struct {
	*fakeChunkRemote
	budget    *revokingBudget
	after     int // revoke once this many PUTs completed; < 0: never
	completed int
	bytes     int // bytes of every PUT attempt
	dups      int // attempts for a key that already completed
	keys      map[string]bool
}

func (r *revokeAfterRemote) Put(ctx context.Context, key string, b []byte) error {
	if r.after >= 0 && r.completed == r.after {
		r.budget.abort(errors.New("test: cut held its host slot past MaxHold"))
		<-ctx.Done()
		return context.Cause(ctx)
	}
	r.bytes += len(b)
	if r.keys[key] {
		r.dups++
	}
	if err := r.fakeChunkRemote.Put(ctx, key, b); err != nil {
		return err
	}
	r.keys[key] = true
	r.completed++
	return nil
}

// A cut whose host budget is revoked mid-upload (MaxHold) keeps the
// chunks it already uploaded: the next cut uploads only the rest, fewer
// bytes than the whole dirty set, and retransmits nothing.
func TestCutRevokedMidUploadKeepsItsUploads(t *testing.T) {
	const dirty = 5
	s, f, l, w := precopyLayer(t, 0, dirty)
	l.cfg.UploadConcurrency = 1
	budget := &revokingBudget{}
	r := &revokeAfterRemote{fakeChunkRemote: f, budget: budget, after: 2, keys: map[string]bool{}}
	l.remote = r
	l.cfg.Budget = budget
	base := f.stats().head.Gen

	if _, err := l.Cut(ctTimeout(t)); !errors.Is(err, errCutBudgetRevoked) {
		t.Fatalf("first cut: %v; want the budget revocation", err)
	}
	if f.stats().head.Gen != base {
		t.Fatal("a revoked cut moved HEAD")
	}
	first := r.bytes
	if first != 2*ctChunkBytes {
		t.Fatalf("first attempt uploaded %d bytes before the revocation; want %d", first, 2*ctChunkBytes)
	}

	r.after, r.bytes = -1, 0
	result, err := l.Cut(ctTimeout(t))
	if err != nil || result.Head.Gen != base+1 {
		t.Fatalf("retry: %+v %v", result, err)
	}
	if result.Stats.StageRounds != 0 {
		t.Fatalf("the retry staged (%d rounds); this control is for uploads made after the barrier", result.Stats.StageRounds)
	}
	if r.dups != 0 {
		t.Fatalf("%d PUTs re-sent an already completed key", r.dups)
	}
	if r.bytes >= dirty*ctChunkBytes || r.bytes != (dirty-2)*ctChunkBytes {
		t.Fatalf("retry uploaded %d bytes; want only the %d chunks not yet uploaded (%d bytes), less than the dirty set (%d)",
			r.bytes, dirty-2, (dirty-2)*ctChunkBytes, dirty*ctChunkBytes)
	}
	assertImage(t, "published image", f.imageOf(t, result.Head), w.imageAt(s.NextLSN()-1))
	assertNoSessionOpen(t, f)
}
