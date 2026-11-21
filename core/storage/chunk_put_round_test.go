package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// roundChunkRemote adds upload rounds to the fake remote and records them.
type roundChunkRemote struct {
	*fakeChunkRemote
	mu     sync.Mutex
	rounds []int    // n of each round begun
	puts   [][2]int // {round, i} of each round Put
	keys   []string // key of each round Put
	fail   func(round, i int, attempt int) error
	tries  map[[2]int]int
}

type recordedRound struct {
	r  *roundChunkRemote
	id int
}

func (r *roundChunkRemote) BeginPutRound(n, objSize int) ChunkPutRound {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rounds = append(r.rounds, n)
	return recordedRound{r: r, id: len(r.rounds) - 1}
}

func (rr recordedRound) Put(ctx context.Context, i int, key string, data []byte) error {
	r := rr.r
	r.mu.Lock()
	r.puts = append(r.puts, [2]int{rr.id, i})
	r.keys = append(r.keys, key)
	if r.tries == nil {
		r.tries = map[[2]int]int{}
	}
	r.tries[[2]int{rr.id, i}]++
	attempt := r.tries[[2]int{rr.id, i}]
	fail := r.fail
	r.mu.Unlock()
	if fail != nil {
		if err := fail(rr.id, i, attempt); err != nil {
			return err
		}
	}
	return r.fakeChunkRemote.Put(ctx, key, data)
}

// A cut uploads its changed chunks through one round of the remote, one
// object index per chunk (the round may be sized for more candidates than
// end up changed); a cut with nothing to upload begins none. A retried
// upload keeps its index.
func TestChunkCut_UploadsThroughOneRound(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := &roundChunkRemote{fakeChunkRemote: newFakeChunkRemote()}
	f.fail = func(round, i, attempt int) error {
		if i == 0 && attempt == 1 {
			return errors.New("transient upload failure")
		}
		return nil
	}
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	w.write(t, s, 9, 0x29)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	rounds, puts := append([]int(nil), f.rounds...), append([][2]int(nil), f.puts...)
	f.mu.Unlock()
	if len(rounds) != 1 || rounds[0] < 1 {
		t.Fatalf("rounds %v; want one round for the cut", rounds)
	}
	seen := map[int]int{}
	for _, p := range puts {
		if p[0] != 0 || p[1] < 0 || p[1] >= rounds[0] {
			t.Fatalf("round put %v outside round 0 of %d", p, rounds[0])
		}
		seen[p[1]]++
	}
	if seen[0] != 2 {
		t.Fatalf("object 0 sent %d times; want the failed attempt and its retry on the same index", seen[0])
	}
	if len(seen) != 2 {
		t.Fatalf("round of %d used indexes %v; want one per changed chunk (2)", rounds[0], seen)
	}

	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	n := len(f.rounds)
	f.mu.Unlock()
	if n != 1 {
		t.Fatalf("a cut with nothing to upload began a round (%d rounds)", n)
	}
}

// A round that cannot store an object fails the cut: no HEAD names it.
func TestChunkCut_FailedRoundPublishesNothing(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := &roundChunkRemote{fakeChunkRemote: newFakeChunkRemote()}
	f.fail = func(int, int, int) error { return ErrChunkKeyExists }
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := layer.Cut(ctx); err == nil {
		t.Fatal("cut with a refused upload succeeded")
	}
	if heads := f.installedHeads(); len(heads) != 0 {
		t.Fatalf("installed %d HEADs after a failed upload", len(heads))
	}
}
