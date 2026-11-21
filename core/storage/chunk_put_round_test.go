package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// A final upload outcome is not retried: the object is sent once, its
// binding is checked, and the cut fails at once.
func TestChunkCut_FinalUploadIsNotRetried(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := &roundChunkRemote{fakeChunkRemote: newFakeChunkRemote()}
	f.fail = func(int, int, int) error { return errors.Join(ErrChunkPutFinal, errors.New("token rejected")) }
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	start := time.Now()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, ErrChunkPutFinal) {
		t.Fatalf("cut with a final upload refusal: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("cut took %v: the final refusal was retried", took)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, n := range f.tries {
		if n != 1 {
			t.Fatalf("object %v sent %d times; want once", k, n)
		}
	}
}

// heldRound blocks every round Put until released; cooperative Puts also
// end with their context. It records the bytes of every attempt.
type heldRound struct {
	*fakeChunkRemote
	cooperative bool
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	mu          sync.Mutex
	sent        [][]byte
	transient   int // the first transient attempts fail after being recorded
	mutate      func()
}

func (h *heldRound) BeginPutRound(n, objSize int) ChunkPutRound { return heldRoundPut{h} }

type heldRoundPut struct{ h *heldRound }

func (p heldRoundPut) Put(ctx context.Context, i int, key string, data []byte) error {
	h := p.h
	h.mu.Lock()
	h.sent = append(h.sent, append([]byte(nil), data...))
	attempt := len(h.sent)
	mutate := h.mutate
	h.mu.Unlock()
	h.once.Do(func() { close(h.entered) })
	if mutate != nil && attempt == 1 {
		mutate()
	}
	if attempt <= h.transient {
		return errors.New("transient upload failure")
	}
	if h.cooperative {
		select {
		case <-h.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		<-h.release
	}
	return h.fakeChunkRemote.Put(ctx, key, data)
}

// While a round's upload call is held, guest writes go on; Close cancels a
// cooperative call and joins it; an uncooperative call is still joined:
// Close does not return before it does.
func TestChunkCut_HeldRoundCallIsJoined(t *testing.T) {
	for _, cooperative := range []bool{true, false} {
		t.Run(fmt.Sprintf("cooperative=%v", cooperative), func(t *testing.T) {
			s, _ := newChunkTestStore(t)
			h := &heldRound{fakeChunkRemote: newFakeChunkRemote(), cooperative: cooperative, entered: make(chan struct{}), release: make(chan struct{})}
			layer := mustAttach(t, s, h, ctConfig(1))
			w := newWriteLog()
			w.write(t, s, 2, 0x22)
			cutDone := make(chan error, 1)
			go func() { _, err := layer.Cut(context.Background()); cutDone <- err }()
			select {
			case <-h.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the cut never reached its upload")
			}
			wrote := make(chan struct{})
			go func() { w.write(t, s, 7, 0x77); close(wrote) }()
			select {
			case <-wrote:
			case <-time.After(5 * time.Second):
				t.Fatal("a guest write waited on the held upload call")
			}
			closeDone := make(chan error, 1)
			go func() { closeDone <- layer.Close() }()
			if !cooperative {
				select {
				case <-closeDone:
					t.Fatal("Close returned while an uncooperative upload call was still running")
				case <-time.After(300 * time.Millisecond):
				}
				close(h.release)
			}
			select {
			case <-closeDone:
			case <-time.After(10 * time.Second):
				t.Fatal("Close did not return")
			}
			select {
			case <-cutDone:
			case <-time.After(5 * time.Second):
				t.Fatal("the cut did not end with Close")
			}
			if cooperative {
				close(h.release)
			}
		})
	}
}

// A retried upload sends the bytes captured for the cut, not the guest's
// newer write to the same block.
func TestChunkCut_RetrySendsTheCapturedBytes(t *testing.T) {
	s, _ := newChunkTestStore(t)
	h := &heldRound{fakeChunkRemote: newFakeChunkRemote(), cooperative: true, entered: make(chan struct{}), release: make(chan struct{}), transient: 1}
	close(h.release)
	w := newWriteLog()
	h.mutate = func() { w.write(t, s, 2, 0x99) } // the guest overwrites the block mid-upload
	layer := mustAttach(t, s, h, ctConfig(1))
	w.write(t, s, 2, 0x22)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.sent) != 2 {
		t.Fatalf("%d upload attempts; want the failed one and its retry", len(h.sent))
	}
	if !bytes.Equal(h.sent[0], h.sent[1]) {
		t.Fatal("the retry sent different bytes than the first attempt")
	}
	if bytes.Contains(h.sent[1], makeBlock(ctBlockSize, 0x99)) {
		t.Fatal("the retry sent the guest's newer write")
	}
}
