package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// rangeFake is a blob store with batch reservations: each reserve hands out
// a new volume id; puts are recorded by fid. Hooks shape the replies.
type rangeFake struct {
	*fakeBlobs
	mu       sync.Mutex
	nextVid  int
	reserves []int // requested counts, in order
	grant    func(requested int) int
	auth     func() string
	refuse   func(fid string) error // non-nil: the put fails with it
	puts     []string
	putAuth  map[string]string
	inflight int
	peak     int
	hold     chan struct{} // non-nil: reserve waits for it (or ctx)
}

func newRangeFake() *rangeFake {
	return &rangeFake{fakeBlobs: newFakeBlobs(), nextVid: 1, putAuth: map[string]string{}}
}

func (b *rangeFake) reserve(ctx context.Context, n int, _ uint64) (blobRange, error) {
	b.mu.Lock()
	b.reserves = append(b.reserves, n)
	hold := b.hold
	b.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return blobRange{}, ctx.Err()
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	count := n
	if b.grant != nil {
		count = b.grant(n)
	}
	auth := ""
	if b.auth != nil {
		auth = b.auth()
	}
	vid := b.nextVid
	b.nextVid++
	return blobRange{base: fmt.Sprintf("%d,0a0b0c0d0e", vid), count: count, url: "fake", auth: auth}, nil
}

func (b *rangeFake) putRange(ctx context.Context, r blobRange, i int, data []byte) error {
	fid, err := r.fid(i)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.puts = append(b.puts, fid)
	b.putAuth[fid] = r.auth
	refuse := b.refuse
	b.mu.Unlock()
	if refuse != nil {
		if err := refuse(fid); err != nil {
			return err
		}
	}
	b.fakeBlobs.mu.Lock()
	b.fakeBlobs.data[fid] = append([]byte(nil), data...)
	b.fakeBlobs.mu.Unlock()
	return nil
}

func (b *rangeFake) reserveCounts() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int(nil), b.reserves...)
}

func roundRemote(t *testing.T, b *rangeFake) *ChunkRemote {
	t.Helper()
	r, err := NewChunkRemote(newFakeFiler(), b, "/swblock/vols/v1")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func roundKey(i int) string { return fmt.Sprintf("k%05d", i) }

// putAll runs one round of n objects with w concurrent workers.
func putAll(t *testing.T, r *ChunkRemote, n, w int) error {
	t.Helper()
	rd := r.BeginPutRound(n, 4096)
	var mu sync.Mutex
	next := 0
	var first error
	var wg sync.WaitGroup
	for j := 0; j < w; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				i := next
				next++
				mu.Unlock()
				if i >= n {
					return
				}
				if err := rd.Put(context.Background(), i, roundKey(i), []byte(roundKey(i))); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return first
}

func checkBound(t *testing.T, r *ChunkRemote, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		got, err := r.Get(context.Background(), roundKey(i))
		if err != nil || !bytes.Equal(got, []byte(roundKey(i))) {
			t.Fatalf("key %d: %q %v", i, got, err)
		}
	}
}

// D objects take G = min(8, ceil(D/64)) reservations, balanced across the
// groups; D = 0 makes none. Every key is bound to its own fid.
func TestChunkRound_GroupsPerRound(t *testing.T) {
	for _, c := range []struct{ d, g int }{{0, 0}, {1, 1}, {64, 1}, {65, 2}, {512, 8}, {513, 8}, {1000, 8}} {
		t.Run(fmt.Sprint(c.d), func(t *testing.T) {
			b := newRangeFake()
			r := roundRemote(t, b)
			if err := putAll(t, r, c.d, 16); err != nil {
				t.Fatal(err)
			}
			got := b.reserveCounts()
			if len(got) != c.g {
				t.Fatalf("%d reservations %v for %d objects, want %d", len(got), got, c.d, c.g)
			}
			total, lo, hi := 0, c.d, 0
			for _, n := range got {
				total += n
				lo, hi = min(lo, n), max(hi, n)
			}
			if total != c.d || (c.g > 0 && hi-lo > 1) {
				t.Fatalf("reservations %v: want %d ids balanced within one", got, c.d)
			}
			checkBound(t, r, c.d)
			seen := map[string]bool{}
			for i := 0; i < c.d; i++ {
				fid := r.fids[roundKey(i)]
				if seen[fid] {
					t.Fatalf("fid %s bound twice", fid)
				}
				seen[fid] = true
			}
			if a := r.AssignCounts(); a.Initial != int64(c.g) || a.Extra() != 0 {
				t.Fatalf("assign counts %+v, want %d initial, no extras", a, c.g)
			}
		})
	}
}

// A short grant is topped up by a reservation for the remainder, counted as
// an extra, not an initial assign.
func TestChunkRound_ShortGrantReservesRemainder(t *testing.T) {
	b := newRangeFake()
	first := true
	b.grant = func(n int) int {
		if first {
			first = false
			return n / 2
		}
		return n
	}
	r := roundRemote(t, b)
	if err := putAll(t, r, 10, 4); err != nil {
		t.Fatal(err)
	}
	if got := b.reserveCounts(); len(got) != 2 || got[0] != 10 || got[1] != 5 {
		t.Fatalf("reservations %v, want [10 5]", got)
	}
	checkBound(t, r, 10)
	if a := r.AssignCounts(); a.Initial != 1 || a.Short != 1 || a.Extra() != 1 {
		t.Fatalf("assign counts %+v", a)
	}
}

// A definite capacity refusal leaves objects already stored on the old range
// alone and moves the rest of the group to a fresh range; a volume that keeps
// refusing ends the round after a bounded number of replacements.
func TestChunkRound_CapacityRefusalReplacesRemainder(t *testing.T) {
	b := newRangeFake()
	b.refuse = func(fid string) error {
		// The first volume is full from its third id on.
		if fid == "1,0a0b0c0d0e_2" || fid == "1,0a0b0c0d0e_3" {
			return errRangeRefused
		}
		return nil
	}
	r := roundRemote(t, b)
	if err := putAll(t, r, 6, 1); err != nil {
		t.Fatal(err)
	}
	checkBound(t, r, 6)
	if r.fids[roundKey(0)] != "1,0a0b0c0d0e" || r.fids[roundKey(1)] != "1,0a0b0c0d0e_1" {
		t.Fatalf("committed prefix moved: %s %s", r.fids[roundKey(0)], r.fids[roundKey(1)])
	}
	if got := b.reserveCounts(); len(got) != 2 || got[1] != 4 {
		t.Fatalf("reservations %v, want a fresh range for the remaining 4", got)
	}
	if a := r.AssignCounts(); a.Initial != 1 || a.Capacity != 1 {
		t.Fatalf("assign counts %+v", a)
	}

	b2 := newRangeFake()
	b2.refuse = func(string) error { return errRangeRefused }
	r2 := roundRemote(t, b2)
	err := putAll(t, r2, 3, 1)
	if !errors.Is(err, errRangeRefused) {
		t.Fatalf("always-full volumes: %v; want the refusal after bounded replacements", err)
	}
	if got := b2.reserveCounts(); len(got) != 1+roundMaxReplacements {
		t.Fatalf("%d reservations %v; want 1 + %d replacements, then stop", len(got), got, roundMaxReplacements)
	}
}

// An expired token is never sent: the remainder gets a fresh range. A 401 is
// terminal: no replacement.
func TestChunkRound_TokenExpiryAndAuth(t *testing.T) {
	b := newRangeFake()
	calls := 0
	b.auth = func() string {
		calls++
		if calls == 1 {
			return jwtWithExp(time.Now().Add(-time.Minute).Unix())
		}
		return jwtWithExp(time.Now().Add(time.Hour).Unix())
	}
	r := roundRemote(t, b)
	if err := putAll(t, r, 3, 1); err != nil {
		t.Fatal(err)
	}
	checkBound(t, r, 3)
	b.mu.Lock()
	for fid, a := range b.putAuth {
		if exp, ok := (blobRange{auth: a}).sendDeadline(); !ok || exp.Before(time.Now()) {
			t.Fatalf("upload to %s sent with an expired token", fid)
		}
	}
	b.mu.Unlock()
	if a := r.AssignCounts(); a.Initial != 1 || a.Expired != 1 {
		t.Fatalf("assign counts %+v", a)
	}

	b2 := newRangeFake()
	b2.refuse = func(string) error { return errRangeAuth }
	r2 := roundRemote(t, b2)
	if err := putAll(t, r2, 2, 1); !errors.Is(err, errRangeAuth) {
		t.Fatalf("401: %v", err)
	}
	if got := b2.reserveCounts(); len(got) != 1 {
		t.Fatalf("reservations %v after a 401; want no replacement", got)
	}
}

// A transient failure leaves the slot frozen: the retry sends the same fid.
func TestChunkRound_RetryKeepsTheSlot(t *testing.T) {
	b := newRangeFake()
	failed := false
	b.refuse = func(fid string) error {
		if fid == "1,0a0b0c0d0e_1" && !failed {
			failed = true
			return errors.New("connection reset")
		}
		return nil
	}
	r := roundRemote(t, b)
	rd := r.BeginPutRound(2, 4096)
	ctx := context.Background()
	if err := rd.Put(ctx, 0, roundKey(0), []byte(roundKey(0))); err != nil {
		t.Fatal(err)
	}
	if err := rd.Put(ctx, 1, roundKey(1), []byte(roundKey(1))); err == nil {
		t.Fatal("transient failure reported success")
	}
	if err := rd.Put(ctx, 1, roundKey(1), []byte(roundKey(1))); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	puts := append([]string(nil), b.puts...)
	b.mu.Unlock()
	if len(puts) != 3 || puts[1] != "1,0a0b0c0d0e_1" || puts[2] != "1,0a0b0c0d0e_1" {
		t.Fatalf("uploads %v; want the retry on the same fid", puts)
	}
	if got := b.reserveCounts(); len(got) != 1 {
		t.Fatalf("reservations %v; a transient failure takes no new range", got)
	}
	// A bound key is never rebound.
	if err := rd.Put(ctx, 1, roundKey(1), []byte("other")); !errors.Is(err, storage.ErrChunkKeyExists) {
		t.Fatalf("rebinding a key: %v", err)
	}
}

// A reservation waiting on the filer ends with the caller's context, and a
// reply that arrives after it never starts an upload.
func TestChunkRound_ReserveFollowsTheCaller(t *testing.T) {
	b := newRangeFake()
	b.hold = make(chan struct{})
	r := roundRemote(t, b)
	rd := r.BeginPutRound(1, 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := rd.Put(ctx, 0, roundKey(0), []byte("x")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("held reservation: %v", err)
	}
	close(b.hold)
	b.mu.Lock()
	n := len(b.puts)
	b.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d uploads after the caller gave up", n)
	}
}
