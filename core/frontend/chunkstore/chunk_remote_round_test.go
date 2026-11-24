package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
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
	refuse   func(fid string) error // non-nil: the put fails with it (nothing stored)
	// reserveHook, when set, runs before each reserve (call counts from 1);
	// an error fails the reserve. It decides itself whether to follow ctx.
	reserveHook func(ctx context.Context, call int) error
	// putHook, when set, decides each put: store the bytes or not, and the
	// error to return (a stored put with an error is a lost reply).
	putHook func(ctx context.Context, fid string) (store bool, err error)
	puts    []string
	putAuth map[string]string
	putDL   map[string]time.Time // deadline of the context each put was sent under
}

func newRangeFake() *rangeFake {
	return &rangeFake{fakeBlobs: newFakeBlobs(), nextVid: 1, putAuth: map[string]string{}, putDL: map[string]time.Time{}}
}

func (b *rangeFake) reserve(ctx context.Context, n int, _ uint64) (blobRange, error) {
	b.mu.Lock()
	b.reserves = append(b.reserves, n)
	call := len(b.reserves)
	hook := b.reserveHook
	b.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, call); err != nil {
			return blobRange{}, err
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
	return blobRange{base: fmt.Sprintf("%d,0a0b0c0d0e", vid), count: count, url: "fake:1", auth: auth}, nil
}

func (b *rangeFake) putRange(ctx context.Context, r blobRange, i int, data []byte) error {
	fid, err := r.fid(i)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.puts = append(b.puts, fid)
	b.putAuth[fid] = r.auth
	if dl, ok := ctx.Deadline(); ok {
		b.putDL[fid] = dl
	}
	refuse, hook := b.refuse, b.putHook
	b.mu.Unlock()
	store, perr := true, error(nil)
	if refuse != nil {
		if err := refuse(fid); err != nil {
			store, perr = false, err
		}
	}
	if hook != nil {
		store, perr = hook(ctx, fid)
	}
	if store {
		c, _, err := canonicalFid(fid)
		if err != nil {
			return err
		}
		b.fakeBlobs.mu.Lock()
		b.fakeBlobs.data[c] = append([]byte(nil), data...)
		b.fakeBlobs.mu.Unlock()
	}
	return perr
}

func (b *rangeFake) probe(_ context.Context, r blobRange, i int) ([]byte, error) {
	fid, err := r.fid(i)
	if err != nil {
		return nil, err
	}
	c, _, err := canonicalFid(fid)
	if err != nil {
		return nil, err
	}
	b.fakeBlobs.mu.Lock()
	defer b.fakeBlobs.mu.Unlock()
	d, ok := b.fakeBlobs.data[c]
	if !ok {
		return nil, fmt.Errorf("%w %s", errBlobAbsent, fid)
	}
	return append([]byte(nil), d...), nil
}

func (b *rangeFake) putList() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.puts...)
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
	if r.fids[roundKey(0)] != canon(t, "1,0a0b0c0d0e") || r.fids[roundKey(1)] != canon(t, "1,0a0b0c0d0e_1") {
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

// waitFor waits for ch, failing t after a bound.
func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// releaser closes a channel once; registered as cleanup before any worker
// that waits on it starts, so a failing test never strands that worker.
func releaser(t *testing.T) (ch chan struct{}, release func()) {
	ch = make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return ch, release
}

// A reservation waiting on the filer ends with the caller's context when it
// follows it; one that ignores it is still joined (Put does not return
// before it), and its late successful reply never starts an upload.
func TestChunkRound_ReserveFollowsTheCaller(t *testing.T) {
	t.Run("cooperative", func(t *testing.T) {
		b := newRangeFake()
		hold, _ := releaser(t)
		entered := make(chan struct{})
		b.reserveHook = func(ctx context.Context, _ int) error {
			close(entered)
			select {
			case <-hold:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		r := roundRemote(t, b)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		joined := false
		t.Cleanup(func() { // ends and joins the worker whatever happens below
			cancel()
			if joined {
				return
			}
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("Put never returned")
			}
		})
		go func() { done <- r.BeginPutRound(1, 4096).Put(ctx, 0, roundKey(0), []byte("x")) }()
		waitFor(t, entered, "the reservation call")
		cancel()
		select {
		case err := <-done:
			joined = true
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled reservation: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Put did not end with its context")
		}
	})
	t.Run("uncooperative", func(t *testing.T) {
		b := newRangeFake()
		hold, release := releaser(t)
		entered := make(chan struct{})
		b.reserveHook = func(context.Context, int) error { close(entered); <-hold; return nil } // ignores ctx
		r := roundRemote(t, b)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- r.BeginPutRound(1, 4096).Put(ctx, 0, roundKey(0), []byte("x")) }()
		t.Cleanup(func() { // joins the worker whatever happens below
			release()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("Put never returned")
			}
		})
		waitFor(t, entered, "the reservation call")
		cancel()
		select {
		case err := <-done:
			done <- err
			t.Fatalf("Put returned (%v) while its admitted reservation call was still running", err)
		case <-time.After(200 * time.Millisecond):
		}
		release() // the late reply is a success
		select {
		case err := <-done:
			done <- err
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("late reply after the caller gave up: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Put did not return after its reservation call did")
		}
		if puts := b.putList(); len(puts) != 0 {
			t.Fatalf("uploads %v after the caller gave up", puts)
		}
	})
}

// A replacement reply that arrives after a lower slot retired the group's
// ranges is discarded: the group is reserved again from its first uncovered
// slot, so no slot is left uncovered (and no zero-id reservation is asked).
func TestChunkRound_StaleReplacementIsDiscarded(t *testing.T) {
	b := newRangeFake()
	r := roundRemote(t, b)
	rd := r.BeginPutRound(4, 4096).(*putRound)
	g := rd.groups[0]
	second := make(chan struct{})
	var secondOnce sync.Once
	release, releaseAll := releaser(t)
	b.reserveHook = func(ctx context.Context, call int) error {
		if call == 2 {
			secondOnce.Do(func() { close(second) })
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	slot0Sent := make(chan struct{})
	b.putHook = func(ctx context.Context, fid string) (bool, error) {
		switch fid {
		case "1,0a0b0c0d0e_1": // slot 1: refused at once
			return false, errRangeRefused
		case "1,0a0b0c0d0e": // slot 0: sent, refused once the replacement is in flight
			close(slot0Sent)
			select {
			case <-second:
			case <-ctx.Done():
				return false, ctx.Err()
			}
			return false, errRangeRefused
		}
		return true, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	errs := make(chan error, 2)
	started := 0
	t.Cleanup(func() { // release and join every worker started
		releaseAll()
		cancel()
		for ; started > 0; started-- {
			select {
			case <-errs:
			case <-time.After(10 * time.Second):
				t.Error("a round worker never returned")
				return
			}
		}
	})
	started++
	go func() { errs <- rd.Put(ctx, 0, roundKey(0), []byte(roundKey(0))) }()
	waitFor(t, slot0Sent, "slot 0's upload")
	started++
	go func() { errs <- rd.Put(ctx, 1, roundKey(1), []byte(roundKey(1))) }()
	waitFor(t, second, "the replacement reservation")
	deadline := time.Now().Add(10 * time.Second)
	for { // slot 0's refusal retires the rest of the group meanwhile
		g.mu.Lock()
		v := g.ver
		g.mu.Unlock()
		if v == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("group version %d; slot 0's refusal never retired the range", v)
		}
		time.Sleep(time.Millisecond)
	}
	releaseAll()
	for started > 0 {
		select {
		case err := <-errs:
			started-- // joined, before any Fatal: cleanup must not wait for it again
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a round worker did not finish")
		}
	}
	got := b.reserveCounts()
	for _, n := range got {
		if n <= 0 {
			t.Fatalf("reservations %v: asked for %d ids", got, n)
		}
	}
	if len(got) != 3 || got[0] != 4 || got[1] != 3 || got[2] != 4 {
		t.Fatalf("reservations %v; want [4 3 4] (the stale [1,4) reply discarded)", got)
	}
	for i := 0; i < 2; i++ {
		if fid := r.fids[roundKey(i)]; !strings.HasPrefix(fid, "3,") {
			t.Fatalf("slot %d bound to %s; want the fresh range", i, fid)
		}
	}
	if a := r.AssignCounts(); a.Initial != 1 || a.Capacity != 1 || a.Retry != 1 || a.Failed != 1 {
		t.Fatalf("assign counts %+v", a)
	}
}

// A lost reservation reply is counted: the retry is an extra attempt.
func TestChunkRound_LostReservationReplyIsCounted(t *testing.T) {
	b := newRangeFake()
	b.reserveHook = func(_ context.Context, call int) error {
		if call == 1 {
			return errors.New("lost assign reply")
		}
		return nil
	}
	r := roundRemote(t, b)
	rd := r.BeginPutRound(1, 4096)
	ctx := context.Background()
	if err := rd.Put(ctx, 0, roundKey(0), []byte("x")); err == nil {
		t.Fatal("lost reply reported success")
	}
	if err := rd.Put(ctx, 0, roundKey(0), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if a := r.AssignCounts(); a.Initial != 1 || a.Retry != 1 || a.Failed != 1 || a.Extra() != 1 {
		t.Fatalf("assign counts %+v; want 1 initial + 1 retry, 1 failed", a)
	}
}

// An upload whose reply was lost keeps its fid: a retry after the group's
// lower slot retired the range, or after the token expired, does not move
// it. Past expiry the fid is read back: the exact bytes are bound there,
// definitely absent bytes go to a fresh range.
func TestChunkRound_AmbiguousUploadKeepsItsFid(t *testing.T) {
	ctx := context.Background()
	t.Run("lower slot retires the range", func(t *testing.T) {
		b := newRangeFake()
		lost := true
		b.putHook = func(_ context.Context, fid string) (bool, error) {
			switch {
			case fid == "1,0a0b0c0d0e_1" && lost:
				lost = false
				return false, errors.New("connection reset") // outcome unknown to the client
			case fid == "1,0a0b0c0d0e":
				return false, errRangeRefused
			}
			return true, nil
		}
		r := roundRemote(t, b)
		rd := r.BeginPutRound(2, 4096)
		if err := rd.Put(ctx, 1, roundKey(1), []byte(roundKey(1))); err == nil {
			t.Fatal("lost reply reported success")
		}
		if err := rd.Put(ctx, 0, roundKey(0), []byte(roundKey(0))); err != nil {
			t.Fatal(err)
		}
		if err := rd.Put(ctx, 1, roundKey(1), []byte(roundKey(1))); err != nil {
			t.Fatal(err)
		}
		if fid := r.fids[roundKey(1)]; fid != canon(t, "1,0a0b0c0d0e_1") {
			t.Fatalf("retried slot bound to %s; want its first fid", fid)
		}
		if fid := r.fids[roundKey(0)]; !strings.HasPrefix(fid, "2,") {
			t.Fatalf("refused slot bound to %s; want the fresh range", fid)
		}
	})
	for _, stored := range []bool{true, false} {
		t.Run(fmt.Sprintf("token expires, stored=%v", stored), func(t *testing.T) {
			b := newRangeFake()
			calls := 0
			b.auth = func() string {
				calls++
				if calls == 1 { // expires (less skew) about a second from now
					return jwtWithExp(time.Now().Add(jwtSendSkew + time.Second).Unix())
				}
				return jwtWithExp(time.Now().Add(time.Hour).Unix())
			}
			lost := true
			b.putHook = func(context.Context, string) (bool, error) {
				if lost {
					lost = false
					return stored, errors.New("reply lost")
				}
				return true, nil
			}
			r := roundRemote(t, b)
			rd := r.BeginPutRound(1, 4096)
			if err := rd.Put(ctx, 0, roundKey(0), []byte(roundKey(0))); err == nil {
				t.Fatal("lost reply reported success")
			}
			b.mu.Lock()
			first := b.putAuth["1,0a0b0c0d0e"]
			b.mu.Unlock()
			dl, _ := (blobRange{auth: first}).sendDeadline()
			time.Sleep(time.Until(dl) + 10*time.Millisecond)
			if err := rd.Put(ctx, 0, roundKey(0), []byte(roundKey(0))); err != nil {
				t.Fatal(err)
			}
			fid, puts := r.fids[roundKey(0)], b.putList()
			if stored {
				if fid != canon(t, "1,0a0b0c0d0e") || len(puts) != 1 {
					t.Fatalf("stored upload: bound %s after uploads %v; want its first fid, no resend", fid, puts)
				}
				return
			}
			if !strings.HasPrefix(fid, "2,") || len(puts) != 2 {
				t.Fatalf("absent upload: bound %s after uploads %v; want a fresh range", fid, puts)
			}
			if a := r.AssignCounts(); a.Initial != 1 || a.Expired != 1 {
				t.Fatalf("assign counts %+v", a)
			}
		})
	}
}

// An upload is sent under its token's send deadline: a queued upload ends
// there, not at the caller's later deadline.
func TestChunkRound_UploadCarriesTheTokenDeadline(t *testing.T) {
	b := newRangeFake()
	b.auth = func() string { return jwtWithExp(time.Now().Add(jwtSendSkew + time.Second).Unix()) }
	b.putHook = func(ctx context.Context, _ string) (bool, error) {
		<-ctx.Done() // queued until its context ends
		return false, ctx.Err()
	}
	r := roundRemote(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	err := r.BeginPutRound(1, 4096).Put(ctx, 0, roundKey(0), []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 10*time.Second {
		t.Fatalf("queued upload: %v after %v; want it ended at the token's deadline", err, time.Since(start))
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for f, a := range b.putAuth {
		dl, _ := (blobRange{auth: a}).sendDeadline()
		if !b.putDL[f].Equal(dl) {
			t.Fatalf("upload to %s sent under deadline %v; want the token's %v", f, b.putDL[f], dl)
		}
	}
}

// A rejected token and used-up replacements are final for the caller; a
// transient failure is not.
func TestChunkRound_FinalOutcomes(t *testing.T) {
	put := func(b *rangeFake) error {
		return roundRemote(t, b).BeginPutRound(1, 4096).Put(context.Background(), 0, roundKey(0), []byte("x"))
	}
	b := newRangeFake()
	b.refuse = func(string) error { return errRangeAuth }
	if err := put(b); !errors.Is(err, storage.ErrChunkPutFinal) {
		t.Fatalf("401: %v; want final", err)
	}
	b2 := newRangeFake()
	b2.refuse = func(string) error { return errRangeRefused }
	if err := put(b2); !errors.Is(err, storage.ErrChunkPutFinal) {
		t.Fatalf("replacements used up: %v; want final", err)
	}
	b3 := newRangeFake()
	b3.refuse = func(string) error { return errors.New("connection reset") }
	if err := put(b3); err == nil || errors.Is(err, storage.ErrChunkPutFinal) {
		t.Fatalf("transient failure: %v; want retryable", err)
	}
}
