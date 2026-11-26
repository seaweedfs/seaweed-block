//go:build chunkstore_integration

package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// writeGate does not implement any condition: every write is the real filer
// RPC. It holds each write until every writer of the round has reached it
// (after all client-side pre-reads), releases them together, and records
// the real outcome, so a client-side refusal cannot stand in for the
// filer's conditional write.
type writeGate struct {
	filerpb.SeaweedFilerClient
	index    int
	arrived  chan<- int
	release  <-chan struct{}
	outcomes chan<- gateOutcome
	lag      time.Duration // after the common release
}

type gateOutcome struct {
	index int
	ok    bool
	what  string
}

func (g *writeGate) hold(ctx context.Context) error {
	select {
	case g.arrived <- g.index:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-g.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if g.lag > 0 {
		time.Sleep(g.lag)
	}
	return nil
}

func (g *writeGate) CreateEntry(ctx context.Context, in *filerpb.CreateEntryRequest, opts ...grpc.CallOption) (*filerpb.CreateEntryResponse, error) {
	if err := g.hold(ctx); err != nil {
		return nil, err
	}
	resp, err := g.SeaweedFilerClient.CreateEntry(ctx, in, opts...)
	ok := err == nil && resp.GetErrorCode() == filerpb.FilerError_OK && resp.GetError() == ""
	g.outcomes <- gateOutcome{g.index, ok, fmt.Sprintf("grpc=%s code=%s", status.Code(err), resp.GetErrorCode())}
	return resp, err
}

func (g *writeGate) UpdateEntry(ctx context.Context, in *filerpb.UpdateEntryRequest, opts ...grpc.CallOption) (*filerpb.UpdateEntryResponse, error) {
	if err := g.hold(ctx); err != nil {
		return nil, err
	}
	resp, err := g.SeaweedFilerClient.UpdateEntry(ctx, in, opts...)
	g.outcomes <- gateOutcome{g.index, err == nil, fmt.Sprintf("grpc=%s", status.Code(err))}
	return resp, err
}

func (g *writeGate) DeleteEntry(ctx context.Context, in *filerpb.DeleteEntryRequest, opts ...grpc.CallOption) (*filerpb.DeleteEntryResponse, error) {
	if err := g.hold(ctx); err != nil {
		return nil, err
	}
	resp, err := g.SeaweedFilerClient.DeleteEntry(ctx, in, opts...)
	g.outcomes <- gateOutcome{g.index, err == nil && resp.GetError() == "", fmt.Sprintf("grpc=%s", status.Code(err))}
	return resp, err
}

const (
	casWriters = 16 // writers per round, as the store's earlier concurrent test
	casRounds  = 4
)

// raceRound gives each of n writers its own real filer connection behind a
// gate, runs op on all of them at once, and returns each writer's result and
// the filer's real outcome per writer.
func raceRound(t *testing.T, c *weedCluster, root string, id layout.VolumeID, op func(ctx context.Context, i int, s *LayoutStore) error) (errs []error, filer []gateOutcome) {
	return raceRoundLag(t, c, root, id, nil, op)
}

// raceRoundLag is raceRound with lag(i) added to writer i after the common
// release (nil: none), to bias which kind of writer reaches the filer first.
func raceRoundLag(t *testing.T, c *weedCluster, root string, id layout.VolumeID, lag func(i int) time.Duration, op func(ctx context.Context, i int, s *LayoutStore) error) (errs []error, filer []gateOutcome) {
	t.Helper()
	return newRacers(t, c, root, id).round(t, lag, op)
}

// racers are casWriters gated writers, each on its own real filer
// connection, kept across rounds (many rounds would otherwise open a
// connection per writer per round).
type racers struct {
	gates  []*writeGate
	stores []*LayoutStore
}

func newRacers(t *testing.T, c *weedCluster, root string, id layout.VolumeID) *racers {
	t.Helper()
	r := &racers{}
	for i := 0; i < casWriters; i++ {
		cl := c.client(t, root+"/")
		g := &writeGate{SeaweedFilerClient: cl.fc, index: i}
		cl.fc = g
		s, err := NewLayoutStore(cl, cl, root, id)
		if err != nil {
			t.Fatal(err)
		}
		r.gates, r.stores = append(r.gates, g), append(r.stores, s)
	}
	return r
}

// round runs op on every writer at once: each is held at its filer write
// until all have arrived, then all are released together (plus lag(i)).
func (r *racers) round(t *testing.T, lag func(i int) time.Duration, op func(ctx context.Context, i int, s *LayoutStore) error) (errs []error, filer []gateOutcome) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	arrived := make(chan int, casWriters)
	release := make(chan struct{})
	outcomes := make(chan gateOutcome, casWriters)
	for i, g := range r.gates {
		g.arrived, g.release, g.outcomes, g.lag = arrived, release, outcomes, 0
		if lag != nil {
			g.lag = lag(i)
		}
	}
	errs = make([]error, casWriters)
	var wg sync.WaitGroup
	for i := range r.stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = op(ctx, i, r.stores[i])
		}(i)
	}
	var once sync.Once
	defer once.Do(func() { close(release) })
	seen := map[int]bool{}
	for len(seen) < casWriters {
		select {
		case i := <-arrived:
			if seen[i] {
				t.Fatalf("writer %d reached the filer twice", i)
			}
			seen[i] = true
		case <-ctx.Done():
			t.Fatalf("only %d/%d writers reached the filer write: %v", len(seen), casWriters, ctx.Err())
		}
	}
	once.Do(func() { close(release) })
	wg.Wait()
	close(outcomes)
	for o := range outcomes {
		filer = append(filer, o)
	}
	if len(filer) != casWriters {
		t.Fatalf("%d filer outcomes recorded; want %d", len(filer), casWriters)
	}
	return errs, filer
}

// oneWinner requires exactly one writer to succeed at the filer and in its
// caller, every other caller refused with a precondition, and returns the
// winner.
func oneWinner(t *testing.T, step string, errs []error, filer []gateOutcome) int {
	t.Helper()
	won := -1
	for _, o := range filer {
		if o.ok {
			if won >= 0 {
				t.Fatalf("%s: writers %d and %d both succeeded at the filer", step, won, o.index)
			}
			won = o.index
		}
	}
	if won < 0 {
		t.Fatalf("%s: no writer succeeded at the filer: %+v", step, filer)
	}
	for i, err := range errs {
		switch {
		case i == won && err != nil:
			t.Fatalf("%s: the filer's winner %d reported %v", step, i, err)
		case i != won && !errors.Is(err, layout.ErrPrecondition):
			t.Fatalf("%s: writer %d lost at the filer but reported %v; want a precondition refusal", step, i, err)
		}
	}
	return won
}

func logWeedBinary(t *testing.T) {
	t.Helper()
	f, err := os.Open(os.Getenv("CHUNKSTORE_WEED"))
	if err != nil {
		return
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err == nil {
		t.Logf("real filer binary SHA256=%x", h.Sum(nil))
	}
}

// Concurrent conditional writes on a real filer (stock weed): of 16 writers
// released together with the same condition, the filer lets exactly one
// through and refuses the rest, and the entry holds the winner's bytes. For
// HEAD updates at one generation, for exclusive HEAD creates, for MARKs on
// one object entry, for conditional deletes of it, and for a delete raced
// by reuses of the same marked entry.
func TestIntegration_ConcurrentConditionalWritesOneWinner(t *testing.T) {
	c := startWeed(t)
	logWeedBinary(t)
	rn := layout.RootName{Hash: layout.Sum([]byte("root"))}
	at := func(vol layout.VolumeID, pos uint64) layout.Head {
		return layout.Head{VolumeID: vol, Epoch: 3, Holder: "node/r", ClaimID: "claim", Activity: layout.Activity{Kind: layout.ActIdle}, Current: &rn, Position: pos, State: layout.StateLive}
	}
	newVolume := func(t *testing.T, kind string) (string, layout.VolumeID, *LayoutStore) {
		t.Helper()
		id, err := layout.NewVolumeID()
		if err != nil {
			t.Fatal(err)
		}
		root := "/swblock/cas-" + kind + "/" + layout.VolumeDir("v1", id)
		cl := c.client(t, root+"/")
		s, err := NewLayoutStore(cl, cl, root, id)
		if err != nil {
			t.Fatal(err)
		}
		return root, id, s
	}
	// statOf reads an object's metadata back; a failed read fails the test
	// (it is never taken as an absent entry).
	statOf := func(ctx context.Context, t *testing.T, s *LayoutStore, rel string) (layout.ObjectMeta, bool) {
		t.Helper()
		m, found, err := s.Stat(ctx, rel)
		if err != nil {
			t.Fatalf("read back %s: %v", rel, err)
		}
		return m, found
	}
	headIs := func(ctx context.Context, t *testing.T, s *LayoutStore, gen uint64, want layout.Head) {
		t.Helper()
		got, found, err := s.ReadHead(ctx)
		if err != nil || !found {
			t.Fatalf("HEAD: found %v, %v", found, err)
		}
		a, _ := got.Encode()
		b, _ := want.Encode()
		if got.Gen != gen || !bytes.Equal(a, b) {
			t.Fatalf("HEAD at gen %d position %d; want the winner's bytes at gen %d (position %d)", got.Gen, got.Position, gen, want.Position)
		}
	}

	t.Run("HEAD update at one generation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		root, id, s := newVolume(t, "update")
		if err := s.CreateHead(ctx, layout.Unclaimed(id, nil)); err != nil {
			t.Fatal(err)
		}
		for round := 0; round < casRounds; round++ {
			gen := uint64(round + 1)
			proposal := func(i int) layout.Head { return at(id, uint64(1000*round+i+1)) }
			errs, filer := raceRound(t, c, root, id, func(ctx context.Context, i int, s *LayoutStore) error {
				return s.UpdateHead(ctx, gen, proposal(i))
			})
			won := oneWinner(t, fmt.Sprintf("round %d", round), errs, filer)
			headIs(ctx, t, s, gen+1, proposal(won))
		}
	})

	t.Run("exclusive HEAD create", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		for round := 0; round < casRounds; round++ {
			root, id, s := newVolume(t, "create")
			proposal := func(i int) layout.Head { return at(id, uint64(i+1)) }
			errs, filer := raceRound(t, c, root, id, func(ctx context.Context, i int, s *LayoutStore) error {
				return s.UpdateHead(ctx, 0, proposal(i))
			})
			won := oneWinner(t, fmt.Sprintf("round %d", round), errs, filer)
			headIs(ctx, t, s, 1, proposal(won))
		}
	})

	t.Run("MARK of one object entry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		root, id, s := newVolume(t, "mark")
		for round := 0; round < casRounds; round++ {
			data := []byte(fmt.Sprintf("orphan-%d", round))
			rel := layout.NameChunk(uint64(round+1), data).Path()
			if err := layout.EnsureObject(ctx, s, rel, data, "s0"); err != nil {
				t.Fatal(err)
			}
			plan, found, err := s.Stat(ctx, rel)
			if err != nil || !found {
				t.Fatalf("object: found %v, %v", found, err)
			}
			mark := func(i int) string { return fmt.Sprintf("c%d-%d", round, i) }
			errs, filer := raceRound(t, c, root, id, func(ctx context.Context, i int, s *LayoutStore) error {
				return s.Mark(ctx, rel, plan, mark(i))
			})
			won := oneWinner(t, fmt.Sprintf("round %d", round), errs, filer)
			if m, _ := statOf(ctx, t, s, rel); m.Mark != mark(won) {
				t.Fatalf("round %d: entry carries mark %q; want the winner's %q", round, m.Mark, mark(won))
			}
		}
	})

	t.Run("conditional delete of one object entry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		root, id, s := newVolume(t, "delete")
		for round := 0; round < casRounds; round++ {
			data := []byte(fmt.Sprintf("marked-%d", round))
			rel := layout.NameChunk(uint64(round+1), data).Path()
			if err := layout.EnsureObject(ctx, s, rel, data, "s0"); err != nil {
				t.Fatal(err)
			}
			plan, found := statOf(ctx, t, s, rel)
			if !found {
				t.Fatalf("round %d: object %s missing after create", round, rel)
			}
			const own = "c-own"
			if err := s.Mark(ctx, rel, plan, own); err != nil {
				t.Fatal(err)
			}
			// Writer 0 holds the entry's mark; the 15 others name marks the
			// entry never carried. Only writer 0 may delete it; a refused
			// delete of an entry already gone is no error to its caller, so
			// the filer's own outcomes decide.
			errs, filer := raceRound(t, c, root, id, func(ctx context.Context, i int, s *LayoutStore) error {
				if i == 0 {
					return s.DeleteMarked(ctx, rel, own)
				}
				return s.DeleteMarked(ctx, rel, fmt.Sprintf("c-other-%d", i))
			})
			for _, o := range filer {
				if o.ok && o.index != 0 {
					t.Fatalf("round %d: writer %d deleted the entry with a mark it never carried (%s)", round, o.index, o.what)
				}
				if !o.ok && o.index == 0 {
					t.Fatalf("round %d: the delete with the entry's own mark was refused (%s)", round, o.what)
				}
			}
			for i, err := range errs {
				if err != nil && !errors.Is(err, layout.ErrPrecondition) {
					t.Fatalf("round %d: writer %d: %v", round, i, err)
				}
			}
			if _, found := statOf(ctx, t, s, rel); found {
				t.Fatalf("round %d: the entry survived its own conditional delete", round)
			}
		}
	})

	// A marked entry raced by its remover's DELETE and a successor's REUSE:
	// half the writers delete with the entry's mark, half reuse it on the
	// metadata they read (that mark). The filer lets exactly one through:
	// either a delete, and every reuse is refused (the entry is gone), or
	// one reuse, and every delete and every other reuse is refused (the
	// entry stays, unmarked, under the reuser's session). Never both.
	// casSimultaneous rounds release both kinds at the same instant (the
	// race itself, whichever wins); the lagged rounds then hold one kind
	// back a few milliseconds so that each outcome is seen at least once.
	t.Run("DELETE against REUSE of one marked entry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		root, id, s := newVolume(t, "delreuse")
		r := newRacers(t, c, root, id)
		const casSimultaneous = 128
		const m = "c-mark"
		race := func(round int, lag func(i int) time.Duration) (deleteWon bool) {
			t.Helper()
			data := []byte(fmt.Sprintf("raced-%d", round))
			rel := layout.NameChunk(uint64(round+1), data).Path()
			if err := layout.EnsureObject(ctx, s, rel, data, "s0"); err != nil {
				t.Fatal(err)
			}
			plan, found := statOf(ctx, t, s, rel)
			if !found {
				t.Fatalf("round %d: object %s missing after create", round, rel)
			}
			if err := s.Mark(ctx, rel, plan, m); err != nil {
				t.Fatal(err)
			}
			marked := layout.ObjectMeta{Session: "s0", Mark: m, Volume: id}
			reuser := func(i int) string { return fmt.Sprintf("r%d-%d", round, i) }
			errs, filer := r.round(t, lag, func(ctx context.Context, i int, s *LayoutStore) error {
				if i%2 == 0 {
					return s.DeleteMarked(ctx, rel, m)
				}
				return s.Reuse(ctx, rel, marked, reuser(i))
			})
			won := -1
			for _, o := range filer {
				if o.ok {
					if won >= 0 {
						t.Fatalf("round %d: writers %d and %d both succeeded at the filer (%s)", round, won, o.index, o.what)
					}
					won = o.index
				}
			}
			if won < 0 {
				t.Fatalf("round %d: neither a delete nor a reuse succeeded: %+v", round, filer)
			}
			if errs[won] != nil {
				t.Fatalf("round %d: the filer's winner %d reported %v", round, won, errs[won])
			}
			meta, found := statOf(ctx, t, s, rel)
			if won%2 == 0 {
				if found {
					t.Fatalf("round %d: delete %d won but the entry remains: %+v", round, won, meta)
				}
				for i := 1; i < casWriters; i += 2 {
					if !errors.Is(errs[i], layout.ErrPrecondition) {
						t.Fatalf("round %d: reuse %d after the delete: %v; want refused", round, i, errs[i])
					}
				}
				// The other deletes lost at the filer; a refused delete of an
				// entry already gone is no error to its caller, and nothing else
				// is allowed.
				for i := 0; i < casWriters; i += 2 {
					if i != won && errs[i] != nil && !errors.Is(errs[i], layout.ErrPrecondition) {
						t.Fatalf("round %d: delete %d after delete %d won: %v", round, i, won, errs[i])
					}
				}
				return true
			}
			if !found || meta != (layout.ObjectMeta{Session: reuser(won), Volume: id}) {
				t.Fatalf("round %d: reuse %d won; entry %+v (found %v); want its session, unmarked", round, won, meta, found)
			}
			for i, err := range errs {
				if i != won && !errors.Is(err, layout.ErrPrecondition) {
					t.Fatalf("round %d: writer %d after reuse %d won: %v; want refused", round, i, won, err)
				}
			}
			return false
		}
		simDeletes := 0
		for round := 0; round < casSimultaneous; round++ {
			if race(round, nil) {
				simDeletes++
			}
		}
		t.Logf("%d simultaneous rounds: a delete won %d, a reuse won %d, never both", casSimultaneous, simDeletes, casSimultaneous-simDeletes)
		deletes, reuses := 0, 0
		for k := 0; k < 2*casRounds; k++ {
			round := casSimultaneous + k
			// Alternate rounds hold back the reuses or the deletes.
			lag := func(i int) time.Duration {
				if (i%2 == 0) == (k%2 == 1) {
					return 5 * time.Millisecond
				}
				return 0
			}
			if race(round, lag) {
				deletes++
			} else {
				reuses++
			}
		}
		t.Logf("%d lagged rounds: a delete won %d, a reuse won %d", 2*casRounds, deletes, reuses)
		if deletes == 0 || reuses == 0 {
			t.Fatalf("only one outcome occurred in the lagged rounds (deletes %d, reuses %d): the other branch was never checked", deletes, reuses)
		}
	})
}
