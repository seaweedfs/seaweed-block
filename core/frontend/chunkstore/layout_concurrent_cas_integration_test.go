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
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	arrived := make(chan int, casWriters)
	release := make(chan struct{})
	outcomes := make(chan gateOutcome, casWriters)
	stores := make([]*LayoutStore, casWriters)
	for i := range stores {
		cl := c.client(t, root+"/")
		cl.fc = &writeGate{SeaweedFilerClient: cl.fc, index: i, arrived: arrived, release: release, outcomes: outcomes}
		s, err := NewLayoutStore(cl, cl, root, id)
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = s
	}
	errs = make([]error, casWriters)
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = op(ctx, i, stores[i])
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
// one object entry, and for conditional deletes of it.
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
	headIs := func(t *testing.T, s *LayoutStore, gen uint64, want layout.Head) {
		t.Helper()
		got, found, err := s.ReadHead(context.Background())
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
		root, id, s := newVolume(t, "update")
		if err := s.CreateHead(context.Background(), layout.Unclaimed(id, nil)); err != nil {
			t.Fatal(err)
		}
		for round := 0; round < casRounds; round++ {
			gen := uint64(round + 1)
			proposal := func(i int) layout.Head { return at(id, uint64(1000*round+i+1)) }
			errs, filer := raceRound(t, c, root, id, func(ctx context.Context, i int, s *LayoutStore) error {
				return s.UpdateHead(ctx, gen, proposal(i))
			})
			won := oneWinner(t, fmt.Sprintf("round %d", round), errs, filer)
			headIs(t, s, gen+1, proposal(won))
		}
	})

	t.Run("exclusive HEAD create", func(t *testing.T) {
		for round := 0; round < casRounds; round++ {
			root, id, s := newVolume(t, "create")
			proposal := func(i int) layout.Head { return at(id, uint64(i+1)) }
			errs, filer := raceRound(t, c, root, id, func(ctx context.Context, i int, s *LayoutStore) error {
				return s.UpdateHead(ctx, 0, proposal(i))
			})
			won := oneWinner(t, fmt.Sprintf("round %d", round), errs, filer)
			headIs(t, s, 1, proposal(won))
		}
	})

	t.Run("MARK of one object entry", func(t *testing.T) {
		root, id, s := newVolume(t, "mark")
		for round := 0; round < casRounds; round++ {
			data := []byte(fmt.Sprintf("orphan-%d", round))
			rel := layout.NameChunk(uint64(round+1), data).Path()
			if err := layout.EnsureObject(context.Background(), s, rel, data, "s0"); err != nil {
				t.Fatal(err)
			}
			plan, found, err := s.Stat(context.Background(), rel)
			if err != nil || !found {
				t.Fatalf("object: found %v, %v", found, err)
			}
			mark := func(i int) string { return fmt.Sprintf("c%d-%d", round, i) }
			errs, filer := raceRound(t, c, root, id, func(ctx context.Context, i int, s *LayoutStore) error {
				return s.Mark(ctx, rel, plan, mark(i))
			})
			won := oneWinner(t, fmt.Sprintf("round %d", round), errs, filer)
			if m, _, _ := s.Stat(context.Background(), rel); m.Mark != mark(won) {
				t.Fatalf("round %d: entry carries mark %q; want the winner's %q", round, m.Mark, mark(won))
			}
		}
	})

	t.Run("conditional delete of one object entry", func(t *testing.T) {
		root, id, s := newVolume(t, "delete")
		for round := 0; round < casRounds; round++ {
			data := []byte(fmt.Sprintf("marked-%d", round))
			rel := layout.NameChunk(uint64(round+1), data).Path()
			if err := layout.EnsureObject(context.Background(), s, rel, data, "s0"); err != nil {
				t.Fatal(err)
			}
			plan, _, _ := s.Stat(context.Background(), rel)
			const own = "c-own"
			if err := s.Mark(context.Background(), rel, plan, own); err != nil {
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
			if _, found, _ := s.Stat(context.Background(), rel); found {
				t.Fatalf("round %d: the entry survived its own conditional delete", round)
			}
		}
	})
}
