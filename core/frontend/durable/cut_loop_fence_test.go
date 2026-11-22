package durable

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// loopLayer is the part of a chunk layer the cut loop uses.
type loopLayer struct {
	storage.ChunkLayer // unused methods panic
	fenced             atomic.Bool
	cuts               atomic.Int32
}

func (l *loopLayer) Fenced() bool                   { return l.fenced.Load() }
func (l *loopLayer) Stats() storage.ChunkLayerStats { return storage.ChunkLayerStats{} }
func (l *loopLayer) Cut(context.Context) (storage.ChunkCutResult, error) {
	l.cuts.Add(1)
	return storage.ChunkCutResult{}, nil
}

// A fenced layer can never publish again: its loop ends instead of
// retrying refused cuts forever.
func TestChunkLoop_EndsWhenTheLayerIsFenced(t *testing.T) {
	p := &DurableProvider{cfg: ProviderConfig{ChunkCaptureInterval: time.Millisecond}}
	layer := &loopLayer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go p.chunkLoop(ctx, "v1", layer, done, nil)
	deadline := time.Now().Add(5 * time.Second)
	for layer.cuts.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the loop never cut before the fence")
		}
		time.Sleep(10 * time.Millisecond)
	}
	layer.fenced.Store(true)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the cut loop kept running after the layer was fenced")
	}
}

// refusingLayer refuses every cut with err.
type refusingLayer struct {
	storage.ChunkLayer // unused methods panic
	err                error
	cuts               atomic.Int32
}

func (l *refusingLayer) Fenced() bool                   { return false }
func (l *refusingLayer) Stats() storage.ChunkLayerStats { return storage.ChunkLayerStats{} }
func (l *refusingLayer) Cut(context.Context) (storage.ChunkCutResult, error) {
	l.cuts.Add(1)
	return storage.ChunkCutResult{}, l.err
}

// The timer holds a host ticket while a refusal is a wait, and gives it up,
// with the reason in status, on a refusal that waiting cannot fix.
func TestChunkLoop_TicketKeptForAWaitDroppedOtherwise(t *testing.T) {
	c := hostlimit.DefaultConfig(t.TempDir())
	lim, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	tickets := func() int {
		ents, _ := os.ReadDir(filepath.Join(c.Directory, "tickets"))
		return len(ents)
	}
	for _, tc := range []struct {
		name     string
		err      error
		keep     bool
		dropText string
	}{
		{"wait", fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrNotHead), true, ""},
		{"oversized payload", fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrBusy), false, "no background slot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &DurableProvider{cfg: ProviderConfig{ChunkCaptureInterval: time.Millisecond, ChunkBudget: lim}}
			layer := &refusingLayer{err: tc.err}
			wait := &cutWaitSlot{}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go p.chunkLoop(ctx, "v1", layer, done, wait)
			for deadline := time.Now().Add(5 * time.Second); layer.cuts.Load() < 3; time.Sleep(5 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("the loop did not try")
				}
			}
			st := wait.status(0)
			if tc.keep && (tickets() != 1 || st.TicketDueHostNs == 0) {
				t.Fatalf("a wait must keep one ticket: files=%d status=%+v", tickets(), st)
			}
			if !tc.keep && (st.TicketDueHostNs != 0 || !strings.Contains(st.TicketDropReason, tc.dropText)) {
				t.Fatalf("a refusal that waiting cannot fix must drop the ticket with its reason: %+v", st)
			}
			cancel()
			<-done
			if tickets() != 0 {
				t.Fatalf("%d ticket files left after the loop ended", tickets())
			}
		})
	}
}
