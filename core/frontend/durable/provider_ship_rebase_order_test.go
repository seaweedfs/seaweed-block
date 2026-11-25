package durable_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

type countingRebaser struct {
	mu         sync.Mutex
	lsns       []uint64
	refuseFrom int
}

func (o *countingRebaser) Observe(context.Context, uint32, uint64, []byte) error { return nil }
func (o *countingRebaser) Sync(context.Context, uint64) error                    { return nil }
func (o *countingRebaser) RebaseShipLSN(lsn uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lsns = append(o.lsns, lsn)
	if o.refuseFrom > 0 && len(o.lsns) >= o.refuseFrom {
		return errors.New("injected: writes pending")
	}
	return nil
}
func (o *countingRebaser) seen() []uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]uint64(nil), o.lsns...)
}

// B may claim before seeding. Guest I/O remains closed until the local base
// and the observer's ship position have both settled.
func restoreWithObserver(t *testing.T, obs *countingRebaser) (*layoutCase, *durable.DurableProvider, uint64, error) {
	t.Helper()
	ctx := hangGuard(t)
	c := newLayoutCase(t, 64, 4)
	source := c.provider(t, t.TempDir(), viewAs("old", 1), "old", nil)
	s := openRecovered(t, source)
	writeSync(t, s, 0, 0x7a)
	if _, err := source.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	position := c.head(t).Position
	if position == 0 {
		t.Fatal("source did not publish a nonzero B position")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	target := c.provider(t, t.TempDir(), v1View(), "r1", nil)
	if _, err := target.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if err := target.Backend("v1").SetWriteObserver(obs); err != nil {
		t.Fatal(err)
	}
	_, err := target.RecoverVolume(ctx, "v1")
	return c, target, position, err
}

func TestRestoreShipRebasePrecedesFirstServe(t *testing.T) {
	obs := &countingRebaser{}
	c, p, position, err := restoreWithObserver(t, obs)
	if err != nil {
		t.Fatal(err)
	}
	wantNext := p.LogicalStorage("v1").NextLSN()
	seen := obs.seen()
	if len(seen) < 2 || seen[len(seen)-1] != wantNext || wantNext <= position {
		t.Fatalf("ship order not rebased before serving: rebases=%v next=%d B position=%d", seen, wantNext, position)
	}
	got := make([]byte, 4096)
	if n, err := p.Backend("v1").Read(context.Background(), 0, got); err != nil || n != len(got) || !bytes.Equal(got, block(0x7a)) {
		t.Fatalf("restored bytes before serving: n=%d err=%v", n, err)
	}
	if head := c.head(t); head.Position != position {
		t.Fatalf("restore changed published position: %+v", head)
	}
}

func TestRestoreRefusedShipRebaseKeepsIOClosed(t *testing.T) {
	obs := &countingRebaser{refuseFrom: 2}
	c, p, position, err := restoreWithObserver(t, obs)
	if err == nil {
		t.Fatal("restore succeeded although observer refused post-seed rebase")
	}
	if head := c.head(t); head.Position != position {
		t.Fatalf("refused restore changed published image: %+v", head)
	}
	if _, rerr := p.Backend("v1").Read(context.Background(), 0, make([]byte, 4096)); !errors.Is(rerr, frontend.ErrNotReady) {
		t.Fatalf("I/O after refused ship rebase: %v; want ErrNotReady", rerr)
	}
	if got := obs.seen(); len(got) != 2 {
		t.Fatalf("rebases=%v; want install + post-seed", got)
	}
}
