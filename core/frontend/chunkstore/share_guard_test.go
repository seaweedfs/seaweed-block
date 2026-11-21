package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// recGuard records Begin and Finish; onFinish runs inside Finish.
type recGuard struct {
	mu        sync.Mutex
	events    []string
	n         int
	finishErr error
	onFinish  func()
}

func (g *recGuard) Begin(_ context.Context, vol string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	g.events = append(g.events, "begin:"+vol)
	return fmt.Sprintf("tok-%d", g.n), nil
}

func (g *recGuard) Finish(_ context.Context, vol, token string, head storage.ChunkHead) error {
	if g.onFinish != nil {
		g.onFinish()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.events = append(g.events, fmt.Sprintf("finish:%s:%s:%d", vol, token, head.Gen))
	return g.finishErr
}

func (g *recGuard) list() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.events...)
}

func countUnder(f *fakeFiler, prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for k := range f.entries {
		if strings.HasPrefix(k, prefix) {
			n++
		}
	}
	return n
}

// A snapshot of a passed-in HEAD begins the share, checks the HEAD is still
// current, and finishes it before the snapshot entry is written.
func TestShareGuard_SnapshotFinishesBeforeActivation(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	g := &recGuard{}
	g.onFinish = func() {
		if n := countUnder(f, catBase+"/v1/snaps/s1/"); n != 0 {
			t.Errorf("snapshot entry written before Finish (%d entries)", n)
		}
	}
	c.SetShareGuard(g)
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	want := []string{"begin:v1", fmt.Sprintf("finish:v1:tok-1:%d", h.Gen)}
	if got := g.list(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("guard events %v; want %v", got, want)
	}
	if countUnder(f, catBase+"/v1/snaps/s1/") == 0 {
		t.Fatal("snapshot not written after a successful Finish")
	}
}

// A refused Finish aborts the snapshot: nothing activated, its intent dropped.
func TestShareGuard_RefusedFinishAbortsTheSnapshot(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	intents := countUnder(f, catBase+"/v1/intents/")
	c.SetShareGuard(&recGuard{finishErr: errors.New("token expired")})
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err == nil {
		t.Fatal("snapshot activated after a refused Finish")
	}
	if n := countUnder(f, catBase+"/v1/snaps/s1/"); n != 0 {
		t.Fatalf("%d snapshot entries after a refused Finish", n)
	}
	if n := countUnder(f, catBase+"/v1/intents/"); n != intents {
		t.Fatalf("intents %d -> %d; want the snapshot's intent dropped", intents, n)
	}
}

// A passed-in HEAD that is no longer current is refused before Finish.
func TestShareGuard_StaleHeadIsRefused(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	publishSource(t, f, b, "v1", map[uint64][]byte{5: chunkBytes(0x20)}) // HEAD moves on
	g := &recGuard{}
	c.SetShareGuard(g)
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err == nil {
		t.Fatal("snapshot of a superseded HEAD accepted with a guard")
	}
	for _, e := range g.list() {
		if strings.HasPrefix(e, "finish:") {
			t.Fatalf("Finish called for a refused share: %v", g.list())
		}
	}
	if n := countUnder(f, catBase+"/v1/snaps/s1/"); n != 0 {
		t.Fatalf("%d snapshot entries for a refused share", n)
	}
}

// A caller that began the share itself passes its token: no second Begin.
func TestShareGuard_CallerToken(t *testing.T) {
	_, _, c, h := catalogFixture(t)
	g := &recGuard{}
	c.SetShareGuard(g)
	if _, err := c.CreateSnapshot(context.Background(), "v1", SnapshotRequest{Name: "s1", RequestID: "r", ShareToken: "owner-tok"}, h); err != nil {
		t.Fatal(err)
	}
	if got := g.list(); len(got) != 1 || got[0] != fmt.Sprintf("finish:v1:owner-tok:%d", h.Gen) {
		t.Fatalf("guard events %v; want only Finish with the caller's token", got)
	}
}

// A fork begins the share before reading the snapshot and finishes it with
// the snapshot's HEAD before writing the child's HEAD; a refused Finish
// writes no child HEAD.
func TestShareGuard_Fork(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	g := &recGuard{}
	g.onFinish = func() {
		if countUnder(f, catBase+"/v2/HEAD") != 0 {
			t.Error("child HEAD written before Finish")
		}
	}
	c.SetShareGuard(g)
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork-1"}); err != nil {
		t.Fatal(err)
	}
	want := []string{"begin:v1", fmt.Sprintf("finish:v1:tok-1:%d", h.Gen)}
	if got := g.list(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("guard events %v; want %v", got, want)
	}

	c.SetShareGuard(&recGuard{finishErr: errors.New("token expired")})
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v3", RequestID: "fork-2"}); err == nil {
		t.Fatal("fork activated after a refused Finish")
	}
	if countUnder(f, catBase+"/v3/HEAD") != 0 {
		t.Fatal("child HEAD written after a refused Finish")
	}
}
