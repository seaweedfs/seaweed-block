package durable_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
)

type lockedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// waitLog waits until the log holds want, then returns the log.
func waitLog(t *testing.T, l *lockedLog, want string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(l.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("log never held %q:\n%s", want, l.String())
		}
		time.Sleep(time.Millisecond)
	}
	return l.String()
}

// inOrder checks that every line holding a want[i] fragment set appears,
// each after the previous one.
func inOrder(t *testing.T, logs string, want ...[]string) {
	t.Helper()
	lines := strings.Split(logs, "\n")
	at := 0
next:
	for _, frags := range want {
		for ; at < len(lines); at++ {
			ok := true
			for _, f := range frags {
				ok = ok && strings.Contains(lines[at], f)
			}
			if ok {
				at++
				continue next
			}
		}
		t.Fatalf("no line with %q after the previous expected line:\n%s", frags, logs)
	}
}

// The deferred attach logs its wait (with the projection fields that
// fail), the first attach attempt and how the wait ended. B Start's HEAD
// claim is observed from the catalog rather than a legacy claim log line.
func TestChunkAuthorityWaitLogsTheWaitTheAttachAndTheEnd(t *testing.T) {
	logs := &lockedLog{}
	captureWriteLogs(t, logs)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4}
	fixture := testutil.NewLayoutFixture(t, "v1", 64, 4)
	fixture.Configure(&cfg)
	p, err := durable.NewDurableProvider(cfg, v)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	v.active.Store(true)
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "waits for master authority")
	close(v.assigned)
	got := waitLog(t, logs, "chunk authority wait for volume=v1 ended after")
	inOrder(t, got,
		[]string{"volume=v1 waits for master authority (up to 30s): projection lacks healthy=false epoch=0"},
		[]string{"volume=v1: master authority after", `replica="r1" epoch=3 endpoint=1`, "first attach attempt"},
		[]string{"chunk authority wait for volume=v1 ended after", ": attached after 1 attempt(s)"},
	)
	head, found, err := fixture.Catalog.ReadHead(ctx, fixture.Ref)
	if err != nil || !found || head.Epoch != 3 || head.Holder != "r1" || head.ClaimID == "" {
		t.Fatalf("B Start did not claim after the wait: %+v found=%v err=%v", head, found, err)
	}
	if strings.Count(got, "first attach attempt") != 1 {
		t.Fatalf("first attach attempt logged more than once:\n%s", got)
	}
}

// A wait that reaches its bound logs the assignment-unavailable transition
// with the fields still missing; a wait ended by Close says so.
func TestChunkAuthorityWaitLogsDeadlineAndClose(t *testing.T) {
	for _, mode := range []string{"limit", "close"} {
		t.Run(mode, func(t *testing.T) {
			logs := &lockedLog{}
			captureWriteLogs(t, logs)
			v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
			limit := 40 * time.Millisecond
			if mode == "close" {
				limit = time.Minute
			}
			cfg := durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
				ChunkAssignmentTimeout: limit,
			}
			testutil.NewLayoutFixture(t, "v1", 64, 4).Configure(&cfg)
			p, err := durable.NewDurableProvider(cfg, v)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			v.active.Store(true)
			if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatal(err)
			}
			want := "ended after"
			if mode == "close" {
				waitLog(t, logs, "waits for master authority")
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
			}
			got := waitLog(t, logs, want)
			end := ": assignment unavailable, not serving (projection lacks healthy=false epoch=0)"
			if mode == "close" {
				end = ": provider closed"
			}
			inOrder(t, got,
				[]string{"volume=v1 waits for master authority (up to " + limit.String() + "): projection lacks healthy=false epoch=0"},
				[]string{"chunk authority wait for volume=v1 ended after", end},
			)
			if strings.Contains(got, "first attach attempt") || strings.Contains(got, "HEAD claim for") {
				t.Fatalf("attach logged without authority:\n%s", got)
			}
		})
	}
}

// headFailStartStore fails the layout HEAD read while fails > 0 (forever when < 0).
type headFailStartStore struct {
	layout.StartStore
	fails atomic.Int32
}

var errHeadRead = errors.New("injected HEAD read failure")

func (r *headFailStartStore) ReadHead(ctx context.Context) (layout.Head, bool, error) {
	if n := r.fails.Load(); n != 0 {
		if n > 0 {
			r.fails.Add(-1)
		}
		return layout.Head{}, false, errHeadRead
	}
	return r.StartStore.ReadHead(ctx)
}

// A claim whose first HEAD read fails logs its start and its failure with
// the elapsed time; the retry then claims. Close during the retry backoff
// ends the wait as closed, not superseded.
func TestChunkAuthorityWaitLogsAFailedClaim(t *testing.T) {
	for _, mode := range []string{"retried", "closed in backoff"} {
		t.Run(mode, func(t *testing.T) {
			logs := &lockedLog{}
			captureWriteLogs(t, logs)
			fixture := testutil.NewLayoutFixture(t, "v1", 64, 4)
			st, err := fixture.Catalog.Store(fixture.Ref)
			if err != nil {
				t.Fatal(err)
			}
			store := &headFailStartStore{StartStore: st}
			store.fails.Store(1)
			if mode != "retried" {
				store.fails.Store(-1)
			}
			v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
			cfg := durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
			}
			fixture.Configure(&cfg)
			cfg.ChunkLayoutHeadFor = func(string) (layout.StartStore, error) { return store, nil }
			p, err := durable.NewDurableProvider(cfg, v)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			v.active.Store(true)
			if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatal(err)
			}
			waitLog(t, logs, "waits for master authority")
			close(v.assigned)
			if mode == "retried" {
				got := waitLog(t, logs, "chunk authority wait for volume=v1 ended after")
				inOrder(t, got,
					[]string{"chunk layer attach for volume=v1:", errHeadRead.Error(), "(retrying)"},
					[]string{"chunk authority wait for volume=v1 ended after", ": attached after 2 attempt(s)"},
				)
				head, found, err := fixture.Catalog.ReadHead(context.Background(), fixture.Ref)
				if err != nil || !found || head.Epoch != 3 || head.Holder != "r1" {
					t.Fatalf("retry did not claim B HEAD: %+v found=%v err=%v", head, found, err)
				}
				return
			}
			waitLog(t, logs, "(retrying)")
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			got := waitLog(t, logs, "chunk authority wait for volume=v1 ended after")
			inOrder(t, got,
				[]string{"chunk layer attach for volume=v1:", errHeadRead.Error(), "(retrying)"},
				[]string{"chunk authority wait for volume=v1 ended after", ": provider closed during the retry backoff"},
			)
		})
	}
}
