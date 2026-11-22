package durable_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// servedRoot: root A of holder rA served at epoch 3 and published once.
func servedRoot(t *testing.T) (*storage.MemChunkRemote, string) {
	t.Helper()
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	root := t.TempDir()
	p := chunkProvider(t, root, remote, viewAs("rA", 3), 0)
	s := openRecovered(t, p)
	writeSync(t, s, 0, 0xA1)
	if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	return remote, root
}

// foreignHead installs a HEAD of holder rB at epoch.
func foreignHead(t *testing.T, remote *storage.MemChunkRemote, epoch uint64) {
	t.Helper()
	ctx := context.Background()
	head, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next := head
	next.Epoch, next.Holder, next.HolderRoot = epoch, "rB", "bbbbbbbbbbbbbbbb"
	if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{Gen: head.Gen, Epoch: head.Epoch}, next); err != nil {
		t.Fatal(err)
	}
}

// recoverUnassigned recovers root as rA with no assignment yet.
func recoverUnassigned(t *testing.T, remote *storage.MemChunkRemote, root string) (*durable.DurableProvider, error) {
	t.Helper()
	p := chunkProviderAs(t, root, remote, newStubView(frontend.Projection{}), 0, "rA")
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	_, err := p.RecoverVolume(context.Background(), "v1")
	return p, err
}

// V1-SPEC r2.38 (TERM-STOP-STALE-ROOT-001), early check: before any
// assignment, an existing root whose HEAD another holder published at an
// epoch at or above the root's recorded epoch ends its startup with no retry
// (stale_local_root) and the cure; a lower epoch, or a root without a
// record, takes the normal path (recovery waits for the assignment).
func TestEarlyStaleRootCheckBeforeTheAssignmentWait(t *testing.T) {
	for _, tc := range []struct {
		name  string
		epoch uint64
		stop  bool
	}{
		{"foreign HEAD at the recorded epoch", 3, true},
		{"foreign HEAD above the recorded epoch", 4, true},
		{"foreign HEAD below the recorded epoch", 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote, root := servedRoot(t)
			if tc.epoch < 3 {
				// HEAD epochs only move forward: model a late lower-epoch
				// foreign publication on a fresh remote seeded below A.
				remote = storage.NewMemChunkRemote()
				if _, err := remote.CASHead(context.Background(), storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: tc.epoch, Holder: "rB", HolderRoot: "bbbbbbbbbbbbbbbb", State: storage.ChunkHeadActive}); err != nil {
					t.Fatal(err)
				}
			} else {
				foreignHead(t, remote, tc.epoch)
			}
			p, err := recoverUnassigned(t, remote, root)
			gen, ended := p.ChunkStartupOutcome("v1")
			reason, detail := p.ChunkStartupStop("v1", gen)
			if tc.stop {
				if !errors.Is(err, durable.ErrChunkStaleLocalRoot) || !ended || reason != "stale_local_root" || detail == "" {
					t.Fatalf("err=%v ended=%t reason=%q detail=%q, want an early stale_local_root stop with the cure", err, ended, reason, detail)
				}
				return
			}
			if err != nil || ended || reason != "" {
				t.Fatalf("err=%v ended=%t reason=%q, want the normal path (waiting for the assignment)", err, ended, reason)
			}
		})
	}

	t.Run("root without a record", func(t *testing.T) {
		remote, root := servedRoot(t)
		ids, _ := filepath.Glob(filepath.Join(root, "*.root"))
		for _, f := range ids {
			if err := os.Remove(f); err != nil {
				t.Fatal(err)
			}
		}
		foreignHead(t, remote, 4)
		p, err := recoverUnassigned(t, remote, root)
		if _, ended := p.ChunkStartupOutcome("v1"); err != nil || ended {
			t.Fatalf("err=%v ended=%t, want the normal path for a root without a record", err, ended)
		}
	})
}

// V1-SPEC r2.38: a rolled-back copy of this root is refused at attach and its
// startup ends with no retry, reason local_root_rolled_back.
func TestRolledBackRootEndsWithItsOwnReason(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	root := t.TempDir()
	a := chunkProvider(t, root, remote, v1View(), 0)
	s := openRecovered(t, a)
	writeSync(t, s, 1, 0x01)
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	backup := t.TempDir()
	copyRoot(t, root, backup)
	a = chunkProvider(t, root, remote, v1View(), 0)
	s = openRecovered(t, a)
	writeSync(t, s, 2, 0x02)
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b := chunkProvider(t, backup, remote, v1View(), 0)
	if _, err := b.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	_, err := b.RecoverVolume(ctx, "v1")
	gen, ended := b.ChunkStartupOutcome("v1")
	reason, _ := b.ChunkStartupStop("v1", gen)
	if !errors.Is(err, durable.ErrChunkStaleLocalRoot) || !ended || reason != "local_root_rolled_back" {
		t.Fatalf("err=%v ended=%t reason=%q, want local_root_rolled_back", err, ended, reason)
	}
}

// heldHeadRemote blocks GetHead until its context ends while held.
type heldHeadRemote struct {
	storage.ChunkRemote
	held    atomic.Bool
	entered chan struct{}
	once    sync.Once
}

func (r *heldHeadRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	if r.held.Load() {
		r.once.Do(func() { close(r.entered) })
		<-ctx.Done()
		return storage.ChunkHead{}, ctx.Err()
	}
	return r.ChunkRemote.GetHead(ctx)
}

// The early HEAD read (C.W: staleRootBeforeWait) is bounded and owned by
// the provider: while the read is held, Close cancels it, recovery settles
// within 3 s (the read alone would wait its 5 s bound), Close returns, and
// no stale-root stop is recorded. The test owns both workers: their cleanup
// (release the held read, then join recovery and Close with bounds) is
// registered before either starts, independent of Close cancelling.
func TestEarlyStaleRootHeadReadEndsWithProviderClose(t *testing.T) {
	mem, root := servedRoot(t)
	remote := &heldHeadRemote{ChunkRemote: mem, entered: make(chan struct{})}
	remote.held.Store(true)
	p := chunkProviderAs(t, root, remote, newStubView(frontend.Projection{}), 0, "rA")
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	recovered := make(chan error, 1)
	closed := make(chan error, 1)
	var recoverStarted, closeStarted atomic.Bool
	join := func(name string, started *atomic.Bool, ch chan error) {
		if !started.Load() {
			return
		}
		select {
		case err := <-ch:
			ch <- err
		case <-time.After(10 * time.Second):
			t.Errorf("%s did not settle within 10s of cleanup", name)
		}
	}
	t.Cleanup(func() {
		remote.held.Store(false) // independent release, whatever Close did
		join("recovery", &recoverStarted, recovered)
		join("Close", &closeStarted, closed)
	})
	recoverStarted.Store(true)
	go func() {
		_, err := p.RecoverVolume(context.Background(), "v1")
		recovered <- err
	}()
	select {
	case <-remote.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery never reached the early HEAD read")
	}
	closeStarted.Store(true)
	go func() { closed <- p.Close() }()
	select {
	case err := <-recovered:
		recovered <- err
	case <-time.After(3 * time.Second):
		t.Fatal("recovery did not settle within 3s of Close (the read alone would wait its 5s bound) while its early HEAD read was held")
	}
	select {
	case err := <-closed:
		closed <- err
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5s of the cancelled early HEAD read")
	}
	if reason, _ := p.ChunkStartupStop("v1", 1); reason != "" {
		t.Fatalf("a cancelled early read recorded stop %q", reason)
	}
}

// V1-SPEC r2.38: a rolled-back copy of this root is caught before the
// assignment wait too (HEAD names this very root above its record), with
// its own reason and the operator's line; an unreadable root record stops
// with its own no-retry reason instead of looping on 75.
func TestEarlyStopForRolledBackCopyAndUnreadableRecord(t *testing.T) {
	t.Run("rolled-back copy", func(t *testing.T) {
		ctx := context.Background()
		remote := storage.NewMemChunkRemote()
		root := t.TempDir()
		a := chunkProviderAs(t, root, remote, viewAs("rA", 3), 0, "rA")
		s := openRecovered(t, a)
		writeSync(t, s, 1, 0x01)
		if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		backup := t.TempDir()
		copyRoot(t, root, backup)
		a = chunkProviderAs(t, root, remote, viewAs("rA", 3), 0, "rA")
		s = openRecovered(t, a)
		writeSync(t, s, 2, 0x02)
		if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := recoverUnassigned(t, remote, backup)
		gen, ended := p.ChunkStartupOutcome("v1")
		reason, detail := p.ChunkStartupStop("v1", gen)
		if !errors.Is(err, durable.ErrChunkStaleLocalRoot) || !ended || reason != "local_root_rolled_back" || !strings.Contains(detail, "rolled-back copy") || !strings.Contains(detail, "never from a copy of a root") {
			t.Fatalf("err=%v ended=%t reason=%q detail=%q, want an early local_root_rolled_back stop with the operator line", err, ended, reason, detail)
		}
	})
	t.Run("unreadable record", func(t *testing.T) {
		remote, root := servedRoot(t)
		ids, _ := filepath.Glob(filepath.Join(root, "*.root"))
		if len(ids) != 1 {
			t.Fatalf("identity files %v", ids)
		}
		if err := os.WriteFile(ids[0], []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		p, err := recoverUnassigned(t, remote, root)
		gen, ended := p.ChunkStartupOutcome("v1")
		reason, detail := p.ChunkStartupStop("v1", gen)
		if err == nil || !ended || reason != "local_root_record_unreadable" || !strings.Contains(detail, "unreadable") {
			t.Fatalf("err=%v ended=%t reason=%q detail=%q, want local_root_record_unreadable", err, ended, reason, detail)
		}
	})
}
