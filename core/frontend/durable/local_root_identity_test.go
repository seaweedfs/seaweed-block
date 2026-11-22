package durable_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// copyRoot copies a provider's storage root (files only) to dst.
func copyRoot(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A rolled-back copy of a holder's own local root (a restored backup, an
// older disk image) predates HEAD generations this holder published after
// the copy was taken. Attaching it would serve, and later publish over, an
// image older than HEAD under the same holder name. Attach must refuse it.
func TestWALStoreChunk_RolledBackCopyOfOwnRootIsRefused(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	root := t.TempDir()
	a := chunkProvider(t, root, remote, v1View(), 0)
	s := openRecovered(t, a)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	backup := t.TempDir()
	copyRoot(t, root, backup) // taken at HEAD gen N

	a = chunkProvider(t, root, remote, v1View(), 0)
	s = openRecovered(t, a)
	if _, err := s.Write(2, block(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	head, _ := remote.GetHead(ctx)

	b := chunkProvider(t, backup, remote, v1View(), 0) // same holder, the old copy
	if _, err := b.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RecoverVolume(ctx, "v1"); !errors.Is(err, durable.ErrChunkStaleLocalRoot) {
		t.Fatalf("a rolled-back copy of this holder's root attached under HEAD gen %d: RecoverVolume %v; want %v", head.Gen, err, durable.ErrChunkStaleLocalRoot)
	}
	if after, _ := remote.GetHead(ctx); after.Gen != head.Gen {
		t.Fatalf("HEAD moved from gen %d to %d under the refused copy", head.Gen, after.Gen)
	}
}

// casFaultRemote fails capture CASes on request: before reaching the remote
// (a crash after the high-water record, before the CAS), or after the remote
// applied it (a crash after the CAS, before the acknowledgement).
type casFaultRemote struct {
	storage.ChunkRemote
	mode   atomic.Value // "", "before" or "lost-ack"
	faults atomic.Int32 // CASes the fault mode answered
}

func (r *casFaultRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	switch r.mode.Load() {
	case "before":
		r.faults.Add(1)
		return 0, errors.New("test: the CAS failed before reaching the remote")
	case "lost-ack":
		r.faults.Add(1)
		if _, err := r.ChunkRemote.CASHead(ctx, expected, next); err != nil {
			return 0, err
		}
		return 0, errors.New("test: the CAS applied but its answer was lost")
	}
	return r.ChunkRemote.CASHead(ctx, expected, next)
}

// An orderly reopen (an error, then Close; not a power cut) after the root
// recorded a generation whose CAS then failed before reaching the remote
// (HEAD stays one below the record), or applied with its answer lost (HEAD
// at the record), is this root's own lineage: it attaches. The fault is
// witnessed at the CAS boundary and the record is checked exactly.
func TestWALStoreChunk_ReopenAfterAFailedOrUnansweredPublishCASAttaches(t *testing.T) {
	for _, mode := range []string{"before", "lost-ack"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			mem := storage.NewMemChunkRemote()
			remote := &casFaultRemote{ChunkRemote: mem}
			root := t.TempDir()
			a := chunkProvider(t, root, remote, v1View(), 0)
			s := openRecovered(t, a)
			if _, err := s.Write(1, block(1)); err != nil {
				t.Fatal(err)
			}
			if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(2, block(2)); err != nil {
				t.Fatal(err)
			}
			before, _ := mem.GetHead(ctx)
			remote.mode.Store(mode)
			// A CAS that failed before the remote is an error; one that
			// applied with its answer lost is settled by the exact reread.
			if _, err := a.CaptureVolume(ctx, "v1"); err == nil && mode == "before" {
				t.Fatal("a capture whose CAS never reached the remote reported success")
			}
			if remote.faults.Load() == 0 {
				t.Fatal("the capture never reached the faulted CAS boundary")
			}
			after, _ := mem.GetHead(ctx)
			_, hw, ep, ok := durable.LocalRootRecord(localRootVolume(t, root))
			if !ok || hw != before.Gen+1 || ep != before.Epoch {
				t.Fatalf("record before the faulted CAS: hw=%d epoch=%d ok=%t, want hw=%d epoch=%d", hw, ep, ok, before.Gen+1, before.Epoch)
			}
			switch mode {
			case "before":
				if after.Gen != before.Gen {
					t.Fatalf("a CAS that never reached the remote moved HEAD %d -> %d", before.Gen, after.Gen)
				}
			case "lost-ack":
				if after.Gen != hw {
					t.Fatalf("the applied CAS left HEAD at gen %d, want the recorded %d", after.Gen, hw)
				}
			}
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			remote.mode.Store("")
			b := chunkProvider(t, root, remote, v1View(), 0)
			if _, err := b.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := b.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatalf("the root's own reopen after a %s publish CAS was refused: %v", mode, err)
			}
		})
	}
}

// Two roots of one holder (same server and replica, each created fresh: two
// lineages) never adopt each other's HEAD: after the second root attached
// fresh from HEAD and published, the first root's restart is refused.
func TestWALStoreChunk_TwoRootsOfOneHolderDoNotAdoptEachOthersHead(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	root1, root2 := t.TempDir(), t.TempDir()
	a := chunkProvider(t, root1, remote, v1View(), 0)
	s := openRecovered(t, a)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b := chunkProvider(t, root2, remote, v1View(), 0)
	s2 := openRecovered(t, b) // a new root: attaches fresh from HEAD
	if _, err := s2.Write(2, block(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	head, _ := remote.GetHead(ctx)
	c := chunkProvider(t, root1, remote, v1View(), 0)
	if _, err := c.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RecoverVolume(ctx, "v1"); !errors.Is(err, durable.ErrChunkStaleLocalRoot) {
		t.Fatalf("the first root attached under HEAD gen %d published by the second root of the same holder: %v; want %v", head.Gen, err, durable.ErrChunkStaleLocalRoot)
	}
}

// Upgrade (V1-SPEC r2.38): a pre-release root (no identity file)
// under a pre-release HEAD (no HolderRoot) of the same holder is adopted once
// and serves; the claim then stamps this root's nonce. A copy of the same
// pre-release root taken before that conversion is refused afterwards: its
// nonce-less form no longer matches the converted HEAD.
func TestWALStoreChunk_PreReleaseRootAdoptedOnceThenACopyIsRefused(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	root := t.TempDir()
	a := chunkProvider(t, root, remote, v1View(), 0)
	s := openRecovered(t, a)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	// Make it pre-release: no identity file, and a HEAD without HolderRoot.
	ids, _ := filepath.Glob(filepath.Join(root, "*.root"))
	if len(ids) == 0 {
		t.Fatal("no local root identity file was written")
	}
	for _, f := range ids {
		if err := os.Remove(f); err != nil {
			t.Fatal(err)
		}
	}
	head, _ := remote.GetHead(ctx)
	legacy := head
	legacy.HolderRoot = ""
	if legacy.DetachedBy != nil {
		d := *legacy.DetachedBy
		d.HolderRoot = ""
		legacy.DetachedBy = &d
	}
	if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{Gen: head.Gen, Epoch: head.Epoch}, legacy); err != nil {
		t.Fatal(err)
	}
	oldCopy := t.TempDir()
	copyRoot(t, root, oldCopy)

	b := chunkProvider(t, root, remote, v1View(), 0)
	if _, err := b.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("a pre-release root under its own pre-release HEAD was refused: %v", err)
	}
	if after, _ := remote.GetHead(ctx); after.HolderRoot == "" {
		t.Fatalf("the adopting claim did not stamp the root's nonce: HEAD %+v", after)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	c := chunkProvider(t, oldCopy, remote, v1View(), 0)
	if _, err := c.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RecoverVolume(ctx, "v1"); !errors.Is(err, durable.ErrChunkStaleLocalRoot) {
		t.Fatalf("a copy of the pre-release root attached after the conversion: %v; want %v", err, durable.ErrChunkStaleLocalRoot)
	}
}

// The case only the root check decides: the first root recorded generation
// G+1 and crashed before its CAS; a second, fresh root of the same holder
// then claimed and installed exactly G+1. HEAD is not above the first root's
// high-water, but it names another root: the first root is refused.
func TestWALStoreChunk_AnotherRootAtTheRecordedGenerationIsRefused(t *testing.T) {
	ctx := context.Background()
	mem := storage.NewMemChunkRemote()
	remote := &casFaultRemote{ChunkRemote: mem}
	root1, root2 := t.TempDir(), t.TempDir()
	a := chunkProvider(t, root1, remote, v1View(), 0)
	s := openRecovered(t, a)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(2, block(2)); err != nil {
		t.Fatal(err)
	}
	remote.mode.Store("before")
	_, _ = a.CaptureVolume(ctx, "v1") // records G+1, no CAS
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	remote.mode.Store("")
	before, _ := mem.GetHead(ctx)
	b := chunkProvider(t, root2, remote, v1View(), 0)
	_ = openRecovered(t, b) // fresh: claims, installing G+1
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	head, _ := mem.GetHead(ctx)
	if head.Gen != before.Gen+1 {
		t.Fatalf("setup: HEAD gen %d after the second root's claim, want %d", head.Gen, before.Gen+1)
	}
	c := chunkProvider(t, root1, remote, v1View(), 0)
	if _, err := c.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RecoverVolume(ctx, "v1"); !errors.Is(err, durable.ErrChunkStaleLocalRoot) {
		t.Fatalf("the first root attached under another root's HEAD at its own recorded generation: %v; want %v", err, durable.ErrChunkStaleLocalRoot)
	}
}

// INV-WS-LOCAL-ROOT-IDENTITY-001, the record barrier: the record's rename
// succeeds but its directory sync fails, so no CAS runs. After a reopen the
// record is visible but not proven durable: the retry of the same generation
// must run the barrier again (file and directory) before its CAS, and the
// CAS then runs. A visible record never stands in for a failed barrier.
func TestLocalRootRecordBarrierRunsAgainAfterReopen(t *testing.T) {
	ctx := context.Background()
	mem := storage.NewMemChunkRemote()
	remote := &casCountRemote{ChunkRemote: mem}
	root := t.TempDir()
	a := chunkProvider(t, root, remote, v1View(), 0)
	s := openRecovered(t, a)
	writeSync(t, s, 1, 0x01)
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	writeSync(t, s, 2, 0x02)
	var barriers atomic.Int32
	failing := atomic.Bool{}
	failing.Store(true)
	restore := durable.SetLocalRootSyncDirForTest(func(string) error {
		barriers.Add(1)
		if failing.Load() {
			return errors.New("test: directory sync failed")
		}
		return nil
	})
	t.Cleanup(restore)
	before, _ := mem.GetHead(ctx)
	casBefore := remote.cas.Load()
	if _, err := a.CaptureVolume(ctx, "v1"); err == nil {
		t.Fatal("a capture whose record barrier failed published")
	}
	if remote.cas.Load() != casBefore {
		t.Fatal("a CAS ran after its record barrier failed")
	}
	if after, _ := mem.GetHead(ctx); after.Gen != before.Gen {
		t.Fatalf("HEAD moved from gen %d to %d without a durable record", before.Gen, after.Gen)
	}
	vol := localRootVolume(t, root)
	if _, hw, _, ok := durable.LocalRootRecord(vol); !ok || hw != before.Gen+1 {
		t.Fatalf("the visible record after the failed barrier: hw=%d ok=%t, want %d", hw, ok, before.Gen+1)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the record of G+1 is visible; the barrier must run again.
	// Its claim proposes exactly that generation again.
	failing.Store(false)
	n := barriers.Load()
	casN := remote.cas.Load()
	b := chunkProvider(t, root, remote, v1View(), 0)
	sb := openRecovered(t, b)
	if barriers.Load() == n {
		t.Fatal("the reopened root's claim retried the recorded generation without running its record barrier again")
	}
	if remote.cas.Load() == casN {
		t.Fatal("the reopened root's claim sent no CAS after its barrier succeeded")
	}
	writeSync(t, sb, 3, 0x03)
	if _, err := b.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if after, _ := mem.GetHead(ctx); after.Gen <= before.Gen {
		t.Fatalf("no publication after the repaired barrier: HEAD gen %d", after.Gen)
	}
}

// casCountRemote counts HEAD CAS calls that reach the remote.
type casCountRemote struct {
	storage.ChunkRemote
	cas atomic.Int32
}

func (r *casCountRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	r.cas.Add(1)
	return r.ChunkRemote.CASHead(ctx, expected, next)
}

// localRootVolume is the volume path whose <volume>.root lives in root.
func localRootVolume(t *testing.T, root string) string {
	t.Helper()
	ids, _ := filepath.Glob(filepath.Join(root, "*.root"))
	if len(ids) != 1 {
		t.Fatalf("want one local root identity in %s, have %v", root, ids)
	}
	return strings.TrimSuffix(ids[0], ".root")
}
