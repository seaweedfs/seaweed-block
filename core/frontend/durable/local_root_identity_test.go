package durable_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
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
	mode atomic.Value // "", "before" or "lost-ack"
}

func (r *casFaultRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	switch r.mode.Load() {
	case "before":
		return 0, errors.New("test: crashed before the CAS")
	case "lost-ack":
		if _, err := r.ChunkRemote.CASHead(ctx, expected, next); err != nil {
			return 0, err
		}
		return 0, errors.New("test: crashed after the CAS, before its acknowledgement")
	}
	return r.ChunkRemote.CASHead(ctx, expected, next)
}

// A root that crashed between recording a generation and its CAS (HEAD stays
// below the root's record), or after the CAS but before its acknowledgement
// (HEAD at the recorded generation), is this root's own lineage: it attaches.
func TestWALStoreChunk_CrashAroundThePublishCASStillAttaches(t *testing.T) {
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
			remote.mode.Store(mode)
			_, _ = a.CaptureVolume(ctx, "v1") // the crash point
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			remote.mode.Store("")
			b := chunkProvider(t, root, remote, v1View(), 0)
			if _, err := b.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := b.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatalf("the root's own restart after a crash %s its publish CAS was refused: %v", mode, err)
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
