package durable_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// copyRootFiles copies a provider's storage root (files only) to dst.
func copyRootFiles(t *testing.T, src, dst string) {
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

// V1-SPEC r2.45 (2): an existing root whose HEAD names this holder attaches
// only if its local durable LSN reaches HEAD's remote_lsn. A rolled-back
// copy of the root (a backup taken before the last cut) is refused with the
// stale-root refusal, as a no-retry (superseded) startup outcome, with the
// operator's cure; the root's own restart is accepted.
func TestRolledBackCopyBelowHeadLSNIsRefused(t *testing.T) {
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
	copyRootFiles(t, root, backup) // durable LSN at the first cut

	a = chunkProvider(t, root, remote, v1View(), 0)
	s = openRecovered(t, a) // the root's own restart
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

	b := chunkProvider(t, backup, remote, v1View(), 0)
	if _, err := b.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	_, err := b.RecoverVolume(ctx, "v1")
	if !errors.Is(err, durable.ErrChunkStaleLocalRoot) || !strings.Contains(err.Error(), "remove or quarantine the local root") {
		t.Fatalf("a rolled-back copy attached under HEAD remote_lsn %d: %v; want the stale-root refusal", head.RemoteLSN, err)
	}
	gen, ended := b.ChunkStartupOutcome("v1")
	if !ended || !b.ChunkStartupSuperseded("v1", gen) {
		t.Fatalf("the rolled-back copy's startup: ended=%t superseded=%t, want a no-retry outcome", ended, b.ChunkStartupSuperseded("v1", gen))
	}
	if after, _ := remote.GetHead(ctx); after.Gen != head.Gen {
		t.Fatalf("HEAD moved under the refused copy: gen %d -> %d", head.Gen, after.Gen)
	}

	c := chunkProvider(t, root, remote, v1View(), 0)
	if _, err := c.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("the root's own restart after its last cut was refused: %v", err)
	}
}
