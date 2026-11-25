package durable_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
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

// The durable-reopen rule: an existing root whose HEAD names this holder attaches
// only if its local durable LSN reaches HEAD's remote_lsn. A rolled-back
// copy of the root (a backup taken before the last cut) is refused with the
// stale-root refusal, as a no-retry (superseded) startup outcome, with the
// operator's cure; it makes no claim, never serves and is left as it was. An
// exact copy of the root at HEAD, and the root's own restart, attach.
func TestRolledBackCopyBelowHeadLSNIsRefused(t *testing.T) {
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	root := t.TempDir()
	a := layoutChunkProvider(t, root, fixture, v1View(), 0)
	s := openRecovered(t, a)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	copyLSN, _, _ := s.(*storage.WALStore).Boundaries()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	backup := t.TempDir()
	copyRootFiles(t, root, backup) // durable LSN at the first cut

	a = layoutChunkProvider(t, root, fixture, v1View(), 0)
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
	head := layoutHead(t, fixture)
	exact := t.TempDir()
	copyRootFiles(t, root, exact) // durable LSN at HEAD
	puts := fixture.Blobs.Puts()

	b := layoutChunkProvider(t, backup, fixture, v1View(), 0)
	bs, err := b.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.RecoverVolume(ctx, "v1")
	if !errors.Is(err, layout.ErrStaleLocalRoot) {
		t.Fatalf("a rolled-back copy attached under HEAD position %d: %v; want the stale-root refusal", head.Position, err)
	}
	gen, ended := b.ChunkStartupOutcome("v1")
	if !ended || !b.ChunkStartupSuperseded("v1", gen) {
		t.Errorf("the rolled-back copy's startup: ended=%t superseded=%t, want a no-retry outcome", ended, b.ChunkStartupSuperseded("v1", gen))
	}
	if cure := b.ChunkStartupCure("v1", gen); cure != layout.StaleLocalRootCure {
		t.Errorf("the startup end carries cure %q, want %q for the final exit line", cure, layout.StaleLocalRootCure)
	}
	if after := layoutHead(t, fixture); after.Gen != head.Gen || after.Epoch != head.Epoch || after.Holder != head.Holder || fixture.Blobs.Puts() != puts {
		t.Fatalf("the refused copy claimed or wrote: HEAD gen %d -> %d, PUTs %d -> %d", head.Gen, after.Gen, puts, fixture.Blobs.Puts())
	}
	if err := b.Backend("v1").AdmitsIO(); err == nil {
		t.Fatal("the refused copy admits I/O")
	}
	// The refused root is left as it was: its data and durable LSN are those
	// of the copy (no seed, replay or rewrite toward HEAD).
	ws := bs.(*storage.WALStore)
	if r, _, _ := ws.Boundaries(); r != copyLSN {
		t.Fatalf("the refused root's durable LSN moved: %d -> %d", copyLSN, r)
	}
	for lba, want := range map[uint32][]byte{1: block(1), 2: make([]byte, 4096)} {
		got, err := ws.Read(lba)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("the refused root's block %d changed: %v", lba, err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	x := layoutChunkProvider(t, exact, fixture, v1View(), 0)
	if _, err := x.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := x.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("an exact copy at HEAD position %d was refused: %v", head.Position, err)
	}
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}

	c := layoutChunkProvider(t, root, fixture, v1View(), 0)
	if _, err := c.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("the root's own restart after its last cut was refused: %v", err)
	}

}
