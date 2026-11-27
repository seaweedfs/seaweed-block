package storage

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"
)

// A synced guest write into an absent chunk, then a crash: on reopen the
// replayed write must not reach the extent before the chunk layer attaches,
// or it would not be marked local and the next fetch of its chunk would
// install the snapshot's block over it. The store's owner holds the drain
// (as the durable provider does, before Recover); a flush before attach
// drains nothing, and after attach the block reads back as written and is
// drained as local.
func TestReplayedWriteIsNotDrainedBeforeTheChunkLayerAttaches(t *testing.T) {
	s, path, crash := crashableStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	layer := mustAttach(t, s, f, cfg)
	for lba := uint32(8); lba < 12; lba++ {
		if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(0x80+lba))); err != nil {
			t.Fatal(err)
		}
	}
	mustSyncFlush(t, s)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if err := layer.EvictChunk(2); err != nil {
		t.Fatalf("evict: %v", err)
	}
	newer := makeBlock(ctBlockSize, 0xE9)
	if _, err := s.Write(9, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crash()

	// Reopen as the provider does: hold the drain, then recover.
	s2, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeBounded(t, s2) })
	s2.DisableAutoFlushForRecoveryTest()
	s2.HoldExtentDrainUntilChunkLayer()
	if _, err := s2.Recover(); err != nil {
		t.Fatalf("local recovery: %v", err)
	}
	if err := s2.flusher.flushOnce(); err != nil { // a flusher tick before attach
		t.Fatal(err)
	}
	mustAttach(t, s2, f, cfg)
	if got, err := s2.Read(9); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("after restart LBA 9 = %#x (err %v); want the synced write %#x", got[0], err, newer[0])
	}
	// After attach the drain runs, marks the block local, and a later read
	// still returns the write (no fetch installs over it).
	if err := s2.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got, err := s2.Read(9); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("after the drain LBA 9 = %#x (err %v); want the synced write %#x", got[0], err, newer[0])
	}
}

// The final flush of a stop is held too: a clean Close before the chunk
// layer attaches leaves the replayed write in the WAL, so the next open
// replays it again and it reads back after attach.
func TestAStopBeforeAttachDrainsNothing(t *testing.T) {
	s, path, crash := crashableStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	layer := mustAttach(t, s, f, cfg)
	if _, err := s.Write(9, makeBlock(ctBlockSize, 0x89)); err != nil {
		t.Fatal(err)
	}
	mustSyncFlush(t, s)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if err := layer.EvictChunk(2); err != nil {
		t.Fatal(err)
	}
	newer := makeBlock(ctBlockSize, 0xE9)
	if _, err := s.Write(9, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crash()

	reopenHeld := func() *WALStore {
		t.Helper()
		r, err := OpenWALStore(path)
		if err != nil {
			t.Fatal(err)
		}
		r.HoldExtentDrainUntilChunkLayer()
		if _, err := r.Recover(); err != nil {
			t.Fatal(err)
		}
		return r
	}
	first := reopenHeld()
	if err := first.Close(); err != nil { // the flusher's stop flush runs here
		t.Fatal(err)
	}
	second := reopenHeld()
	t.Cleanup(func() { closeBounded(t, second) })
	second.DisableAutoFlushForRecoveryTest()
	mustAttach(t, second, f, cfg)
	if got, err := second.Read(9); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("after a stop before attach and a reopen, LBA 9 = %#x (err %v); want %#x", got[0], err, newer[0])
	}
}

// A store whose drain is held takes no BASE (a BASE writes the extent).
func TestABaseInstallIsRefusedWhileTheDrainIsHeld(t *testing.T) {
	s, _, _ := crashableStore(t)
	s.HoldExtentDrainUntilChunkLayer()
	if _, err := s.BeginBaseInstall(1, [16]byte{1}); !errors.Is(err, ErrDrainHeldForChunkLayer) {
		t.Fatalf("BeginBaseInstall while held: %v; want ErrDrainHeldForChunkLayer", err)
	}
}

// Every path that writes the extent is listed with how it keeps a block
// written before the chunk layer attaches local: the flusher's drain (held
// until attach), a BASE (refused while held) and a fetched block (only an
// attached layer fetches). A new writer fails here until it is classified.
func TestExtentWritersAreEnumerated(t *testing.T) {
	want := map[string]string{
		"writeExtentIfCurrent": "the flusher's drain; held until attach (flushOnceInternal)",
		"WriteBase":            "BASE install; refused while held (BeginBaseInstall)",
		"installFetchedBlock":  "a fetched block; only an attached chunk layer fetches",
	}
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "writeExtentUnlocked" {
							got[fn.Name.Name] = true
						}
					}
					return true
				})
			}
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s writes the extent and is not classified: a block it writes before the chunk layer attaches must be held or marked local", name)
		}
	}
	for name, why := range want {
		if !got[name] {
			t.Errorf("listed extent writer %s (%s) no longer calls writeExtentUnlocked; update the list", name, why)
		}
	}
}
