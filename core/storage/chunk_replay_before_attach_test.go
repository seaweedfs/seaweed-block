package storage

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"time"
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
	// Attach released the hold; the drain now runs and advances the
	// checkpoint, and it marked the block local: a fetch of its chunk (a
	// read of LBA 10, still absent) does not install over it.
	if s2.ExtentDrainHeld() {
		t.Fatal("attach did not release the hold")
	}
	before := s2.CheckpointLSN()
	if err := s2.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if cp := s2.CheckpointLSN(); cp <= before {
		t.Fatalf("the drain after attach did not advance the checkpoint (%d -> %d)", before, cp)
	}
	if _, err := s2.Read(10); err != nil { // fetches chunk 2
		t.Fatal(err)
	}
	if got, err := s2.Read(9); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("after the drain and a fetch of its chunk, LBA 9 = %#x (err %v); want the synced write %#x", got[0], err, newer[0])
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

// extentWriterSites counts every reference to the extent writer (a call,
// a method value such as w := s.writeExtentUnlocked, a method expression),
// keyed by its receiver-qualified owner: "(*WALStore).installFetchedBlock",
// a plain function name, or "<package level>" for a declaration
// initializer (var w = (*WALStore).writeExtentUnlocked, or a closure).
func extentWriterSites(files []*ast.File) map[string]int {
	sites := map[string]int{}
	count := func(owner string, root ast.Node) {
		ast.Inspect(root, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "writeExtentUnlocked" {
				sites[owner]++
			}
			return true
		})
	}
	for _, file := range files {
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body != nil {
					count(funcOwner(d), d.Body)
				}
			case *ast.GenDecl:
				count("<package level>", d)
			}
		}
	}
	return sites
}

// funcOwner is a function's receiver-qualified name.
func funcOwner(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	t := fn.Recv.List[0].Type
	star := ""
	if p, ok := t.(*ast.StarExpr); ok {
		star, t = "*", p.X
	}
	if ix, ok := t.(*ast.IndexExpr); ok {
		t = ix.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return "(" + star + id.Name + ")." + fn.Name.Name
	}
	return fn.Name.Name
}

func TestExtentWritersAreEnumerated(t *testing.T) {
	want := map[string]int{
		"(*WALStore).writeExtentIfCurrent": 1, // the flusher's drain; held until attach (flushOnceInternal)
		"(*installHandleV3).WriteBase":     1, // BASE install; refused while held (BeginBaseInstall)
		"(*WALStore).installFetchedBlock":  1, // a fetched block; only an attached chunk layer fetches
	}
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			files = append(files, f)
		}
	}
	got := extentWriterSites(files)
	for name, n := range got {
		if want[name] != n {
			t.Errorf("%s references the extent writer %d time(s), listed %d: classify each site (held, refused or attached-only)", name, n, want[name])
		}
	}
	for name, n := range want {
		if got[name] == 0 {
			t.Errorf("listed extent writer %s (%d site) no longer references writeExtentUnlocked; update the list", name, n)
		}
	}

	// Negative controls: an alias, a second reference, a method expression,
	// package-level initializers (a method-expression alias and a closure),
	// and a same-named method on another receiver are each counted under
	// their own owner.
	src := `package storage
var pkgAlias = (*WALStore).writeExtentUnlocked
var pkgClosure = func(s *WALStore) error { return s.writeExtentUnlocked(0, nil) }
func (s *WALStore) aliasWriter() { w := s.writeExtentUnlocked; _ = w(0, nil) }
func (s *WALStore) twoWrites() { _ = s.writeExtentUnlocked(0, nil); _ = s.writeExtentUnlocked(1, nil) }
func exprWriter() { _ = (*WALStore).writeExtentUnlocked }
func (o *other) installFetchedBlock(s *WALStore) { _ = s.writeExtentUnlocked(0, nil) }
`
	f, err := parser.ParseFile(token.NewFileSet(), "control.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	c := extentWriterSites([]*ast.File{f})
	if c["<package level>"] != 2 || c["(*WALStore).aliasWriter"] != 1 || c["(*WALStore).twoWrites"] != 2 || c["exprWriter"] != 1 ||
		c["(*other).installFetchedBlock"] != 1 || c["(*WALStore).installFetchedBlock"] != 0 {
		t.Fatalf("the census missed or misattributed a control site: %v", c)
	}
}

// The structural backstop: a store awaiting its chunk layer refuses any
// extent write, so a writer missed by the enumeration fails loudly; once a
// layer is attached the same write is admitted.
func TestAHeldStoreRefusesAnyExtentWriteUntilAttached(t *testing.T) {
	s, _, _ := crashableStore(t)
	s.HoldExtentDrainUntilChunkLayer()
	if err := s.writeExtentUnlocked(0, makeBlock(ctBlockSize, 0x11)); !errors.Is(err, ErrDrainHeldForChunkLayer) {
		t.Fatalf("extent write while held: %v; want ErrDrainHeldForChunkLayer", err)
	}
	mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
	if s.ExtentDrainHeld() {
		t.Fatal("attach did not release the hold")
	}
	if err := s.writeExtentUnlocked(0, makeBlock(ctBlockSize, 0x11)); err != nil {
		t.Fatalf("extent write after attach: %v", err)
	}
}

// A flusher cycle that began before the owner armed the hold (the flusher
// starts at open) and pauses after its entry checks must not drain the
// dirty map Recover fills after the hold: the hold is checked with the
// snapshot, under store.mu, which Recover takes.
func TestACycleStartedBeforeTheHoldDoesNotDrainTheRecoveredWrite(t *testing.T) {
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

	s2, err := OpenWALStore(path) // the flusher is running from here
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeBounded(t, s2) })
	s2.DisableAutoFlushForRecoveryTest() // its ticks are replaced by the cycle below
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	s2.flusher.beforeSnapshotForTest = func() { enterOnce.Do(func() { close(entered) }); <-release }
	done := make(chan error, 1)
	settled := false
	// On any failure below: release the paused cycle and join it (bounded)
	// before the store's own cleanup, so no goroutine stays parked.
	t.Cleanup(func() {
		unblock()
		if !settled {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("the paused cycle did not finish after release")
			}
		}
		s2.flusher.beforeSnapshotForTest = nil
	})
	go func() { done <- s2.flusher.flushOnce() }() // a cycle that saw no hold
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the cycle never reached its snapshot")
	}
	s2.HoldExtentDrainUntilChunkLayer()
	before := s2.CheckpointLSN()
	if _, err := s2.Recover(); err != nil {
		t.Fatalf("local recovery: %v", err)
	}
	unblock()
	select {
	case err := <-done:
		settled = true
		if err != nil {
			t.Fatalf("the paused cycle: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the paused cycle did not finish after release")
	}
	s2.flusher.beforeSnapshotForTest = nil
	if cp := s2.CheckpointLSN(); cp != before {
		t.Fatalf("the paused cycle advanced the checkpoint %d -> %d before attach", before, cp)
	}
	mustAttach(t, s2, f, cfg)
	if got, err := s2.Read(9); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("after attach LBA 9 = %#x (err %v); want the synced write %#x", got[0], err, newer[0])
	}
}
