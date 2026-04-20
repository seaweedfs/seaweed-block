package authority

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ============================================================
// P14 S5 — Store + Bootstrap Unit Tests
//
// Covers proof matrix rows 5, 8, 9, 11, 12 and the store's own
// round-trip correctness. Rows 1-4, 6, 7, 10 are in
// authority_restart_test.go which exercises the full
// Publisher/Controller reload path.
// ============================================================

func newTempStore(t *testing.T) (*FileAuthorityStore, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatalf("NewFileAuthorityStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func sampleRecord(vid, rid string, epoch, ev, seq uint64) DurableRecord {
	return DurableRecord{
		VolumeID:        vid,
		ReplicaID:       rid,
		Epoch:           epoch,
		EndpointVersion: ev,
		DataAddr:        "data-" + rid,
		CtrlAddr:        "ctrl-" + rid,
		WriteSeq:        seq,
	}
}

func TestFileAuthorityStore_RoundTrip(t *testing.T) {
	s, _ := newTempStore(t)
	want := sampleRecord("v1", "r1", 5, 2, 7)
	if err := s.Put(want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if got[0] != want {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got[0], want)
	}
}

func TestFileAuthorityStore_LatestWinsPerVolumeID(t *testing.T) {
	s, _ := newTempStore(t)
	_ = s.Put(sampleRecord("v1", "r1", 1, 1, 1))
	_ = s.Put(sampleRecord("v1", "r2", 2, 1, 2)) // same volume, different replica
	recs, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("per-volume rule: expected 1 record for v1 after overwrite, got %d", len(recs))
	}
	if recs[0].ReplicaID != "r2" || recs[0].Epoch != 2 {
		t.Fatalf("expected latest write to win: got %+v", recs[0])
	}
}

// Row 5: Corrupt record fails closed for that volume only
func TestDurableAuthority_CorruptRecord_PerVolumeFailClosed(t *testing.T) {
	s, dir := newTempStore(t)
	// Write one good record.
	_ = s.Put(sampleRecord("v1", "r1", 5, 1, 1))

	// Write a second file with corrupt content under a different
	// "volume".
	badPath := filepath.Join(dir, "v2"+storeFileExtension)
	if err := os.WriteFile(badPath, []byte("not-json-at-all"), 0o644); err != nil {
		t.Fatalf("write bad file: %v", err)
	}

	recs, skips, err := s.LoadWithSkips()
	if err != nil {
		t.Fatalf("index-level failure wrongly reported: %v", err)
	}
	if len(recs) != 1 || recs[0].VolumeID != "v1" {
		t.Fatalf("good record v1 must load: got %+v", recs)
	}
	if len(skips) != 1 {
		t.Fatalf("expected exactly 1 skip, got %d (skips=%v)", len(skips), skips)
	}
	if !errors.Is(skips[0], ErrCorruptRecord) {
		t.Fatalf("skip must wrap ErrCorruptRecord, got %v", skips[0])
	}
}

// Row 8: Atomic rename survives mid-write crash
func TestDurableAuthority_AtomicWrite_NoTornRecord(t *testing.T) {
	s, dir := newTempStore(t)
	// Put a good record.
	_ = s.Put(sampleRecord("v1", "r1", 5, 1, 1))

	// Simulate a prior failed write leaving a tempfile behind.
	orphan := filepath.Join(dir, "v1"+storeFileExtension+tempSuffix)
	if err := os.WriteFile(orphan, []byte("garbage-from-prior-crash"), 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	// Load cleans up orphan tempfiles and returns only the real
	// record. No "torn record" is ever delivered to the caller.
	recs, skips, err := s.LoadWithSkips()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(recs) != 1 || recs[0].VolumeID != "v1" {
		t.Fatalf("expected 1 good record, got %+v", recs)
	}
	if len(skips) != 0 {
		t.Fatalf("orphan tempfiles must be cleaned silently, got skips=%v", skips)
	}
	if _, statErr := os.Stat(orphan); !os.IsNotExist(statErr) {
		t.Fatalf("orphan tempfile should be removed by Load, stat err: %v", statErr)
	}
}

// Row 9: Single-owner file lock prevents a second process-like
// acquisition on the same directory.
func TestDurableAuthority_ProcessLock_ExclusiveOwner(t *testing.T) {
	dir := t.TempDir()
	first, err := AcquireStoreLock(dir)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	defer first.Release()

	second, err := AcquireStoreLock(dir)
	if err == nil {
		second.Release()
		t.Fatal("second Acquire must fail while first is held")
	}
	if !errors.Is(err, ErrStoreLockHeld) {
		t.Fatalf("second Acquire: want ErrStoreLockHeld, got %v", err)
	}

	// Release first; a fresh acquire must now succeed.
	if err := first.Release(); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	third, err := AcquireStoreLock(dir)
	if err != nil {
		t.Fatalf("third Acquire after Release: %v", err)
	}
	if err := third.Release(); err != nil {
		t.Fatalf("third Release: %v", err)
	}
}

func TestDurableAuthority_ProcessLock_IdempotentRelease(t *testing.T) {
	dir := t.TempDir()
	l, err := AcquireStoreLock(dir)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("second Release must be idempotent, got %v", err)
	}
}

// TestDurableAuthority_ProcessLock_SurvivesStaleFileOnDisk is
// the regression for the architect round-4 finding that the
// old lock was a stale-lockfile scheme (O_EXCL + remove-on-close)
// instead of an OS advisory lock. With OS-level locks, a lock
// file left on disk from a crashed holder does NOT block the
// next acquirer — the OS dropped the lock when the crashed
// process's handle was closed, so a fresh open+lock succeeds.
//
// We simulate a crashed-holder situation by writing a lock file
// on disk with a stale PID BUT no held OS lock (because we
// never acquired one). A fresh AcquireStoreLock must succeed.
func TestDurableAuthority_ProcessLock_SurvivesStaleFileOnDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, LockFilename)

	// Simulate a stale lockfile left by a previous crashed
	// holder: the file exists on disk with a bogus PID, but NO
	// process currently holds an OS-level lock on it.
	if err := os.WriteFile(path, []byte("99999"), 0o644); err != nil {
		t.Fatalf("write stale lockfile: %v", err)
	}

	// A fresh acquire MUST succeed — the OS lock is
	// handle-bound, and no live handle holds it.
	l, err := AcquireStoreLock(dir)
	if err != nil {
		t.Fatalf("Acquire must succeed despite stale lockfile on disk; got %v", err)
	}
	defer l.Release()

	// And a second concurrent Acquire from within the same
	// process must now fail — the OS lock is actually held by
	// our handle.
	second, err := AcquireStoreLock(dir)
	if err == nil {
		second.Release()
		t.Fatal("second Acquire must fail while first holds OS lock")
	}
	if !errors.Is(err, ErrStoreLockHeld) {
		t.Fatalf("second Acquire: want ErrStoreLockHeld, got %v", err)
	}
}

// Row 12: Unreadable store index fails boot
func TestDurableAuthority_UnreadableIndex_BootFailsClosed(t *testing.T) {
	if os.Getenv("SKIP_PERMISSION_TESTS") != "" {
		t.Skip("skipped by SKIP_PERMISSION_TESTS")
	}
	s, _ := newTempStore(t)
	// Use a directory that does not exist by overwriting `dir`.
	// Easier: simulate by closing and pointing to non-listable
	// path. We construct a FileAuthorityStore with a
	// deliberately-bogus dir that cannot be enumerated by ReadDir
	// (non-existent).
	bogus := &FileAuthorityStore{dir: "/this/path/should/not/exist/xyz_9f8e7d6"}
	if _, _, err := bogus.LoadWithSkips(); err == nil {
		_ = s // quiet unused
		t.Fatal("expected index-level failure for unreadable directory")
	}
}

// Row 11: Wall-clock is NOT consulted for stale reload
func TestDurableAuthority_OldWallClock_StillReloaded(t *testing.T) {
	s, dir := newTempStore(t)
	rec := sampleRecord("v1", "r1", 5, 1, 1)
	if err := s.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Set the file's mtime to far in the past — e.g. 10 years ago.
	// A wall-clock-based stale check would reject this, but our
	// rule is WriteSeq + checksum, NOT wall-clock (sketch §8).
	path := filepath.Join(dir, "v1"+storeFileExtension)
	past := time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	recs, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(recs) != 1 || recs[0] != rec {
		t.Fatalf("old-mtime record must still load unchanged; got %+v", recs)
	}
}

// Row 6: Boundary guard — store files never construct a
// DurableRecord literal with a non-zero Epoch / EndpointVersion.
// The only way a non-zero value reaches a record is by
// passthrough from a Put() parameter. If a store file ever
// starts synthesizing authority values, this test fails.
func TestDurableAuthority_StoreNeverMintsAuthority_BoundaryGuard(t *testing.T) {
	files, err := listAuthorityStoreFiles(t)
	if err != nil {
		t.Fatalf("list store files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no authority_store_*.go files found")
	}

	fset := token.NewFileSet()
	for _, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || cl.Type == nil {
				return true
			}
			if !isDurableRecordType(cl.Type) {
				return true
			}
			// Check each field: if Epoch or EndpointVersion is
			// set to a non-zero literal or to a non-parameter
			// expression, flag it.
			for _, elt := range cl.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				if key.Name != "Epoch" && key.Name != "EndpointVersion" {
					continue
				}
				if isLiteralZero(kv.Value) {
					continue
				}
				// Any non-zero value in a DurableRecord literal
				// inside authority_store_* is suspect. The store
				// must never invent authority values.
				pos := fset.Position(kv.Pos())
				t.Errorf("%s: DurableRecord literal sets %s to non-zero in a store file — store must never mint authority",
					pos, key.Name)
			}
			return true
		})
	}
}

// Row 7: Boundary guard — only core/authority/ may call store
// write verbs. Walks the whole repo, flags any .Put() call on a
// parameter or field whose type is AuthorityStore outside the
// authority package.
//
// This is a heuristic test; it errs on the side of flagging
// suspicious call sites rather than proving global correctness.
func TestDurableAuthority_StoreWriteCallerAllowlist_BoundaryGuard(t *testing.T) {
	repoRoot, err := findRepoRootFromAuthority(t)
	if err != nil {
		t.Fatalf("find repo root: %v", err)
	}

	fset := token.NewFileSet()
	var bad []string
	err = filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			name := d.Name()
			if name == "vendor" || (len(name) > 0 && name[0] == '.') {
				if path != repoRoot {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		// The authority package itself is the only legitimate
		// write caller.
		if strings.Contains(path, string(filepath.Separator)+"core"+string(filepath.Separator)+"authority"+string(filepath.Separator)) {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		// Quick substring filter before parsing to keep the test
		// fast on large repos: skip files that don't mention
		// "AuthorityStore" or ".Put(" on a receiver named store.
		if !strings.Contains(string(src), "AuthorityStore") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			return nil
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if sel.Sel.Name != "Put" {
				return true
			}
			// We don't have a full type resolver here. Approximate:
			// flag the call if the file also declares an
			// AuthorityStore-typed identifier. This is
			// conservative — any file in the non-authority
			// production tree that names AuthorityStore AND calls
			// .Put is flagged for human review.
			bad = append(bad, fset.Position(call.Pos()).String())
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(bad) > 0 {
		t.Fatalf("AuthorityStore.Put called from outside core/authority/:\n  %s",
			strings.Join(bad, "\n  "))
	}
}

// ============================================================
// Helpers
// ============================================================

func listAuthorityStoreFiles(t *testing.T) ([]string, error) {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(cwd)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "authority_store") {
			continue
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		out = append(out, filepath.Join(cwd, name))
	}
	return out, nil
}

func isDurableRecordType(e ast.Expr) bool {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name == "DurableRecord"
	case *ast.SelectorExpr:
		return t.Sel.Name == "DurableRecord"
	}
	return false
}

func isLiteralZero(e ast.Expr) bool {
	if lit, ok := e.(*ast.BasicLit); ok {
		return lit.Kind == token.INT && lit.Value == "0"
	}
	return false
}

func findRepoRootFromAuthority(t *testing.T) (string, error) {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := cwd
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", errors.New("repo root not found")
}

// TestEncodeVolumeIDForFilename_Injective is the regression for
// the architect round-4 finding that the old encoding aliased
// distinct VolumeIDs onto the same filename (e.g. "/" and
// "_002f" both mapped to "_002f"). The new encoding escapes
// every non-[a-zA-Z0-9-] byte including literal '_', making it
// strictly injective.
func TestEncodeVolumeIDForFilename_Injective(t *testing.T) {
	// Pairs of DISTINCT VolumeIDs that must produce DISTINCT
	// encoded filenames.
	pairs := []struct {
		a, b string
	}{
		{"/", "_002f"},              // the architect's exact case
		{"_", "5f"},                 // literal underscore vs hex-looking string
		{"hello_world", "hello/world"},
		{"a_b_c", "a/b/c"},
		{"", "_"},                   // empty vs single underscore
		{"\x5f", "_"},               // same? these ARE the same byte, skip
	}
	for _, p := range pairs {
		if p.a == p.b {
			continue // skip equal-input cases
		}
		encA := encodeVolumeIDForFilename(p.a)
		encB := encodeVolumeIDForFilename(p.b)
		if encA == encB {
			t.Errorf("collision: %q and %q both encoded to %q", p.a, p.b, encA)
		}
	}

	// Explicit check against the architect's exact example.
	if got := encodeVolumeIDForFilename("/"); got == encodeVolumeIDForFilename("_002f") {
		t.Fatalf("architect regression: %q aliased to same encoded name: %q",
			"/ vs _002f", got)
	}

	// Spot-check the new encoding's literal output.
	cases := map[string]string{
		"v1":          "v1",
		"vol-1":       "vol-1",
		"/":           "_2f",
		"_":           "_5f",
		"_002f":       "_5f002f",
		"a/b":         "a_2fb",
		"abc123":      "abc123",
		"_X_":         "_5fX_5f",
	}
	for in, want := range cases {
		if got := encodeVolumeIDForFilename(in); got != want {
			t.Errorf("encode(%q): got %q want %q", in, got, want)
		}
	}
}

// TestEncodeVolumeIDForFilename_NoFilesystemUnsafeOutput is a
// guard that no encoded filename contains path separators or
// reserved characters.
func TestEncodeVolumeIDForFilename_NoFilesystemUnsafeOutput(t *testing.T) {
	cases := []string{
		"vol1", "vol/1", "vol\\1", "vol:1", "vol*1",
		"vol?1", "vol\"1", "vol<1", "vol>1", "vol|1",
		"vol 1", "vol\x001", "léön",
	}
	for _, vid := range cases {
		enc := encodeVolumeIDForFilename(vid)
		if strings.ContainsAny(enc, `/\:*?"<>| `+"\x00") {
			t.Errorf("encode(%q)=%q contains filesystem-unsafe character", vid, enc)
		}
	}
}

// Sanity: storeEnvelope encoding is stable JSON.
func TestFileAuthorityStore_EnvelopeShape(t *testing.T) {
	s, dir := newTempStore(t)
	rec := sampleRecord("v1", "r1", 3, 2, 4)
	if err := s.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "v1"+storeFileExtension))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var env storeEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Version != storeEnvelopeVersion {
		t.Fatalf("envelope version: got %d want %d", env.Version, storeEnvelopeVersion)
	}
	if env.Body != rec {
		t.Fatalf("envelope body: got %+v want %+v", env.Body, rec)
	}
	if env.Checksum == "" {
		t.Fatal("envelope checksum must be non-empty")
	}
}
