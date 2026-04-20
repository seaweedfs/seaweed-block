package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// ============================================================
// P14 S5 — Live Bootstrap Tests (cmd/sparrow)
//
// Architect round-5 ask: the durable authority route must be
// exercised through a real non-test caller, and LoadErrors()
// must surface as startup logging. These tests drive the
// Bootstrap helper in cmd/sparrow/bootstrap.go directly (same
// entry point runS5Bootstrap uses) and verify:
//
//   - lock is acquired on fresh dir
//   - reloaded records populate Publisher state
//   - LoadErrors is logged at startup (not stranded)
//   - lock refuses a second concurrent bootstrap on same dir
//   - Close() releases lock and store cleanly
// ============================================================

// captureLog redirects the default logger to a byte buffer for
// the duration of the test. Returns the buffer (snapshot-on-call)
// and a restore func.
func captureLog(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	prev := log.Writer()
	var buf bytes.Buffer
	var mu sync.Mutex
	log.SetOutput(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}))
	return &buf, func() { log.SetOutput(prev) }
}

type writerFunc func(p []byte) (int, error)

func (w writerFunc) Write(p []byte) (int, error) { return w(p) }

func TestBootstrap_FreshDir_AcquiresLockAndReloads(t *testing.T) {
	dir := t.TempDir()

	logBuf, restore := captureLog(t)
	defer restore()

	directive := authority.NewStaticDirective(nil)
	boot, err := Bootstrap(dir, directive)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	defer boot.Close()

	// Lock was acquired — lockfile must exist on disk.
	if _, err := os.Stat(filepath.Join(dir, authority.LockFilename)); err != nil {
		t.Fatalf("lock file not present: %v", err)
	}

	// No prior records, so reload count is 0 and no skips.
	if boot.ReloadedRecords != 0 {
		t.Fatalf("fresh dir reload count: got %d want 0", boot.ReloadedRecords)
	}
	if len(boot.ReloadSkips) != 0 {
		t.Fatalf("fresh dir must have no skips, got %v", boot.ReloadSkips)
	}

	// Startup logs must include the lock-acquired line and the
	// reload-count line. These are the surfaces the architect
	// asked for: real startup visibility of the durable route.
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "durable authority lock acquired") {
		t.Fatalf("startup log missing lock-acquired line:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "durable authority reload: 0 records") {
		t.Fatalf("startup log missing reload-count line:\n%s", logOutput)
	}
}

func TestBootstrap_WithExistingRecord_ReloadsPublisherState(t *testing.T) {
	dir := t.TempDir()

	// Simulate a prior run via the real publisher-run path:
	// construct a Publisher with a Directive that emits one
	// Bind, run briefly to let the write-through land, then
	// tear down cleanly. This keeps the S5 boundary intact —
	// the test does NOT call store.Put directly (only
	// core/authority/ production code is allowed to do that).
	seedDurableRecord(t, dir, authority.AssignmentAsk{
		VolumeID: "v-reload", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: authority.IntentBind,
	})

	// Now the real bootstrap.
	logBuf, restore := captureLog(t)
	defer restore()

	boot, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	defer boot.Close()

	if boot.ReloadedRecords != 1 {
		t.Fatalf("reload count: got %d want 1", boot.ReloadedRecords)
	}

	basis, ok := boot.Publisher.LastAuthorityBasis("v-reload", "r1")
	if !ok {
		t.Fatal("Publisher must have reloaded r1 into state")
	}
	if basis.Epoch != 1 {
		t.Fatalf("reloaded Epoch: got %d want 1", basis.Epoch)
	}

	if !strings.Contains(logBuf.String(), "durable authority reload: 1 records") {
		t.Fatalf("startup log missing reload=1 line:\n%s", logBuf.String())
	}
}

func TestBootstrap_CorruptRecord_LoggedAtStartup(t *testing.T) {
	dir := t.TempDir()

	// Seed one good record via the publisher-run path.
	seedDurableRecord(t, dir, authority.AssignmentAsk{
		VolumeID: "v-good", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: authority.IntentBind,
	})

	// Now drop a corrupt file next to it. The filename uses the
	// expected extension so Load will try to parse it.
	badPath := filepath.Join(dir, "v-bad.v3auth.json")
	if err := os.WriteFile(badPath, []byte("{not valid}"), 0o644); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}

	// Bootstrap MUST succeed despite the corrupt record (per-volume
	// fail-closed). The skip MUST be logged at startup so operators
	// see it, not stranded inside Publisher.LoadErrors.
	logBuf, restore := captureLog(t)
	defer restore()

	boot, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("Bootstrap must survive corrupt record: %v", err)
	}
	defer boot.Close()

	if boot.ReloadedRecords != 1 {
		t.Fatalf("good record must still reload: got %d", boot.ReloadedRecords)
	}
	if len(boot.ReloadSkips) == 0 {
		t.Fatal("corrupt record must produce a reload skip")
	}

	out := logBuf.String()
	if !strings.Contains(out, "reload skip") {
		t.Fatalf("startup log missing reload-skip line:\n%s", out)
	}
}

func TestBootstrap_LockContention_RefusesSecondBootstrap(t *testing.T) {
	dir := t.TempDir()

	first, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("first Bootstrap: %v", err)
	}
	defer first.Close()

	second, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err == nil {
		second.Close()
		t.Fatal("second Bootstrap must fail while first holds the lock")
	}
	if !errors.Is(err, authority.ErrStoreLockHeld) {
		t.Fatalf("second Bootstrap: want ErrStoreLockHeld, got %v", err)
	}

	// After first.Close(), a third bootstrap must succeed.
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	third, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("third Bootstrap after first Close: %v", err)
	}
	third.Close()
}

func TestBootstrap_EmptyStoreDir_Errors(t *testing.T) {
	_, err := Bootstrap("", authority.NewStaticDirective(nil))
	if err == nil {
		t.Fatal("Bootstrap with empty dir must fail")
	}
}

// seedDurableRecord simulates "a prior sparrow run persisted an
// assignment, then exited cleanly". Uses the real Publisher+
// Directive path so the store is written by core/authority/ —
// respecting the S5 boundary that only the authority package
// may call store write verbs.
//
// Blocks until the record is visible in the store or the
// deadline elapses.
func seedDurableRecord(t *testing.T, dir string, ask authority.AssignmentAsk) {
	t.Helper()
	lock, err := authority.AcquireStoreLock(dir)
	if err != nil {
		t.Fatalf("seed acquire lock: %v", err)
	}
	store, err := authority.NewFileAuthorityStore(dir)
	if err != nil {
		_ = lock.Release()
		t.Fatalf("seed store: %v", err)
	}
	directive := authority.NewStaticDirective([]authority.AssignmentAsk{ask})
	pub := authority.NewPublisher(directive, authority.WithStore(store))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = pub.Run(ctx) }()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		recs, err := store.Load()
		if err == nil && len(recs) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := store.Close(); err != nil {
		t.Fatalf("seed close store: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("seed release lock: %v", err)
	}
}

// TestParseFlags_AuthorityStoreRequiresS5Bootstrap is the
// regression for architect round-8 finding #1: the default
// validation path is not restart-clean, so --authority-store
// must only be accepted when paired with --s5-bootstrap. The
// flag parser rejects the combination at parse time.
func TestParseFlags_AuthorityStoreRequiresS5Bootstrap(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{
			name:    "store-without-bootstrap rejected",
			args:    []string{"--authority-store", "/tmp/sparrow"},
			wantErr: true,
		},
		{
			name:    "bootstrap-without-store rejected",
			args:    []string{"--s5-bootstrap"},
			wantErr: true,
		},
		{
			name:    "both-together accepted",
			args:    []string{"--authority-store", "/tmp/sparrow", "--s5-bootstrap"},
			wantErr: false,
		},
		{
			name:    "neither accepted (default mode)",
			args:    []string{},
			wantErr: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseFlags(c.args)
			if c.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestS5Bootstrap_RepeatedRunsAreRestartClean is the regression
// for architect round-8 finding #3: the test must actually
// exercise repeated runs, check exit codes, and prove the
// behavior it claims. Here we run runS5Bootstrap twice against
// the same directory with a durable record seeded in between
// (via the real publisher path). Both runs must exit 0, and the
// second run must report the reloaded count.
//
// Without the pre-check in Bootstrap (fix #2), the second run
// would still have worked because the index is readable; the
// value of this test is primarily the repeated-run integrity
// assertion: no stale lock, no silent failure.
func TestS5Bootstrap_RepeatedRunsAreRestartClean(t *testing.T) {
	dir := t.TempDir()

	stdout := os.Stdout
	devNull, _ := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if devNull != nil {
		os.Stdout = devNull
		defer func() {
			os.Stdout = stdout
			devNull.Close()
		}()
	}

	// First live bootstrap: empty store.
	exit1 := runS5Bootstrap(options{authorityStore: dir, s5Bootstrap: true})
	if exit1 != 0 {
		t.Fatalf("first runS5Bootstrap exit: got %d want 0", exit1)
	}

	// Seed a durable record between runs via the real
	// publisher+directive path (core/authority/ is the only
	// allowed writer per S5 boundary; this uses the same helper
	// used by the other bootstrap tests).
	seedDurableRecord(t, dir, authority.AssignmentAsk{
		VolumeID: "v-persisted", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: authority.IntentBind,
	})

	// Second run on the same directory must be restart-clean:
	// no stale-lock failure, no Bind-on-already-bound, exit 0.
	// It also must report the reload.
	logBuf, restore := captureLog(t)
	defer restore()

	exit2 := runS5Bootstrap(options{authorityStore: dir, s5Bootstrap: true})
	if exit2 != 0 {
		t.Fatalf("second runS5Bootstrap exit: got %d want 0 (restart-clean failure; log:\n%s)",
			exit2, logBuf.String())
	}

	if !strings.Contains(logBuf.String(), "durable authority reload: 1 records") {
		t.Fatalf("second run did not reload persisted record; log:\n%s", logBuf.String())
	}
}


// indexFailingAuthorityStore is a mock AuthorityStore whose
// LoadWithSkips always returns an index-level error. Used to
// deterministically exercise the pre-check cleanup path in
// BootstrapFromStore — a filesystem-permission-based approach
// does not portably reproduce the path across Unix and Windows.
type indexFailingAuthorityStore struct {
	closed bool
}

var errInjectedIndexFailure = errors.New("injected index failure")

func (s *indexFailingAuthorityStore) Load() ([]authority.DurableRecord, error) {
	return nil, nil
}
func (s *indexFailingAuthorityStore) LoadWithSkips() ([]authority.DurableRecord, []error, error) {
	return nil, nil, errInjectedIndexFailure
}
func (s *indexFailingAuthorityStore) Put(_ authority.DurableRecord) error { return nil }
func (s *indexFailingAuthorityStore) Close() error                        { s.closed = true; return nil }

// TestBootstrap_PreCheckFailure_ReleasesLockAndClosesStore is
// the direct regression for architect round-8 finding #2 plus
// round-9 finding 3 (previous test did not actually exercise
// the pre-check branch).
//
// Uses the BootstrapFromStore seam to inject a mock store whose
// LoadWithSkips deterministically returns an index-level error
// on every platform. Asserts:
//   1. BootstrapFromStore returns the wrapped injected error.
//   2. The mock store's Close() was called.
//   3. A fresh AcquireStoreLock on the same directory succeeds,
//      proving the lock was released despite the failure — the
//      core cleanup contract of the round-8 fix.
func TestBootstrap_PreCheckFailure_ReleasesLockAndClosesStore(t *testing.T) {
	dir := t.TempDir()

	lock, err := authority.AcquireStoreLock(dir)
	if err != nil {
		t.Fatalf("AcquireStoreLock: %v", err)
	}

	fake := &indexFailingAuthorityStore{}
	_, err = BootstrapFromStore(lock, fake, authority.NewStaticDirective(nil))
	if err == nil {
		t.Fatal("BootstrapFromStore must return error on index-level LoadWithSkips failure")
	}
	if !errors.Is(err, errInjectedIndexFailure) {
		t.Fatalf("error must wrap the injected sentinel, got %v", err)
	}
	if !fake.closed {
		t.Fatal("store.Close() must be called on pre-check failure")
	}

	// Second AcquireStoreLock must succeed — proves the lock was
	// released by the failing BootstrapFromStore path.
	lock2, err := authority.AcquireStoreLock(dir)
	if err != nil {
		t.Fatalf("AcquireStoreLock after pre-check failure must succeed (proves release): %v", err)
	}
	_ = lock2.Release()
}

func TestBootstrap_Close_Idempotent(t *testing.T) {
	dir := t.TempDir()
	boot, err := Bootstrap(dir, authority.NewStaticDirective(nil))
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if err := boot.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := boot.Close(); err != nil {
		t.Fatalf("second Close must be idempotent: %v", err)
	}
}
