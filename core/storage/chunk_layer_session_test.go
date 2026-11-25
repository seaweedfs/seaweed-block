package storage

// The capture path's session: a cut that uploads chunks begins the holder's
// session before the first upload; the commit (a confirmed HEAD CAS) ends
// it. The session lives as long as the staged uploads: a refused cut that
// keeps them (an upload error, admission, frozen tail) keeps it, and its
// retry commits them without staging again; dropping them (fenced, a HEAD
// moved past the commit's base, Close) aborts it. An uncertain commit keeps
// the session, and the settlement replays the same commit in it.

import (
	"errors"
	"sort"
	"strings"
	"testing"
)

// opsSince returns the remote ops recorded after mark.
func opsSince(f *fakeChunkRemote, mark int) []string {
	return f.stats().ops[mark:]
}

func indexOf(ops []string, pred func(string) bool) int {
	for i, op := range ops {
		if pred(op) {
			return i
		}
	}
	return -1
}

func countOps(ops []string, prefix string) int {
	n := 0
	for _, op := range ops {
		if strings.HasPrefix(op, prefix) {
			n++
		}
	}
	return n
}

func uploadedKeys(ops []string) []string {
	var keys []string
	for _, op := range ops {
		if k, ok := strings.CutPrefix(op, "put:"); ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// assertNoSessionOpen: the cut settled, nothing of it is still open.
func assertNoSessionOpen(t *testing.T, f *fakeChunkRemote) {
	t.Helper()
	if s := f.openSession(); s != "" {
		t.Fatalf("session %q still open after the cut settled", s)
	}
}

func TestSession_BegunBeforeFirstUploadAndClosedByCommit(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	w.write(t, s, 9, 0x29)

	mark := len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	ops := opsSince(f, mark)
	iBegin := indexOf(ops, func(op string) bool { return op == "begin:s1" })
	iPut := indexOf(ops, func(op string) bool { return strings.HasPrefix(op, "put:") })
	iCAS := indexOf(ops, func(op string) bool { return op == "cas:ok" })
	if iBegin < 0 || iPut < 0 || iBegin > iPut {
		t.Fatalf("session not begun before the first upload: ops %q", ops)
	}
	if iCAS < iPut || countOps(ops, "abort:") != 0 {
		t.Fatalf("the published cut did not end by its commit alone: ops %q", ops)
	}
	assertNoSessionOpen(t, f)
}

func TestSession_CutWithoutUploadBeginsNone(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 2, 0x00) // the chunk is zero again: the cut only drops it
	mark := len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	ops := opsSince(f, mark)
	if countOps(ops, "begin:") != 0 {
		t.Fatalf("a cut with no upload began a session: ops %q", ops)
	}
	if indexOf(ops, func(op string) bool { return op == "cas:ok" }) < 0 {
		t.Fatalf("the dropping cut did not publish: ops %q", ops)
	}
}

func TestSession_FailedBeginUploadsNothing(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.failBegin = errors.New("injected Begin failure")
	f.mu.Unlock()
	mark := len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkRemoteUnavailable) {
		t.Fatalf("cut with a failed Begin: %v; want a remote-unavailable refusal", err)
	}
	if got := uploadedKeys(opsSince(f, mark)); len(got) != 0 {
		t.Fatalf("chunks uploaded without a session: %q", got)
	}
	f.mu.Lock()
	f.failBegin = nil
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatalf("cut after Begin recovered: %v", err)
	}
	if heads := f.installedHeads(); len(heads) != 1 || len(heads[0].Chunks) != 1 {
		t.Fatalf("the retried cut did not publish the write: %+v", heads)
	}
}

func TestSession_FailedUploadKeepsItForTheRetry(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.failPut = errors.New("injected upload failure")
	f.mu.Unlock()
	mark := len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkRemoteUnavailable) {
		t.Fatalf("cut with a failed upload: %v; want a remote-unavailable refusal", err)
	}
	if ops := opsSince(f, mark); countOps(ops, "abort:") != 0 || f.openSession() != "s1" {
		t.Fatalf("the failed cut gave up its session: ops %q, open %q", ops, f.openSession())
	}
	f.mu.Lock()
	f.failPut = nil
	f.mu.Unlock()
	mark = len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	ops := opsSince(f, mark)
	iCAS := indexOf(ops, func(op string) bool { return op == "cas:ok" })
	if iCAS < 0 || countOps(ops[:iCAS], "begin:s") != 0 || countOps(ops, "abort:") != 0 {
		t.Fatalf("the retry did not commit in the kept session: ops %q", ops)
	}
	assertNoSessionOpen(t, f)
}

// HEAD is taken over before the CAS: the CAS is refused for good and the
// layer fences. It aborts only its own session (the new owner's HEAD is
// not this remote's to end: the layout kernel refuses another holder's
// Abort, TestLayoutRemote_AbortNeverEndsAnotherHoldersSession).
func TestSession_ForeignOwnerRefusalAbortsNothingOfTheirs(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.beforeCAS = func(f *fakeChunkRemote) { f.takeOverLocked("host-b") }
	f.mu.Unlock()
	mark := len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("cut against a taken-over HEAD: %v; want fenced", err)
	}
	ops := opsSince(f, mark)
	if n := countOps(ops, "abort:"); n > 1 || (n == 1 && countOps(ops, "abort:s1") != 1) {
		t.Fatalf("the fenced cut aborted something other than its own session: ops %q", ops)
	}
	if got := f.stats().head.Holder; got != "host-b" {
		t.Fatalf("HEAD after the refused cut: holder %q, want the new owner host-b", got)
	}
}

// The commit's outcome is unknown and HEAD still holds its base: the
// session stays open (no Abort), and the next cut settles the proposal by
// replaying the same commit in that same session (no new Begin). The stop
// settlement is the other row: it aborts, then reads
// (TestLayoutStopAbortMustConfirmItsOutcome).
func TestSession_UncertainCommitIsNotAbortedUntilSettled(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casUnknownNotApplied}
	f.mu.Unlock()
	mark := len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("cut with an unknown CAS outcome: %v", err)
	}
	if ops := opsSince(f, mark); countOps(ops, "abort:") != 0 || f.openSession() != "s1" {
		t.Fatalf("an unresolved commit's session was aborted: ops %q, open %q", ops, f.openSession())
	}
	mark = len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatalf("settling cut: %v", err)
	}
	ops := opsSince(f, mark)
	iCAS := indexOf(ops, func(op string) bool { return op == "cas:ok" })
	if iCAS < 0 || countOps(ops[:iCAS], "abort:") != 0 || countOps(ops[:iCAS], "begin:s") != 0 {
		t.Fatalf("the settlement did not replay the commit in the open session: ops %q", ops)
	}
	assertNoSessionOpen(t, f)
}

// The CAS applied but its response was lost: the HEAD re-read confirms it,
// and nothing of the cut is left open.
func TestSession_LostCommitReplyConfirmedLeavesNoneOpen(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casAppliedLost}
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatalf("cut whose CAS response was lost: %v", err)
	}
	assertNoSessionOpen(t, f)
}

// Admission is refused at the final check right before the CAS: the cut
// ends without a commit (its session is aborted), its completed uploads
// stay reusable, and the next cut publishes them without uploading again.
func TestSession_AdmissionRefusedBeforeCommitKeepsReusableUploads(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.CaptureAllowed = func() error {
		// The gate is checked concurrently between COW claims as well. Bind
		// demotion to the real upload event, not a call count/schedule.
		if f.stats().puts > 0 { // uploaded in the session, before the CAS
			return errors.New("demoted")
		}
		return nil
	}
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) {
		t.Fatalf("cut refused before its CAS: %v", err)
	}
	if f.stats().puts == 0 {
		t.Fatal("control did not reach the upload")
	}
	if n := f.stats().cas; n != 0 {
		t.Fatalf("refused cut issued %d CAS", n)
	}
	// The refusal keeps the reusable uploads, so it keeps their session:
	// aborting it would make the retry's commit find them not prepared and
	// stage everything again (no convergence under sustained writes).
	if ops := f.stats().ops; countOps(ops, "abort:") != 0 || f.openSession() != "s1" {
		t.Fatalf("the admission refusal gave up its session: ops %q, open %q", ops, f.openSession())
	}
	// Restored admission reuses the exact payload, in the same session.
	layer.(*chunkLayerImpl).cfg.CaptureAllowed = nil
	before := f.stats().puts
	mark := len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if f.stats().puts != before {
		t.Fatal("unchanged refused point uploaded again")
	}
	if ops := opsSince(f, mark); countOps(ops, "begin:s") != 0 || countOps(ops, "abort:") != 0 {
		t.Fatalf("the retry did not commit in the kept session: ops %q", ops)
	}
	assertNoSessionOpen(t, f)
}
