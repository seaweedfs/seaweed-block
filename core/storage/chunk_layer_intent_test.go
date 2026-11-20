package storage

// The capture path's intents (W5): a cut that uploads chunks durably writes
// an intent naming at least those keys before the first upload, and deletes
// only its own intent once the outcome is settled: after a confirmed HEAD
// CAS, or a clean abort. An uncertain CAS keeps the intent until a later
// HEAD read settles it.

import (
	"context"
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

func liveIntents(t *testing.T, f *fakeChunkRemote) []chunkIntent {
	t.Helper()
	in, err := f.ListIntents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func lastPutIntent(t *testing.T, f *fakeChunkRemote) chunkIntent {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.putIntents) == 0 {
		t.Fatal("no intent was written")
	}
	return f.putIntents[len(f.putIntents)-1]
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

func TestChunkIntent_WrittenBeforeFirstUploadAndDeletedAfterCAS(t *testing.T) {
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
	in := lastPutIntent(t, f)
	iIntent := indexOf(ops, func(op string) bool { return op == "putintent:"+in.ID })
	iPut := indexOf(ops, func(op string) bool { return strings.HasPrefix(op, "put:") })
	iCAS := indexOf(ops, func(op string) bool { return op == "cas:ok" })
	iDel := indexOf(ops, func(op string) bool { return op == "deleteintent:"+in.ID })
	if iIntent < 0 || iPut < 0 || iIntent > iPut {
		t.Fatalf("intent not written before the first upload: ops %q", ops)
	}
	if iCAS < 0 || iDel < iCAS {
		t.Fatalf("intent not deleted after the confirmed CAS: ops %q", ops)
	}
	keys := append([]string(nil), in.Keys...)
	sort.Strings(keys)
	for _, key := range uploadedKeys(ops) {
		i := sort.SearchStrings(keys, key)
		if i == len(keys) || keys[i] != key {
			t.Fatalf("uploaded key %q absent from intent %q", key, keys)
		}
	}
	if in.Gen != 0 || in.Epoch != 1 || in.Holder != "host-a" {
		t.Fatalf("intent gen %d epoch %d holder %q, want the cut's base gen 0, epoch 1, host-a", in.Gen, in.Epoch, in.Holder)
	}
	if live := liveIntents(t, f); len(live) != 0 {
		t.Fatalf("intents left after a published cut: %+v", live)
	}
}

func TestChunkIntent_CutWithoutUploadWritesNoIntent(t *testing.T) {
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
	if indexOf(ops, func(op string) bool { return strings.HasPrefix(op, "putintent") }) >= 0 {
		t.Fatalf("a cut with no upload wrote an intent: ops %q", ops)
	}
	if indexOf(ops, func(op string) bool { return op == "cas:ok" }) < 0 {
		t.Fatalf("the dropping cut did not publish: ops %q", ops)
	}
}

func TestChunkIntent_FailedPutIntentUploadsNothing(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.failPutIntent = errors.New("injected PutIntent failure")
	f.mu.Unlock()
	mark := len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkRemoteUnavailable) {
		t.Fatalf("cut with a failed intent write: %v; want a remote-unavailable refusal", err)
	}
	if got := uploadedKeys(opsSince(f, mark)); len(got) != 0 {
		t.Fatalf("chunks uploaded without an intent: %q", got)
	}
	f.mu.Lock()
	f.failPutIntent = nil
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatalf("cut after the intent store recovered: %v", err)
	}
	if heads := f.installedHeads(); len(heads) != 1 || len(heads[0].Chunks) != 1 {
		t.Fatalf("the retried cut did not publish the write: %+v", heads)
	}
}

func TestChunkIntent_FailedUploadDeletesIntent(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.failPut = errors.New("injected upload failure")
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkRemoteUnavailable) {
		t.Fatalf("cut with a failed upload: %v; want a remote-unavailable refusal", err)
	}
	in := lastPutIntent(t, f)
	if live := liveIntents(t, f); len(live) != 0 {
		t.Fatalf("intent %s left after a cleanly aborted cut: %+v", in.ID, live)
	}
}

// HEAD is taken over before the CAS: the CAS is refused for good and the
// layer fences. Its uploads are orphans; its intent goes.
func TestChunkIntent_RefusedCASDeletesIntent(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.beforeCAS = func(f *fakeChunkRemote) { f.takeOverLocked("host-b") }
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("cut against a taken-over HEAD: %v; want fenced", err)
	}
	if live := liveIntents(t, f); len(live) != 0 {
		t.Fatalf("intent left after a refused CAS: %+v", live)
	}
}

// The CAS outcome is unknown and HEAD still holds its base: the intent
// stays until the next cut settles the proposal, then goes.
func TestChunkIntent_UncertainCASKeepsIntentUntilSettled(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casUnknownNotApplied}
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("cut with an unknown CAS outcome: %v", err)
	}
	in := lastPutIntent(t, f)
	if live := liveIntents(t, f); len(live) != 1 || live[0].ID != in.ID {
		t.Fatalf("intents while the CAS is unresolved: %+v; want only %s", live, in.ID)
	}
	mark := len(f.stats().ops)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatalf("settling cut: %v", err)
	}
	ops := opsSince(f, mark)
	iCAS := indexOf(ops, func(op string) bool { return op == "cas:ok" })
	iDel := indexOf(ops, func(op string) bool { return op == "deleteintent:"+in.ID })
	if iCAS < 0 || iDel < iCAS {
		t.Fatalf("held intent not deleted after its proposal landed: ops %q", ops)
	}
	if live := liveIntents(t, f); len(live) != 0 {
		t.Fatalf("intents left after settlement: %+v", live)
	}
}

// The CAS applied but its response was lost: the HEAD re-read confirms it,
// and the intent goes in the same cut.
func TestChunkIntent_LostCASResponseConfirmedDeletesIntent(t *testing.T) {
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
	if live := liveIntents(t, f); len(live) != 0 {
		t.Fatalf("intent left after a confirmed (re-read) CAS: %+v", live)
	}
}

// A failed DeleteIntent does not fail a published cut; the next cut deletes
// the leftover.
func TestChunkIntent_FailedDeleteIsRetriedNextCut(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.failDeleteIntent = 1
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatalf("published cut reported the intent delete failure: %v", err)
	}
	first := lastPutIntent(t, f)
	if live := liveIntents(t, f); len(live) != 1 || live[0].ID != first.ID {
		t.Fatalf("intents after a failed delete: %+v", live)
	}
	w.write(t, s, 9, 0x29)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if live := liveIntents(t, f); len(live) != 0 {
		t.Fatalf("leftover intent not retried: %+v", live)
	}
}

// A cut deletes only its own intent.
func TestChunkIntent_OtherIntentsUntouched(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	other, err := f.PutIntent(context.Background(), chunkIntent{Gen: 0, Epoch: 1, Holder: "snapshot-op", Keys: []string{"chunks/e1/c0/other-1"}})
	if err != nil {
		t.Fatal(err)
	}
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if live := liveIntents(t, f); len(live) != 1 || live[0].ID != other {
		t.Fatalf("intents after the cut: %+v; want only %s", live, other)
	}
}

// Admission is refused at the final check right before the CAS: a clean
// abort, so the intent goes.
func TestChunkIntent_AdmissionRefusedBeforeCASKeepsReusableIntent(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.CaptureAllowed = func() error {
		// The gate is checked concurrently between COW claims as well. Bind
		// demotion to the real upload event, not a call count/schedule.
		if f.stats().puts > 0 { // uploaded under an intent, before the CAS
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
		t.Fatal("control did not reach the intent-protected upload")
	}
	if n := f.stats().cas; n != 0 {
		t.Fatalf("refused cut issued %d CAS", n)
	}
	if live := liveIntents(t, f); len(live) == 0 {
		t.Fatal("completed upload lost its retry root after admission refusal")
	}
	// Restored admission reuses the exact payload and settles its root.
	layer.(*chunkLayerImpl).cfg.CaptureAllowed = nil
	before := f.stats().puts
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if f.stats().puts != before {
		t.Fatal("unchanged refused point uploaded again")
	}
	assertNoIntentsLeft(t, f)
}
