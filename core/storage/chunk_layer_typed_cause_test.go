package storage

import (
	"errors"
	"testing"
)

func TestChunkCut_UploadFailureIsRemoteUnavailable(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	newWriteLog().write(t, s, 2, 0x22)
	f.mu.Lock()
	f.failPut = errFakeRemoteDown
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkRemoteUnavailable) || !errors.Is(err, errFakeRemoteDown) {
		t.Fatalf("cut whose upload failed: %v; want errChunkRemoteUnavailable wrapping the remote's error", err)
	}
}

func TestChunkCut_ClosedLayerIsTyped(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	layer := mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerClosed) {
		t.Fatalf("cut on a closed layer: %v; want errChunkLayerClosed", err)
	}
}

// A CAS that returned no definite answer, after which HEAD reads a newer
// generation from the same holder, did not apply: a head conflict.
func TestChunkCut_CASLostToMovedHeadIsHeadConflict(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 3, 0x33)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casUnknownNotApplied}
	f.beforeCAS = func(f *fakeChunkRemote) { f.installLocked(copyChunkHead(f.head)) }
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkHeadPrecondition) || errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("cut whose CAS lost to a moved HEAD: %v; want errChunkHeadPrecondition, not fenced", err)
	}
}
