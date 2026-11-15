package storage

import (
	"errors"
	"testing"
)

// The causes a control plane reports as typed codes (B9 W2 §2.2) stay
// reachable with errors.Is through the layer's errors.

func TestChunkCut_BaseInstallRefusalIsTyped(t *testing.T) {
	s, _ := newChunkTestStore(t)
	layer := mustAttach(t, s, newFakeChunkRemote(), ctConfig(1))
	newWriteLog().write(t, s, 2, 0x22)
	if err := layer.BeginBaseInstall(); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) || !errors.Is(err, errChunkBaseInstalling) {
		t.Fatalf("cut during a BASE install: %v; want refused with errChunkBaseInstalling", err)
	}
}

func TestChunkCut_UploadFailureIsRemoteUnavailable(t *testing.T) {
	s, _ := newChunkTestStore(t)
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
	s, _ := newChunkTestStore(t)
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
	s, _ := newChunkTestStore(t)
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
