package storage

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func mappedBASELayerV3(t *testing.T) (*WALStore, *fakeChunkRemote, *chunkLayerImpl) {
	t.Helper()
	s, _ := newMappedCheckpointStoreV3(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	return s, f, l
}

func TestChunkLayerV3_BASEIncompleteCaptureGuard(t *testing.T) {
	s, f, l := mappedBASELayerV3(t)
	h, e := s.BeginBaseInstall(7, idA)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.WriteBase(5, blk(0x55)); e != nil {
		t.Fatal(e)
	}
	before := f.stats()
	if _, e = l.Cut(ctTimeout(t)); !errors.Is(e, errChunkBaseInstalling) || !errors.Is(e, errChunkCaptureRefused) {
		t.Fatalf("missing early typed incomplete capture guard: %v", e)
	}
	after := f.stats()
	if after.puts != before.puts || after.cas != before.cas {
		t.Fatal("incomplete capture performed remote writes")
	}
	if e = h.MarkBaseComplete(); e != nil {
		t.Fatal(e)
	}
	if _, e = l.Cut(ctTimeout(t)); e != nil {
		t.Fatal(e)
	}
}

func TestChunkLayerV3_BASEBeginDrainsWholeCut(t *testing.T) {
	s, f, l := mappedBASELayerV3(t)
	if _, e := s.Write(5, blk(0x11)); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Sync(); e != nil {
		t.Fatal(e)
	}
	entered, release := holdPuts(t, f)
	cutDone := startCapture(t, l)
	waitKey(t, entered, "held cut upload after barrier")
	attempted := make(chan struct{})
	l.testBaseTransitionBeforeLock = func() { close(attempted) }
	type result struct {
		h   InstallHandle
		err error
	}
	finished := make(chan result, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		release()
		select {
		case <-joined:
		case <-time.After(ctLocalBound):
			t.Error("BASE begin worker not joined")
		}
	})
	go func() { defer close(joined); h, e := s.BeginBaseInstall(7, idA); finished <- result{h, e} }()
	select {
	case <-attempted:
	case <-time.After(ctLocalBound):
		t.Fatal("BASE did not reach outer drain")
	}
	select {
	case r := <-finished:
		t.Fatalf("BASE begin completed before the captured upload/CAS drained: %v", r.err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if e := waitErr(t, cutDone, "old cut settlement"); e != nil {
		t.Fatal(e)
	}
	var r result
	select {
	case r = <-finished:
	case <-time.After(ctLocalBound):
		t.Fatal("BASE begin failed to resume after cut settled")
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	l.testBaseTransitionBeforeLock = nil
	old := f.stats().head
	if old.RemoteLSN != 1 || !bytes.Equal(f.imageOf(t, old)[5*ctBlockSize:6*ctBlockSize], blk(0x11)) {
		t.Fatal("old cut was not settled before installing")
	}
	if e := r.h.WriteBase(5, blk(0x77)); e != nil {
		t.Fatal(e)
	}
	if _, e := l.Cut(ctTimeout(t)); !errors.Is(e, errChunkBaseInstalling) {
		t.Fatal("partial BASE capture", e)
	}
	if !sameHead(f.stats().head, old) {
		t.Fatal("HEAD advanced during partial BASE")
	}
	if e := r.h.MarkBaseComplete(); e != nil {
		t.Fatal(e)
	}
	next, e := l.Cut(ctTimeout(t))
	if e != nil {
		t.Fatal(e)
	}
	if next.H < 7 || !bytes.Equal(f.imageOf(t, next.Head)[5*ctBlockSize:6*ctBlockSize], blk(0x77)) {
		t.Fatal("completed BASE did not replace the old cut")
	}
}

func TestChunkLayerV3_BASEBeginRefusesUnsettledCAS(t *testing.T) {
	s, f, l := mappedBASELayerV3(t)
	if _, e := s.Write(5, blk(0x11)); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Sync(); e != nil {
		t.Fatal(e)
	}
	f.mu.Lock()
	f.casModes = []fakeCASMode{casDeferred}
	f.mu.Unlock()
	if _, e := l.Cut(ctTimeout(t)); !errors.Is(e, errChunkCASOutcomeUnknown) {
		t.Fatal("setup did not retain ambiguous CAS", e)
	}
	before := s.v3.pub.current()
	if _, e := s.BeginBaseInstall(7, idA); !errors.Is(e, errChunkCASOutcomeUnknown) {
		t.Fatalf("BASE admitted while old CAS could still land: %v", e)
	}
	if after := s.v3.pub.current(); after != before {
		t.Fatal("unsettled CAS refusal changed descriptor")
	}
	f.releaseDeferredCAS()
	if _, e := l.Cut(ctTimeout(t)); e != nil {
		t.Fatal("old publication settlement", e)
	}
	h, e := s.BeginBaseInstall(7, idA)
	if e != nil {
		t.Fatal("settled cut did not allow begin", e)
	}
	if e = h.MarkBaseComplete(); e != nil {
		t.Fatal(e)
	}
}

func TestChunkLayerV3_BASEDrainJoinsClose(t *testing.T) {
	s, f, l := mappedBASELayerV3(t)
	if _, e := s.Write(5, blk(0x11)); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Sync(); e != nil {
		t.Fatal(e)
	}
	entered, release := holdPuts(t, f)
	cutDone := startCapture(t, l)
	waitKey(t, entered, "upload before Close")
	attempted := make(chan struct{})
	l.testBaseTransitionBeforeLock = func() { close(attempted) }
	beginDone := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		release()
		select {
		case <-joined:
		case <-time.After(ctLocalBound):
			t.Error("BASE worker not joined after Close")
		}
	})
	go func() { defer close(joined); _, e := s.BeginBaseInstall(7, idA); beginDone <- e }()
	select {
	case <-attempted:
	case <-time.After(ctLocalBound):
		t.Fatal("BASE never registered for drain")
	}
	within(t, "store Close cancels cut and joins BASE drain", s.Close)
	if e := waitErr(t, beginDone, "BASE after Close"); !errors.Is(e, errChunkLayerClosed) {
		t.Fatalf("BASE ignored closing layer: %v", e)
	}
	if e := waitErr(t, cutDone, "cancelled cut after Close"); e == nil {
		t.Fatal("cancelled upload cut succeeded")
	}
}

func TestChunkLayerV3_BASEPresenceBarrierFailureKeepsInstalling(t *testing.T) {
	s, f, l := mappedBASELayerV3(t)
	if _, e := s.Write(5, blk(0x11)); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Sync(); e != nil {
		t.Fatal(e)
	}
	if e := s.flusher.flushOnce(); e != nil {
		t.Fatal(e)
	}
	if _, e := l.Cut(ctTimeout(t)); e != nil {
		t.Fatal(e)
	}
	if e := l.EvictChunk(1); e != nil {
		t.Fatal(e)
	}
	h, e := s.BeginBaseInstall(7, idA)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.WriteBase(5, blk(0x77)); e != nil {
		t.Fatal(e)
	}
	before := s.v3.pub.current()
	injected := errors.New("injected BASE presence directory barrier")
	realSync := l.syncDirFn
	l.syncDirFn = func(string) error { return injected }
	t.Cleanup(func() { l.syncDirFn = realSync })
	if e = h.MarkBaseComplete(); !errors.Is(e, injected) {
		t.Fatalf("missing presence barrier error: %v", e)
	}
	if after := s.v3.pub.current(); after != before || after.BaseState != BaseInstalling {
		t.Fatal("failed presence barrier published installed")
	}
	if _, e = l.Cut(ctTimeout(t)); !errors.Is(e, errChunkBaseInstalling) {
		t.Fatal("failed BASE became capturable", e)
	}
	l.syncDirFn = realSync
	if e = h.MarkBaseComplete(); e != nil {
		t.Fatal("completion retry", e)
	}
	r, e := l.Cut(ctTimeout(t))
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(f.imageOf(t, r.Head)[5*ctBlockSize:6*ctBlockSize], blk(0x77)) {
		t.Fatal("retry installed an older remote block")
	}
}
