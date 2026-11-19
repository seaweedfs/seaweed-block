package storage

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCutPayload_CloseJoinsAdmittedLocalWrite(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, freed sync.Once
	free := func() { freed.Do(func() { close(release) }) }
	l.testPayloadWrite = func() { once.Do(func() { close(entered) }); <-release }
	done, errs := cowStartCut(t, l, gate)
	t.Cleanup(func() { free(); gate.unblock() })
	gate.unblock()
	cowWaitSignal(t, entered, "admitted payload write")
	closed := make(chan struct{})
	t.Cleanup(func() { free(); cowWaitSignal(t, closed, "layer close cleanup") })
	go func() { defer close(closed); _ = l.Close() }()
	select {
	case <-closed:
		t.Fatal("Close abandoned local payload write")
	case <-time.After(20 * time.Millisecond):
	}
	files, err := filepath.Glob(s.path + ".cut-cow-payload-*")
	if err != nil || len(files) != 1 {
		t.Fatalf("owned file disappeared: %v %v", files, err)
	}
	free()
	cowWaitSignal(t, done, "cancelled copy")
	if err = <-errs; !errors.Is(err, errCutCOWAborted) {
		t.Fatalf("cancelled cut: %v", err)
	}
	cowWaitSignal(t, closed, "layer close")
	files, err = filepath.Glob(s.path + ".cut-cow-*")
	if err != nil || len(files) != 0 {
		t.Fatalf("unsettled scratch files: %v %v", files, err)
	}
}
