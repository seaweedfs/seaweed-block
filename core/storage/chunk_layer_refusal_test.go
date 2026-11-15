package storage

import (
	"errors"
	"fmt"
	"testing"
)

// A capture refused by the owner's admission check keeps the owner's typed
// cause, so callers can tell "not primary" from "lineage changed" (P1).
func TestChunkCapture_RefusalIsTyped(t *testing.T) {
	errNotPrimary := errors.New("test: not primary")
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.CaptureAllowed = func() error { return fmt.Errorf("volume v1: %w", errNotPrimary) }
	layer := mustAttach(t, s, f, cfg)
	newWriteLog().write(t, s, 1, 0x11)

	_, err := layer.Capture(ctTimeout(t))
	if !errors.Is(err, errChunkCaptureRefused) || !errors.Is(err, errNotPrimary) {
		t.Fatalf("Capture refusal %v: want both errChunkCaptureRefused and the owner's cause", err)
	}
	if n := f.stats().cas; n != 0 {
		t.Fatalf("a refused capture issued %d CAS calls", n)
	}
}
