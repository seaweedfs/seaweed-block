package durable

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// TestPlainDetachStopRefusesClosedBackend pins that the plain-detach stop of a
// closed backend returns ErrBackendClosed instead of reporting a clean stop.
func TestPlainDetachStopRefusesClosedBackend(t *testing.T) {
	b := precopyTestBackend(newHeldReadStore())
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.stopPlainDetachIfIdle(); !errors.Is(err, frontend.ErrBackendClosed) {
		t.Fatalf("plain-detach stop of a closed backend: %v", err)
	}
}
