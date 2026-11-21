package durable

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// M5 PlainStop -> Quiesce permits a closed idle handle to seal, while the
// frontend remains closed to guest I/O even if the offline attempt aborts.
func TestPlainDetachStopAcceptsClosedIdleBackend(t *testing.T) {
	b := precopyTestBackend(newHeldReadStore())
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.stopPlainDetachIfIdle(); err != nil {
		t.Fatalf("closed idle backend cannot seal: %v", err)
	}
	if err := b.readyCheck(context.Background()); !errors.Is(err, frontend.ErrBackendClosed) {
		t.Fatalf("offline admission reopened frontend: %v", err)
	}
}
