package volume

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A refusal because every host cut slot is held has its own retryable
// code; other capture refusals, a plain host busy included, keep
// capture_refused.
func TestHostSlotHeldHasItsOwnCode(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{
		{fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrSlotsHeld), codeHostSlotBusy},
		{fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrUrgentPending), codeHostSlotBusy},
		{fmt.Errorf("%w: budget admission: %w", storage.ErrChunkCaptureRefused, hostlimit.ErrBusy), "capture_refused"},
		{fmt.Errorf("%w: not a healthy primary", storage.ErrChunkCaptureRefused), "capture_refused"},
	} {
		status, code, retryable := chunkErrorCode(tc.err)
		if status != http.StatusServiceUnavailable || code != tc.code || !retryable {
			t.Fatalf("%v: %d %s %v; want 503 %s retryable", tc.err, status, code, retryable, tc.code)
		}
	}
}
