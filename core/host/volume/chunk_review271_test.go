package volume

import (
	"errors"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"net/http"
	"testing"
)

func TestChunkBudgetAdmissionRefusalIsRetryable(t *testing.T) {
	status, code, retry := chunkErrorCode(errors.Join(storage.ErrChunkCaptureRefused, errors.New("host full")))
	if status != http.StatusServiceUnavailable || code != "capture_refused" || !retry {
		t.Fatalf("%d %s retry=%v", status, code, retry)
	}
	// Ownership always wins over a resource refusal.
	status, _, retry = chunkErrorCode(errors.Join(storage.ErrChunkCaptureRefused, storage.ErrChunkLayerFenced))
	if status != http.StatusConflict || retry {
		t.Fatalf("fenced: %d retry=%v", status, retry)
	}
}
