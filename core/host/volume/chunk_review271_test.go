package volume

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
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

func TestNamedSnapshotWindowAbortNeverSubstitutesPoint(t *testing.T) {
	fp, ctl := adapterFixture(t)
	fp.cutErr = storage.ErrCutCOWAborted
	info, err := ctl.Snapshot(context.Background(), "v1", ChunkSnapshotRequest{Name: "exact-point", RequestID: "attempt-1"})
	if !errors.Is(err, storage.ErrCutCOWAborted) || fp.cuts != 1 || info.HeadGen != 0 {
		t.Fatalf("snapshot=%+v cuts=%d err=%v", info, fp.cuts, err)
	}
	entries, _, listErr := ctl.ListSnapshots(context.Background(), "v1", chunkstore.ListOptions{})
	if listErr != nil || len(entries) != 0 {
		t.Fatalf("aborted named request published: %+v %v", entries, listErr)
	}
}
