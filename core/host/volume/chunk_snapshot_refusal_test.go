package volume

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// A snapshot the volume cannot name is a typed, final refusal: past the
// per-volume limit, or a name another snapshot already holds.
func TestChunkSnapshotRefusalsAreTyped(t *testing.T) {
	for _, c := range []struct {
		err  error
		code string
	}{
		{layout.ErrTooManySnapshots, chunkstore.CodeSnapshotLimit},
		{layout.ErrSnapshotExists, chunkstore.CodeSnapshotExists},
	} {
		status, code, retry := chunkErrorCode(fmt.Errorf("%w: at most 10 per volume", c.err))
		if status != http.StatusConflict || code != c.code || retry {
			t.Errorf("%v: %d %s retryable=%v; want 409 %s, not retryable", c.err, status, code, retry, c.code)
		}
	}
}
