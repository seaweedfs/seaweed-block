package durable

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// Start's definite refusals end this recovery; a read failure or a CAS
// precondition can be retried. Detached uses the replay-retained branch.
func TestFinalLayoutStartRefusalTable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		final bool
	}{
		{"stale local root", layout.ErrStaleLocalRoot, true},
		{"missing local root", layout.ErrMissingLocalRoot, true},
		{"geometry", layout.ErrGeometryMismatch, true},
		{"absent HEAD", layout.ErrHeadAbsent, true},
		{"deleting", layout.ErrVolumeDeleting, true},
		{"stale epoch", layout.ErrStaleEpoch, true},
		{"another holder", layout.ErrNotHolder, true},
		{"foreign volume", layout.ErrIdentity, true},
		{"corrupt store", layout.ErrCorrupt, true},
		{"malformed store", layout.ErrMalformed, true},
		{"missing VOLUME", layout.ErrMissing, true},
		{"oversized HEAD", layout.ErrHeadTooLarge, true},
		{"legacy store", chunkstore.ErrLegacyStore, true},
		{"CAS precondition", layout.ErrPrecondition, false},
		{"busy", layout.ErrBusy, false},
		{"unanswered read", context.DeadlineExceeded, false},
		{"detached replay", layout.ErrDetached, false},
		{"other error", errors.New("network"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := finalLayoutStartRefusal(fmt.Errorf("Start: %w", tc.err)); got != tc.final {
				t.Fatalf("final=%t, want %t", got, tc.final)
			}
		})
	}
}
