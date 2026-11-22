package chunkstore

import (
	"context"
	"errors"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// CHANGE M6: PurgeDone report section 9. Production is gated on the
// exhaustive model verdict and its final invariant row. No purge runs here
// against a real filer or volume server.
const purgeTestDoneStamp = "swb.gc.done_ns"

func putPurgeTestBinding(t *testing.T, f *fakeFiler, b gcBlobs, key, state string, stamp time.Time) (*ChunkRemote, *filerpb.Entry) {
	t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Put(ctx, key, []byte("unrooted data")); err != nil {
		t.Fatal(err)
	}
	dir, name, err := r.keyLocation(key)
	if err != nil {
		t.Fatal(err)
	}
	e, err := f.LookupEntry(ctx, dir, name)
	if err != nil {
		t.Fatal(err)
	}
	if state != "" {
		e.Extended[extGCDeleted] = []byte(state)
	}
	if !stamp.IsZero() {
		e.Extended[purgeTestDoneStamp] = []byte(strconv.FormatInt(stamp.UnixNano(), 10))
	}
	if err = f.UpdateEntry(ctx, dir, e, nil); err != nil {
		t.Fatal(err)
	}
	return r, e
}

func TestGCDonePurgeAdmissionUsesDoneAgeAndSkew(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		age         time.Duration
		purged      bool
	}{
		{"old-done", "done", time.Hour + time.Minute + time.Nanosecond, true},
		{"at-boundary", "done", time.Hour + time.Minute, false},
		{"young-done", "done", time.Hour, false},
		{"future-done", "done", -time.Hour, false},
		{"tombstoned", "1", 24 * time.Hour, false},
		{"unflagged", "", 24 * time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, b, _, g, now := gcFixture(t)
			r, e := putPurgeTestBinding(t, f, b, "orphan", tc.state, now.Add(-tc.age))
			if _, err := g.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err := f.LookupEntry(context.Background(), r.keyDir(), e.Name)
			if tc.purged {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("eligible DONE binding remains: %v", err)
				}
			} else if err != nil {
				t.Fatalf("ineligible binding disappeared: %v", err)
			}
		})
	}
}
