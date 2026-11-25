package chunkstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// A stop can abort its session while an adapter HEAD read is already in
// progress, before the adapter consumes the abandonment notice. The read
// then observes an ended session and invalidates its staging view. That
// view change alone must not fence the holder on its next no-op cut.
func TestLayoutRemote_NoOpCutAfterStopAbortKeepsTheHolder(t *testing.T) {
	for _, readBeforeNotice := range []bool{false, true} {
		name := "notice-before-read"
		if readBeforeNotice {
			name = "read-before-notice"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			lh := newLayoutHost(t)
			s, l, r := lh.attach("a", true, true)
			defer s.Close()
			writeSync(t, s, map[uint32][]byte{2: block(2)})
			if _, err := l.Capture(ctx); err != nil {
				t.Fatal(err)
			}
			before := lh.head()
			session, err := r.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			st, err := lh.cat.Store(lh.v)
			if err != nil {
				t.Fatal(err)
			}
			// This is the stop's kernel abort. Splitting its kernel write
			// from its non-blocking adapter notice forces the model order.
			if _, err := layout.RunAbort(ctx, st, lh.me, session); err != nil {
				t.Fatal(err)
			}
			if readBeforeNotice {
				if _, err := r.GetHead(ctx); err != nil {
					t.Fatal(err)
				}
			}
			r.Abort()
			if _, err := l.Capture(ctx); err != nil {
				t.Fatalf("no-op cut after an aborted stop fenced the unchanged holder: %v", err)
			}
			after := lh.head()
			if after.Epoch != before.Epoch || after.Holder != before.Holder || after.ClaimID != before.ClaimID ||
				after.State != layout.StateLive || after.Position != before.Position ||
				after.Current == nil || before.Current == nil || *after.Current != *before.Current {
				t.Fatalf("abort/no-op changed holder or image: before %+v, after %+v", before, after)
			}
			got, err := s.Read(2)
			if err != nil || !bytes.Equal(got, block(2)) {
				t.Fatalf("acknowledged bytes after resume: %x, %v", got, err)
			}
			writeSync(t, s, map[uint32][]byte{3: block(3)})
			if _, err := l.Capture(ctx); err != nil {
				t.Fatalf("holder could not publish after resume: %v", err)
			}
		})
	}
}
