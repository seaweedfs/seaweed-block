package storage

import (
	"context"
	"errors"
	"testing"
)

// Abort at a witnessed PUT boundary, after three immutable chunks are stored.
// Count submitted bytes as well as completed PUTs so a retransmission cannot
// hide behind a successful final HEAD.
type abortUploadBytesRemote struct {
	*fakeChunkRemote
	cancel                                     context.CancelFunc
	calls, stored, submittedBytes, storedBytes int
}

func (r *abortUploadBytesRemote) Put(ctx context.Context, key string, b []byte) error {
	r.calls++
	r.submittedBytes += len(b)
	if r.stored == 3 && r.cancel != nil {
		r.cancel()
		return ctx.Err()
	}
	if err := r.fakeChunkRemote.Put(ctx, key, b); err != nil {
		return err
	}
	r.stored++
	r.storedBytes += len(b)
	return nil
}

// INV-CUT-STAGED-REUSE-001: an interrupted rolling cut retains its completed
// uploads in-process; retrying the unchanged image sends only the remainder.
func TestChunkStageV3_MidUploadAbortReducesRetryBytes(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1, 5)
	l.cfg.COWSpillLimit = ctChunkBytes
	l.cfg.UploadConcurrency = 1
	ctx, cancel := context.WithCancel(ctTimeout(t))
	defer cancel()
	r := &abortUploadBytesRemote{fakeChunkRemote: f, cancel: cancel}
	l.remote = r
	before := f.stats().head
	frontier := s.NextLSN() - 1
	want := w.imageAt(frontier)
	if _, err := l.Cut(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-upload cut: %v, want cancellation", err)
	}
	if f.stats().head.Gen != before.Gen {
		t.Fatal("aborted cut published HEAD")
	}
	if r.calls != 4 || r.stored != 3 || r.storedBytes != 3*ctChunkBytes {
		t.Fatalf("abort boundary: calls=%d stored=%d bytes=%d", r.calls, r.stored, r.storedBytes)
	}
	firstCalls, firstStored := r.calls, r.stored
	firstSubmitted, firstBytes := r.submittedBytes, r.storedBytes
	r.cancel = nil
	result, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	retryCalls, retryStored := r.calls-firstCalls, r.stored-firstStored
	retrySubmitted, retryBytes := r.submittedBytes-firstSubmitted, r.storedBytes-firstBytes
	t.Logf("first: PUTs=%d stored=%d submitted_bytes=%d stored_bytes=%d; retry: PUTs=%d stored=%d submitted_bytes=%d stored_bytes=%d", firstCalls, firstStored, firstSubmitted, firstBytes, retryCalls, retryStored, retrySubmitted, retryBytes)
	if retryCalls != 2 || retryStored != 2 || retryBytes != 2*ctChunkBytes || retrySubmitted != retryBytes {
		t.Fatalf("retry did not send only the two remaining chunks: PUTs=%d stored=%d submitted=%d stored_bytes=%d", retryCalls, retryStored, retrySubmitted, retryBytes)
	}
	if retrySubmitted >= firstSubmitted || retryBytes >= firstBytes {
		t.Fatal("retry uploaded at least as many bytes as the aborted attempt")
	}
	if result.Head.RemoteLSN != frontier {
		t.Fatalf("retry frontier=%d want%d", result.Head.RemoteLSN, frontier)
	}
	assertImage(t, "retry image", f.imageOf(t, result.Head), want)
	assertNoSessionOpen(t, f)
}
