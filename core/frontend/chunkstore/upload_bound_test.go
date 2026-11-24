package chunkstore

import (
	"context"
	"errors"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// stallingFiler never answers a create whose path has the prefix stall: it
// waits for the call's context, as a filer that does not reply.
type stallingFiler struct {
	*fakeFiler
	stall string
}

func (s *stallingFiler) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, oExcl bool) error {
	if strings.HasPrefix(path.Join(dir, e.GetName()), s.stall) {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.fakeFiler.CreateEntry(ctx, dir, e, oExcl)
}

func shortUploadBound(t *testing.T) {
	t.Helper()
	old := objectUploadTimeout
	objectUploadTimeout = 50 * time.Millisecond
	t.Cleanup(func() { objectUploadTimeout = old })
}

// within fails the test unless fn returns within d.
func within(t *testing.T, d time.Duration, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("still waiting after %v: the upload-to-bind interval has no bound", d)
		return nil
	}
}

// A background cut and a takeover's legacy upgrade call PrepareManifest with
// a context that has no deadline. A segment's upload and bind still end
// within the object upload bound: a needle is bound within it or never.
func TestUploadBound_SegmentWithoutCallerDeadline(t *testing.T) {
	shortUploadBound(t)
	f, b := newFakeFiler(), gcBlobs{newFakeBlobs()}
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x41), 1: chunkBytes(0x42)})
	r, err := NewChunkRemote(&stallingFiler{f, path.Join(catBase, "v1", keysDir, "seg")}, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	err = within(t, 5*time.Second, func() error {
		base := storage.ChunkHead{Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder} // no segments: every group is written
		_, _, err := r.PrepareManifest(context.Background(), base, h, 32768)
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled segment bind: %v; want the upload bound's deadline", err)
	}
}

// A chunked record's uploads and the write naming them are bounded the same
// way (an intent written by a background cut).
func TestUploadBound_ChunkedRecordWithoutCallerDeadline(t *testing.T) {
	shortUploadBound(t)
	chunkedRecords(t)
	f, b := newFakeFiler(), newFakeBlobs()
	r, err := NewChunkRemote(&stallingFiler{f, "/swblock/chunks/v1/intents/"}, b, "/swblock/chunks/v1")
	if err != nil {
		t.Fatal(err)
	}
	err = within(t, 5*time.Second, func() error {
		_, err := r.PutIntent(context.Background(), storage.ChunkIntent{Gen: 2, Epoch: 1, Holder: "h1", Keys: []string{"k"}})
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled chunked intent: %v; want the upload bound's deadline", err)
	}
	if b.puts == 0 {
		t.Fatal("the record was not chunked: the control did not exercise an upload")
	}
}

// The bound is the chunk objects' bound.
func TestUploadBound_IsTheObjectBound(t *testing.T) {
	if objectUploadTimeout != storage.ObjectUploadTimeout || storage.ObjectUploadTimeout > time.Minute {
		t.Fatalf("upload bound %v (storage %v)", objectUploadTimeout, storage.ObjectUploadTimeout)
	}
}
