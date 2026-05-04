// Ownership: sw unit tests for the shared test doubles.
// Pinned because T2 L0 tests depend on these fakes behaving
// exactly as specified — any drift would silently weaken the
// protocol-layer assertions.
package testback

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

func TestRecordingBackend_WriteThenRead_RoundTrip(t *testing.T) {
	rec := NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1})
	ctx := context.Background()

	if _, err := rec.Write(ctx, 0, []byte("abcdef")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if rec.WriteCount() != 1 {
		t.Fatalf("WriteCount=%d want 1", rec.WriteCount())
	}
	if w := rec.WriteAt(0); w.Offset != 0 || !bytes.Equal(w.Data, []byte("abcdef")) {
		t.Fatalf("WriteAt(0)=%+v", w)
	}

	buf := make([]byte, 6)
	n, err := rec.Read(ctx, 0, buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 6 || !bytes.Equal(buf, []byte("abcdef")) {
		t.Fatalf("Read n=%d buf=%q", n, buf)
	}
	if rec.ReadCount() != 1 {
		t.Fatalf("ReadCount=%d want 1", rec.ReadCount())
	}
}

func TestRecordingBackend_Close_SubsequentReadWriteReturnClosed(t *testing.T) {
	rec := NewRecordingBackend(frontend.Identity{})
	_ = rec.Close()
	ctx := context.Background()
	if _, err := rec.Read(ctx, 0, make([]byte, 4)); !errors.Is(err, frontend.ErrBackendClosed) {
		t.Fatalf("Read post-Close: got %v, want ErrBackendClosed", err)
	}
	if _, err := rec.Write(ctx, 0, []byte("x")); !errors.Is(err, frontend.ErrBackendClosed) {
		t.Fatalf("Write post-Close: got %v, want ErrBackendClosed", err)
	}
}

func TestStaleRejectingBackend_AllOpsReturnStalePrimary(t *testing.T) {
	b := NewStaleRejectingBackend()
	ctx := context.Background()
	if _, err := b.Read(ctx, 0, make([]byte, 4)); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("Read: got %v want ErrStalePrimary", err)
	}
	if _, err := b.Write(ctx, 0, []byte("x")); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("Write: got %v want ErrStalePrimary", err)
	}
	if id := b.Identity(); id.VolumeID == "" {
		t.Fatalf("Identity must be non-zero: %+v", id)
	}
}

func TestStaticProvider_OpenReturnsSuppliedBackend(t *testing.T) {
	rec := NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	prov := NewStaticProvider(rec)
	b, err := prov.Open(context.Background(), "v1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if b != rec {
		t.Fatalf("Open returned %v, want the supplied backend", b)
	}
}
