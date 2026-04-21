package memback

import (
	"context"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// volumeStore is a trivial byte slice addressed by offset.
// Grows on Write; Read returns zeros past end-of-store. Shared
// across all backends opened against the same Identity.
type volumeStore struct {
	mu   sync.Mutex
	data []byte
}

func newVolumeStore() *volumeStore { return &volumeStore{} }

// ErrInvalidOffset is returned from Read/Write when the caller
// passes a negative offset. Kept distinct from ErrStalePrimary /
// ErrBackendClosed because it's a caller-input defect, not a
// lineage or lifecycle fault.
var ErrInvalidOffset = errInvalidOffset{}

type errInvalidOffset struct{}

func (errInvalidOffset) Error() string { return "memback: invalid (negative) offset" }

func (s *volumeStore) read(offset int64, p []byte) (int, error) {
	if offset < 0 {
		return 0, ErrInvalidOffset
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset >= int64(len(s.data)) {
		return 0, nil
	}
	return copy(p, s.data[offset:]), nil
}

func (s *volumeStore) write(offset int64, p []byte) (int, error) {
	if offset < 0 {
		return 0, ErrInvalidOffset
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	need := int(offset) + len(p)
	if need > len(s.data) {
		s.data = append(s.data, make([]byte, need-len(s.data))...)
	}
	return copy(s.data[offset:], p), nil
}

// backend is the per-open memback Backend. Holds a frozen
// Identity captured at Open time and re-reads the projection on
// every I/O to fence stale primary (sketch §7).
type backend struct {
	view  frontend.ProjectionView
	id    frontend.Identity
	store *volumeStore

	mu     sync.Mutex
	closed bool
}

func newBackend(view frontend.ProjectionView, id frontend.Identity, store *volumeStore) *backend {
	return &backend{view: view, id: id, store: store}
}

func (b *backend) Identity() frontend.Identity { return b.id }

func (b *backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *backend) Read(_ context.Context, offset int64, p []byte) (int, error) {
	if err := b.guard(); err != nil {
		return 0, err
	}
	return b.store.read(offset, p)
}

func (b *backend) Write(_ context.Context, offset int64, p []byte) (int, error) {
	if err := b.guard(); err != nil {
		return 0, err
	}
	return b.store.write(offset, p)
}

// guard enforces the two per-operation preconditions:
//   - Backend has not been closed.
//   - Every identity field still matches the current projection
//     AND the projection is healthy. ANY drift returns
//     ErrStalePrimary; partial matches do not count.
func (b *backend) guard() error {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return frontend.ErrBackendClosed
	}
	proj := b.view.Projection()
	if !proj.Healthy ||
		proj.VolumeID != b.id.VolumeID ||
		proj.ReplicaID != b.id.ReplicaID ||
		proj.Epoch != b.id.Epoch ||
		proj.EndpointVersion != b.id.EndpointVersion {
		return frontend.ErrStalePrimary
	}
	return nil
}
