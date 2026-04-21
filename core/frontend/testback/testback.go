// Package testback holds sw-owned test doubles for the T2
// frontend contract work. Used by BOTH core/frontend/iscsi and
// core/frontend/nvme test suites so the three canonical test
// fakes (RecordingBackend, StaleRejectingBackend, StaticProvider)
// don't get duplicated per-protocol.
//
// These are NOT production types. They live in a separate
// package so the boundary guard in core/frontend can still
// refuse any _test.go imports of authority/adapter — testback
// has no such imports by design.
package testback

import (
	"context"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// ---------- StaticProvider: returns a preconfigured backend. ----------

// StaticProvider is a frontend.Provider that returns a single,
// preconfigured backend on every Open. Used by L0 tests that
// want the backend handle directly without wiring a real
// AdapterProjectionView.
type StaticProvider struct{ Backend frontend.Backend }

// NewStaticProvider constructs a provider that hands out b on
// every Open.
func NewStaticProvider(b frontend.Backend) *StaticProvider { return &StaticProvider{Backend: b} }

// Open returns the statically-configured backend. ctx is
// ignored because the backend is already "ready".
func (p *StaticProvider) Open(_ context.Context, _ string) (frontend.Backend, error) {
	return p.Backend, nil
}

// ---------- RecordingBackend: captures every Read/Write call. ----------

// ReadCall records one Read call's arguments.
type ReadCall struct {
	Offset int64
	Length int
}

// WriteCall records one Write call's arguments.
type WriteCall struct {
	Offset int64
	Data   []byte // defensive copy; safe to read after the call
}

// RecordingBackend is a frontend.Backend that stores every
// Read/Write call plus bytes written at each offset. Used by L0
// tests that assert "the protocol target routed N reads/writes
// to the frontend backend with exactly these args".
//
// Holds an in-memory byte slab (like memback) so Read can
// return the most-recent bytes written at that offset.
type RecordingBackend struct {
	mu        sync.Mutex
	id        frontend.Identity
	closed    bool
	reads     []ReadCall
	writes    []WriteCall
	data      []byte
}

// NewRecordingBackend constructs a fresh recording backend at
// the given identity.
func NewRecordingBackend(id frontend.Identity) *RecordingBackend {
	return &RecordingBackend{id: id}
}

func (r *RecordingBackend) Identity() frontend.Identity { return r.id }

func (r *RecordingBackend) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *RecordingBackend) Read(_ context.Context, offset int64, p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, frontend.ErrBackendClosed
	}
	r.reads = append(r.reads, ReadCall{Offset: offset, Length: len(p)})
	if offset < 0 || offset >= int64(len(r.data)) {
		return 0, nil
	}
	return copy(p, r.data[offset:]), nil
}

func (r *RecordingBackend) Write(_ context.Context, offset int64, p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, frontend.ErrBackendClosed
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	r.writes = append(r.writes, WriteCall{Offset: offset, Data: cp})
	if offset < 0 {
		return 0, frontend.ErrStalePrimary // shouldn't be called; caller contract
	}
	need := int(offset) + len(p)
	if need > len(r.data) {
		r.data = append(r.data, make([]byte, need-len(r.data))...)
	}
	return copy(r.data[offset:], p), nil
}

// WriteCount returns the number of Write calls so far.
func (r *RecordingBackend) WriteCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.writes)
}

// ReadCount returns the number of Read calls so far.
func (r *RecordingBackend) ReadCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reads)
}

// WriteAt returns the i-th recorded write, panicking if out of
// range. Matches the pattern QA's L0 spec uses.
func (r *RecordingBackend) WriteAt(i int) WriteCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writes[i]
}

// ReadAt returns the i-th recorded read.
func (r *RecordingBackend) ReadAt(i int) ReadCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads[i]
}

// ---------- StaleRejectingBackend: every op returns ErrStalePrimary. ----------

// StaleRejectingBackend is a frontend.Backend whose Read and
// Write always return frontend.ErrStalePrimary. Used by L0
// tests that assert the protocol target maps ErrStalePrimary
// to the correct protocol failure (SCSI CHECK CONDITION /
// NVMe CQE error status).
//
// Identity is fixed at construction so Identity() still returns
// a stable lineage — the backend exists in the sense of
// "opened", it just refuses every I/O.
type StaleRejectingBackend struct {
	id frontend.Identity
}

// NewStaleRejectingBackend builds one. Identity defaults to a
// non-zero lineage so callers can't accidentally conflate with
// "never opened".
func NewStaleRejectingBackend() *StaleRejectingBackend {
	return &StaleRejectingBackend{
		id: frontend.Identity{
			VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		},
	}
}

// NewStaleRejectingBackendAt builds one with a specific Identity.
func NewStaleRejectingBackendAt(id frontend.Identity) *StaleRejectingBackend {
	return &StaleRejectingBackend{id: id}
}

func (s *StaleRejectingBackend) Identity() frontend.Identity { return s.id }
func (s *StaleRejectingBackend) Close() error                { return nil }

func (s *StaleRejectingBackend) Read(_ context.Context, _ int64, _ []byte) (int, error) {
	return 0, frontend.ErrStalePrimary
}

func (s *StaleRejectingBackend) Write(_ context.Context, _ int64, _ []byte) (int, error) {
	return 0, frontend.ErrStalePrimary
}

// Compile-time checks so type drift shows up early.
var (
	_ frontend.Backend  = (*RecordingBackend)(nil)
	_ frontend.Backend  = (*StaleRejectingBackend)(nil)
	_ frontend.Provider = (*StaticProvider)(nil)
)
