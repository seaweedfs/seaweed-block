// Package frontend is the T1 V3 frontend contract boundary
// (sketch: sw-block/design/v3-phase-15-t1-sketch.md §5).
//
// The frontend consumes adapter/engine projection through a
// read-only ProjectionView. It never mints authority, never
// selects primary, never imports core/authority. This boundary
// is AST-guarded by boundary_guard_test.go.
package frontend

import "context"

// Identity is the full authority lineage a backend was opened
// against. Every Read/Write re-validates identity against the
// current projection — see sketch §7 stale-primary fence point.
type Identity struct {
	VolumeID        string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
}

// Projection is the frontend-facing view of adapter/engine
// readiness plus the identity fields required to stamp a backend.
//
// Per sketch §5 note, the imported projection type may be
// adjusted to match the current adapter/engine API. This struct
// is that adjustment: engine.ReplicaProjection does not carry
// VolumeID / ReplicaID (those live in adapter.AssignmentInfo),
// so the frontend bundles the pair volume hosts already know
// about for their own replica with the engine.ReplicaProjection
// Mode into a single value that ProjectionView.Projection()
// returns.
type Projection struct {
	VolumeID        string
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	Healthy         bool
}

// ProjectionView is the read-only seam between adapter/engine
// state and the frontend. Implementations must not expose
// publisher, authority, directive, or assignment-minting
// methods; this interface is the ONLY channel the frontend is
// allowed to consume.
type ProjectionView interface {
	Projection() Projection
}

// Backend is a per-lineage handle for one open frontend session.
//
// Lifecycle ownership (BUG-005 2026-04-22):
//   Backend.Close() is OWNED BY THE PROVIDER. Session-layer
//   consumers (iSCSI session, NVMe controller, etc.) MUST NOT
//   call Close on a Backend obtained from Provider.Open —
//   treat the returned Backend as a BORROWED reference for the
//   duration of the session. DurableProvider caches one Backend
//   per volumeID; a session-level Close would mark the cached
//   Backend closed and break every subsequent session. Session
//   teardown should release conn + session-local state only.
//   Provider.Close tears down all Backends in correct order.
//
// T3a extends the interface with two methods (all existing
// implementations gain trivial impls in the same commit):
//
//   - Sync: frontends emit this on SYNCHRONIZE CACHE / Flush.
//     For durable backends it calls LogicalStorage.Sync; for
//     in-memory backends it is a no-op returning nil.
//
//   - SetOperational: lifecycle gate controlled by the host /
//     provider. Before SetOperational(true, _) is called, every
//     I/O MUST return ErrNotReady (see INV-DURABLE-OPGATE-001).
//     This is local-readiness only — SetOperational does NOT
//     touch identity/lineage/epoch; authority publication stays
//     on the master path (PCDD-STUFFING-001).
type Backend interface {
	Read(ctx context.Context, offset int64, p []byte) (int, error)
	Write(ctx context.Context, offset int64, p []byte) (int, error)
	Sync(ctx context.Context) error
	SetOperational(ok bool, evidence string)
	Identity() Identity
	Close() error
}

// Provider opens backends against a volume by consulting a
// ProjectionView. Open blocks until the projection becomes
// healthy or ctx expires.
type Provider interface {
	Open(ctx context.Context, volumeID string) (Backend, error)
}
