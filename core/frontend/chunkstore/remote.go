package chunkstore

import (
	"context"
	"errors"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// Filer is the metadata side chunkstore needs from a SeaweedFS filer: one
// entry per volume HEAD (and per snapshot), written with a precondition.
//
// The two SeaweedFS failure styles are normalised here:
//   - CreateEntry reports a failed o_excl / condition in the response's
//     ErrorCode with a nil gRPC error;
//   - UpdateEntry reports a failed expected_extended as gRPC FailedPrecondition.
//
// Both become ErrExists / ErrPrecondition so callers branch on one thing.
type Filer interface {
	// LookupEntry returns ErrNotFound when dir/name does not exist.
	LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error)
	// CreateEntry with oExcl returns ErrExists when dir/name already exists.
	CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, oExcl bool) error
	// UpdateEntry applies e only if every expected extended key still holds
	// the given value; otherwise ErrPrecondition. ErrNotFound if absent.
	UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error
}

// Blobs stores immutable chunk payloads as SeaweedFS needles.
type Blobs interface {
	// Put stores data as a new needle and returns its fid. A fid is never
	// reused, so a Put can never overwrite data another manifest references.
	Put(ctx context.Context, data []byte) (fid string, err error)
	// Get returns the whole needle payload.
	Get(ctx context.Context, fid string) ([]byte, error)
}

var (
	// ErrNotFound: the filer entry does not exist.
	ErrNotFound = errors.New("chunkstore: entry not found")
	// ErrExists: an exclusive create found the entry already there.
	ErrExists = errors.New("chunkstore: entry already exists")
	// ErrPrecondition: a conditional write lost (HEAD changed under us).
	ErrPrecondition = errors.New("chunkstore: precondition failed")
)
