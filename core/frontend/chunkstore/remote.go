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
	// ListEntries returns up to limit (0 = the filer's default) children of
	// dir whose names start with prefix and sort after startAfter, in name
	// order. A directory that does not exist lists as empty.
	ListEntries(ctx context.Context, dir, prefix, startAfter string, limit uint32) ([]*filerpb.Entry, error)
	// DeleteEntry removes dir/name without deleting any data, and only if
	// every expected extended key still holds the given value; otherwise
	// ErrPrecondition (also when the entry is missing and expected is not
	// empty). An unconditional delete of a missing entry succeeds.
	DeleteEntry(ctx context.Context, dir, name string, expected map[string][]byte) error
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
	// ErrInvalidKey: a chunk key is malformed, or names an owner that is
	// not a plain volume id, or is qualified where only own keys are allowed.
	ErrInvalidKey = errors.New("chunkstore: invalid chunk key")
)
