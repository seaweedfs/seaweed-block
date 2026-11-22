package frontend

import (
	"context"
	"errors"
	"io"
	"log"
	"sync/atomic"
	"syscall"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// BackendErrorClass is independent of transport status (INV-FE-ERROR-RETRY-CLASS-001).
type BackendErrorClass uint8

const (
	BackendErrorPermanent BackendErrorClass = iota
	BackendErrorNotReady
	BackendErrorBusy
	BackendErrorStale
	BackendErrorClosed
)

var unclassifiedBackendErrors atomic.Uint64

// UnclassifiedBackendErrorCount reports completions using the unknown-cause
// fallback in V1-SPEC r2.42. It is cumulative for this process.
func UnclassifiedBackendErrorCount() uint64 { return unclassifiedBackendErrors.Load() }

// ClassifyBackendError implements INV-FE-ERROR-RETRY-CLASS-001 and V1-SPEC r2.42.
// Call only for a non-nil backend error. Known permanent causes win across the
// entire wrapped/joined tree; unknown causes are counted and retryable.
func ClassifyBackendError(ctx context.Context, err error) BackendErrorClass {
	// M1 I/O error completion: inspect permanent causes before any retry marker.
	if hasPermanentBackendCause(err) {
		return BackendErrorPermanent
	}
	switch {
	case errors.Is(err, ErrBackendBusy), errors.Is(err, storage.ErrWALPressureTimeout),
		errors.Is(err, storage.ErrChunkHydrationPending),
		errors.Is(err, ErrReplicationAckUnavailable) && errors.Is(err, context.DeadlineExceeded):
		return BackendErrorBusy
	case errors.Is(err, ErrStalePrimary):
		return BackendErrorStale
	case errors.Is(err, ErrBackendClosed):
		return BackendErrorClosed
	case errors.Is(err, ErrNotReady), IsWriteDeadline(ctx, err):
		return BackendErrorNotReady
	default:
		// M1 unknown completion: V1-SPEC r2.42 bounds retry at the initiator.
		count := unclassifiedBackendErrors.Add(1)
		log.Printf("frontend: unclassified backend error count=%d: %v", count, err)
		return BackendErrorNotReady
	}
}

func hasPermanentBackendCause(err error) bool {
	// errors.Is traverses every child of joins, so neither an outer transient
	// wrapper nor a sibling deadline can hide a typed storage/media failure.
	for _, cause := range []error{syscall.EIO, syscall.ENOSPC, syscall.EROFS,
		io.ErrUnexpectedEOF, storage.ErrWALIntegrityFault, storage.ErrFsyncFailed,
		storage.ErrAppendFailed, storage.ErrMapInvalid} {
		if errors.Is(err, cause) {
			return true
		}
	}
	return false
}
