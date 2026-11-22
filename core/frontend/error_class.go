package frontend

import (
	"context"
	"errors"
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

// ClassifyBackendError implements INV-FE-ERROR-RETRY-CLASS-001.
// Call only for a non-nil backend error. A pure expired command deadline is
// transient; a joined physical cause keeps its permanent classification.
func ClassifyBackendError(ctx context.Context, err error) BackendErrorClass {
	// M1 I/O error completion: every cause must be transient before any
	// sentinel can select retry status (INV-FE-ERROR-RETRY-CLASS-001).
	if !backendCausesKnown(ctx, err) {
		return BackendErrorPermanent
	}
	switch {
	case IsWriteDeadline(ctx, err):
		return BackendErrorNotReady
	case errors.Is(err, ErrBackendBusy):
		return BackendErrorBusy
	case errors.Is(err, ErrStalePrimary):
		return BackendErrorStale
	case errors.Is(err, ErrBackendClosed):
		return BackendErrorClosed
	case errors.Is(err, ErrNotReady):
		return BackendErrorNotReady
	default:
		return BackendErrorPermanent
	}
}

// Inspect children before matching sentinels: errors.Is on a joined error can
// match one transient branch while another branch carries a permanent fault.
func backendCausesKnown(ctx context.Context, err error) bool {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !backendCausesKnown(ctx, cause) {
				return false
			}
		}
		return true
	}
	if cause := errors.Unwrap(err); cause != nil {
		return backendCausesKnown(ctx, cause)
	}
	return errors.Is(err, ErrBackendBusy) || errors.Is(err, ErrStalePrimary) ||
		errors.Is(err, ErrBackendClosed) || errors.Is(err, ErrNotReady) ||
		(ctx.Err() == context.DeadlineExceeded && err == context.DeadlineExceeded)
}
