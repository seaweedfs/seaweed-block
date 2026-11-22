package frontend

import (
	"context"
	"errors"
)

// BackendErrorClass is independent of the transport's status codes.
type BackendErrorClass uint8

const (
	BackendErrorPermanent BackendErrorClass = iota
	BackendErrorNotReady
	BackendErrorBusy
	BackendErrorStale
	BackendErrorClosed
)

// ClassifyBackendError implements the approved r2.36 frontend retry-class rule.
// Call only for a non-nil backend error. A pure expired command deadline is
// transient; a joined physical cause keeps its permanent classification.
func ClassifyBackendError(ctx context.Context, err error) BackendErrorClass {
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
