package frontend

import (
	"context"
	"errors"
	"io"
	"log"
	"reflect"
	"sync"
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

var unclassifiedBackendErrorTypes sync.Map // reflect.Type -> struct{}

// ClassifyBackendError implements INV-FE-ERROR-RETRY-CLASS-001 and V1-SPEC r2.42.
// Call only for a non-nil backend error. Known permanent causes win across the
// entire wrapped/joined tree. Deliberate frontend markers decide otherwise;
// a marker-free unknown error stays permanent and is logged once per leaf type.
func ClassifyBackendError(ctx context.Context, err error) BackendErrorClass {
	// M1 I/O error completion: inspect permanent causes before any retry marker.
	if hasPermanentBackendCause(err) {
		return BackendErrorPermanent
	}
	switch {
	case errors.Is(err, ErrBackendBusy):
		return BackendErrorBusy
	case errors.Is(err, ErrStalePrimary):
		return BackendErrorStale
	case errors.Is(err, ErrBackendClosed):
		return BackendErrorClosed
	case errors.Is(err, ErrNotReady), IsWriteDeadline(ctx, err):
		return BackendErrorNotReady
	default:
		// M1 marker-free completion: amended V1-SPEC r2.42 retains the
		// permanent default. Backend owners must classify types seen in gates.
		logUnclassifiedBackendError(err)
		return BackendErrorPermanent
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

// Wrapper types are not backend error types: inspect each unknown leaf so one
// fmt wrapper cannot suppress the first diagnostic for a different cause type.
func logUnclassifiedBackendError(err error) {
	if joined, ok := err.(interface{ Unwrap() []error }); ok && len(joined.Unwrap()) != 0 {
		for _, cause := range joined.Unwrap() {
			logUnclassifiedBackendError(cause)
		}
		return
	}
	if cause := errors.Unwrap(err); cause != nil {
		logUnclassifiedBackendError(cause)
		return
	}
	if _, logged := unclassifiedBackendErrorTypes.LoadOrStore(reflect.TypeOf(err), struct{}{}); !logged {
		log.Printf("frontend: unclassified backend error type=%T: %v", err, err)
	}
}
