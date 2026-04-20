//go:build windows

package authority

import (
	"fmt"
	"os"
	"syscall"
)

// Windows error codes (stdlib does not export these by name in
// all Go versions; values are stable per Win32 API docs).
const (
	errorSharingViolation syscall.Errno = 32
	errorAccessDenied     syscall.Errno = 5
)

// acquireOSLock opens (or creates) the lockfile with EXCLUSIVE
// share mode (dwShareMode=0). Another process attempting to
// open the same file gets ERROR_SHARING_VIOLATION, which we
// map to ErrStoreLockHeld. When our process exits for any
// reason, Windows closes the handle and drops the exclusivity,
// so the next startup can acquire cleanly without manual
// intervention. That is the crash-safe single-owner semantic
// the S5 sketch requires.
//
// We use syscall.CreateFile rather than syscall.LockFileEx
// because the LockFileEx binding is not in the stdlib across
// all supported Go versions; exclusive share mode achieves the
// same single-owner property without any external dependency.
func acquireOSLock(path string) (*os.File, error) {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, fmt.Errorf("encode lock path: %w", err)
	}
	handle, err := syscall.CreateFile(
		pathPtr,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0,                            // dwShareMode = 0 (exclusive)
		nil,                          // security attributes
		syscall.OPEN_ALWAYS,          // create if not present, open if present
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,                            // template handle
	)
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			if errno == errorSharingViolation || errno == errorAccessDenied {
				return nil, ErrStoreLockHeld
			}
		}
		return nil, fmt.Errorf("CreateFile exclusive: %w", err)
	}
	return os.NewFile(uintptr(handle), path), nil
}
