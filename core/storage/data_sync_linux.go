//go:build linux

package storage

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"unsafe"
)

// FIEMAP with SYNC resolves delayed allocation before inspecting the mapping.
// Reject holes, unknown/delayed mappings and unwritten extents; fallocate alone
// does not satisfy the initialized-file experiment's precondition.
func verifyInitializedFile(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	type extent struct {
		Logical, Physical, Length uint64
		Reserved                  [2]uint64
		Flags                     uint32
		Reserved2                 [3]uint32
	}
	type mapping struct {
		Start, Length                  uint64
		Flags, Mapped, Count, Reserved uint32
		Extents                        [128]extent
	}
	end := uint64(info.Size())
	var covered uint64
	for covered < end {
		m := mapping{Start: covered, Length: end - covered, Flags: 1, Count: 128}
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), 0xc020660b, uintptr(unsafe.Pointer(&m)))
		if errno != 0 {
			return fmt.Errorf("storage: FIEMAP: %w", errno)
		}
		if m.Mapped == 0 || m.Mapped > m.Count {
			return fmt.Errorf("storage: uninitialized file at %d", covered)
		}
		for _, e := range m.Extents[:m.Mapped] {
			if e.Logical > covered || e.Length == 0 || e.Logical+e.Length < e.Logical || e.Flags&(2|4|0x800) != 0 {
				return fmt.Errorf("storage: hole or unwritten extent at %d (logical=%d flags=%#x)", covered, e.Logical, e.Flags)
			}
			next := e.Logical + e.Length
			if next <= covered {
				return fmt.Errorf("storage: FIEMAP made no progress")
			}
			covered = next
		}
	}
	return nil
}

func dataSyncSupported() error       { return nil }
func fdatasyncFile(f *os.File) error { return unix.Fdatasync(int(f.Fd())) }
