package ublk

import (
	"context"
	"syscall"
)

// ublk request operations and flags (include/uapi/linux/ublk_cmd.h).
const (
	ioOpRead        = 0
	ioOpWrite       = 1
	ioOpFlush       = 2
	ioOpDiscard     = 3
	ioOpWriteSame   = 4
	ioOpWriteZeroes = 5

	ioFlagFUA = 1 << 13

	sectorShift = 9
)

// Backend is the block contract the ublk bridge needs.
type Backend interface {
	Read(context.Context, int64, []byte) (int, error)
	Write(context.Context, int64, []byte) (int, error)
	Sync(context.Context) error
}

// request is one ublk I/O descriptor: op and flags from op_flags, the
// range in 512-byte sectors.
type request struct {
	op          uint8
	flags       uint32 // op_flags >> 8 shifted back: the raw flag bits
	startSector uint64
	nrSectors   uint32
}

// handler maps ublk requests onto a Backend.
type handler struct {
	backend Backend
	size    uint64 // device size in bytes
}

// handle serves one request against buf (len >= nrSectors<<9 for reads
// and writes) and returns the ublk result: bytes transferred, 0 for a
// flush, or a negative errno.
func (h handler) handle(ctx context.Context, r request, buf []byte) int32 {
	switch r.op {
	case ioOpFlush:
		if err := h.backend.Sync(ctx); err != nil {
			return errno(syscall.EIO)
		}
		return 0
	case ioOpRead, ioOpWrite:
	default:
		return errno(syscall.EOPNOTSUPP)
	}

	length := uint64(r.nrSectors) << sectorShift
	if r.startSector > h.size>>sectorShift {
		return errno(syscall.EINVAL)
	}
	offset := r.startSector << sectorShift
	if length > h.size-offset || length > uint64(len(buf)) || length > 1<<31-1 {
		return errno(syscall.EINVAL)
	}
	data := buf[:length]

	if r.op == ioOpRead {
		n, err := h.backend.Read(ctx, int64(offset), data)
		if err != nil || n != len(data) {
			return errno(syscall.EIO)
		}
		return int32(n)
	}
	n, err := h.backend.Write(ctx, int64(offset), data)
	if err != nil || n != len(data) {
		return errno(syscall.EIO)
	}
	if r.flags&ioFlagFUA != 0 {
		if err := h.backend.Sync(ctx); err != nil {
			return errno(syscall.EIO)
		}
	}
	return int32(n)
}

func errno(e syscall.Errno) int32 { return -int32(e) }
