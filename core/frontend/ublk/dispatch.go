package ublk

import (
	"context"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
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
	size    uint64    // device size in bytes
	stats   *counters // nil: not counted
}

// Stats are the device's request counters since it started. A request is
// counted when it is received, before the backend call; Errors counts the
// requests that completed with a negative result.
type Stats struct {
	ReadRequests  uint64 `json:"read_requests"`
	WriteRequests uint64 `json:"write_requests"`
	FlushRequests uint64 `json:"flush_requests"`
	FUAWrites     uint64 `json:"fua_writes"`
	OtherRequests uint64 `json:"other_requests"`
	Errors        uint64 `json:"errors"`
}

type counters struct {
	reads, writes, flushes, fuaWrites, others, errors atomic.Uint64
}

func (c *counters) snapshot() Stats {
	if c == nil {
		return Stats{}
	}
	return Stats{
		ReadRequests: c.reads.Load(), WriteRequests: c.writes.Load(), FlushRequests: c.flushes.Load(),
		FUAWrites: c.fuaWrites.Load(), OtherRequests: c.others.Load(), Errors: c.errors.Load(),
	}
}

func (c *counters) count(r request) {
	if c == nil {
		return
	}
	switch r.op {
	case ioOpRead:
		c.reads.Add(1)
	case ioOpWrite:
		c.writes.Add(1)
		if r.flags&ioFlagFUA != 0 {
			c.fuaWrites.Add(1)
		}
	case ioOpFlush:
		c.flushes.Add(1)
	default:
		c.others.Add(1)
	}
}

// handle serves one request against buf (len >= nrSectors<<9 for reads
// and writes) and returns the ublk result: bytes transferred, 0 for a
// flush, or a negative errno.
func (h handler) handle(ctx context.Context, r request, buf []byte) int32 {
	h.stats.count(r)
	res := h.serve(ctx, r, buf)
	if res < 0 && h.stats != nil {
		h.stats.errors.Add(1)
	}
	return res
}

func (h handler) serve(ctx context.Context, r request, buf []byte) (result int32) {
	var received time.Time
	if r.op == ioOpWrite || r.op == ioOpFlush {
		received = time.Now()
	}

	switch r.op {
	case ioOpFlush:
		ctx, trace := frontend.BeginDurabilityRequest(ctx, h.backend, "flush")
		trace.IncludeReceive(received)
		defer func() { trace.Finish(result < 0) }()
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
	// Only validated writes may open the lazy provider through the timing hook.
	kind := "write"
	if r.flags&ioFlagFUA != 0 {
		kind = "fua"
	}
	ctx, trace := frontend.BeginDurabilityRequest(ctx, h.backend, kind)
	trace.IncludeReceive(received)
	defer func() { trace.Finish(result < 0) }()
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
