//go:build linux

package ublk

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// A minimal io_uring for ublk: 128-byte SQEs (IORING_SETUP_SQE128, the
// layout URING_CMD needs for an 80-byte command), 16-byte CQEs, one
// submitter. Only what the ublk server uses: NOP (tests) and URING_CMD.

const (
	setupSQE128     = 1 << 10
	featSingleMMap  = 1 << 0
	offSQRing       = 0
	offCQRing       = 0x8000000
	offSQEs         = 0x10000000
	enterGetEvents  = 1 << 0
	opNop           = 0
	opURingCmd      = 46
	sqeSize         = 128
	cqeSize         = 16
	sqeCmdOffset    = 48 // sqe->cmd: 80 bytes with SQE128
	sqeCmdCapacity  = 80
	sqeCmdOpOffset  = 8
	sqeUserDataOff  = 32
	uringParamsSize = 120
)

type sqOffsets struct {
	Head, Tail, RingMask, RingEntries, Flags, Dropped, Array, Resv1 uint32
	UserAddr                                                        uint64
}

type cqOffsets struct {
	Head, Tail, RingMask, RingEntries, Overflow, Cqes, Flags, Resv1 uint32
	UserAddr                                                        uint64
}

type uringParams struct {
	SqEntries, CqEntries, Flags, SqThreadCPU, SqThreadIdle, Features, WqFd uint32
	Resv                                                                   [3]uint32
	SqOff                                                                  sqOffsets
	CqOff                                                                  cqOffsets
}

type cqe struct {
	UserData uint64
	Res      int32
	Flags    uint32
}

type uring struct {
	fd      int
	ringMem []byte
	cqMem   []byte // == ringMem with a single mmap
	sqeMem  []byte

	sqHead, sqTail, sqMask *uint32
	sqArray                []uint32
	cqHead, cqTail, cqMask *uint32
	cqes                   []byte
	sqEntries              uint32
	pending                uint32 // SQEs filled but not yet submitted

	// enter is io_uring_enter (a test may wrap it): it returns how many
	// SQEs the kernel consumed.
	enter func(fd int, toSubmit, minComplete uint32, flags uintptr) (int, unix.Errno)
}

func sysEnter(fd int, toSubmit, minComplete uint32, flags uintptr) (int, unix.Errno) {
	n, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(fd), uintptr(toSubmit), uintptr(minComplete), flags, 0, 0)
	return int(n), errno
}

func newURing(entries uint32) (*uring, error) {
	var p uringParams
	p.Flags = setupSQE128
	fd, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(&p)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("ublk: io_uring_setup: %w", errno)
	}
	r := &uring{fd: int(fd), sqEntries: p.SqEntries, enter: sysEnter}
	sqSize := int(p.SqOff.Array + p.SqEntries*4)
	cqSize := int(p.CqOff.Cqes + p.CqEntries*cqeSize)
	single := p.Features&featSingleMMap != 0
	if single && cqSize > sqSize {
		sqSize = cqSize
	}
	var err error
	if r.ringMem, err = unix.Mmap(r.fd, offSQRing, sqSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		r.close()
		return nil, fmt.Errorf("ublk: mmap sq ring: %w", err)
	}
	r.cqMem = r.ringMem
	if !single {
		if r.cqMem, err = unix.Mmap(r.fd, offCQRing, cqSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
			r.close()
			return nil, fmt.Errorf("ublk: mmap cq ring: %w", err)
		}
	}
	if r.sqeMem, err = unix.Mmap(r.fd, offSQEs, int(p.SqEntries)*sqeSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE); err != nil {
		r.close()
		return nil, fmt.Errorf("ublk: mmap sqes: %w", err)
	}
	u32 := func(mem []byte, off uint32) *uint32 { return (*uint32)(unsafe.Pointer(&mem[off])) }
	r.sqHead, r.sqTail, r.sqMask = u32(r.ringMem, p.SqOff.Head), u32(r.ringMem, p.SqOff.Tail), u32(r.ringMem, p.SqOff.RingMask)
	r.sqArray = unsafe.Slice(u32(r.ringMem, p.SqOff.Array), p.SqEntries)
	r.cqHead, r.cqTail, r.cqMask = u32(r.cqMem, p.CqOff.Head), u32(r.cqMem, p.CqOff.Tail), u32(r.cqMem, p.CqOff.RingMask)
	r.cqes = r.cqMem[p.CqOff.Cqes : p.CqOff.Cqes+p.CqEntries*cqeSize]
	return r, nil
}

// prepare fills the next SQE: op on fd with cmdOp and an inline command
// (copied into sqe->cmd), tagged userData. It returns an error when the
// submission queue is full.
func (r *uring) prepare(op uint8, fd int, cmdOp uint32, cmd []byte, userData uint64) error {
	if len(cmd) > sqeCmdCapacity {
		return fmt.Errorf("ublk: command of %d bytes exceeds the SQE", len(cmd))
	}
	head := atomic.LoadUint32(r.sqHead)
	tail := *r.sqTail + r.pending
	if tail-head >= r.sqEntries {
		return errors.New("ublk: submission queue full")
	}
	idx := tail & *r.sqMask
	sqe := r.sqeMem[int(idx)*sqeSize : int(idx+1)*sqeSize]
	clear(sqe)
	sqe[0] = op
	*(*int32)(unsafe.Pointer(&sqe[4])) = int32(fd)
	*(*uint32)(unsafe.Pointer(&sqe[sqeCmdOpOffset])) = cmdOp
	*(*uint64)(unsafe.Pointer(&sqe[sqeUserDataOff])) = userData
	copy(sqe[sqeCmdOffset:], cmd)
	r.sqArray[idx] = idx
	r.pending++
	return nil
}

// submitAndWait publishes the prepared SQEs, makes sure the kernel has
// consumed every one of them (a short io_uring_enter leaves the rest in
// the ring: they are offered again), then waits for at least minComplete
// completions. A call that consumes nothing is an error: the unsubmitted
// SQEs would otherwise be reported as submitted.
func (r *uring) submitAndWait(minComplete uint32) error {
	atomic.StoreUint32(r.sqTail, *r.sqTail+r.pending)
	r.pending = 0
	for {
		outstanding := atomic.LoadUint32(r.sqTail) - atomic.LoadUint32(r.sqHead)
		if outstanding == 0 {
			break
		}
		n, errno := r.enter(r.fd, outstanding, 0, 0)
		switch {
		case errno == unix.EINTR:
			continue
		case errno != 0:
			return fmt.Errorf("ublk: io_uring_enter: %w", errno)
		case n == 0 && atomic.LoadUint32(r.sqTail)-atomic.LoadUint32(r.sqHead) == outstanding:
			return fmt.Errorf("ublk: io_uring_enter consumed none of %d SQEs", outstanding)
		}
	}
	if minComplete == 0 {
		return nil
	}
	for {
		_, errno := r.enter(r.fd, 0, minComplete, enterGetEvents)
		switch errno {
		case 0:
			return nil
		case unix.EINTR:
			continue
		default:
			return fmt.Errorf("ublk: io_uring_enter wait: %w", errno)
		}
	}
}

// reap returns the available completions and releases their slots.
func (r *uring) reap(out []cqe) []cqe {
	head := *r.cqHead
	tail := atomic.LoadUint32(r.cqTail)
	for ; head != tail; head++ {
		off := int(head&*r.cqMask) * cqeSize
		c := r.cqes[off : off+cqeSize]
		out = append(out, cqe{
			UserData: *(*uint64)(unsafe.Pointer(&c[0])),
			Res:      *(*int32)(unsafe.Pointer(&c[8])),
			Flags:    *(*uint32)(unsafe.Pointer(&c[12])),
		})
	}
	atomic.StoreUint32(r.cqHead, head)
	return out
}

func (r *uring) close() error {
	if r.sqeMem != nil {
		_ = unix.Munmap(r.sqeMem)
	}
	if r.cqMem != nil && &r.cqMem[0] != &r.ringMem[0] {
		_ = unix.Munmap(r.cqMem)
	}
	if r.ringMem != nil {
		_ = unix.Munmap(r.ringMem)
	}
	r.sqeMem, r.cqMem, r.ringMem = nil, nil, nil
	if r.fd >= 0 {
		err := unix.Close(r.fd)
		r.fd = -1
		return err
	}
	return nil
}
