//go:build linux && amd64

package iouring

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	ioUringOffSQRing = 0
	ioUringOffCQRing = 0x08000000
	ioUringOffSQEs   = 0x10000000

	ioUringFeatSingleMmap  = 1 << 0
	ioUringEnterGetEvents  = 1 << 0
	ioUringRegisterEventFD = 4
	ioUringRegisterProbe   = 8
	ioUringOpSupported     = 1 << 0

	ioUringOpFsync = 3
	ioUringOpWrite = 23

	probeOpcodeCount = 256
	probeBlockSize   = 4096
)

type ioSqringOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Reserved    uint32
	UserAddr    uint64
}

type ioCqringOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	CQEs        uint32
	Flags       uint32
	Reserved    uint32
	UserAddr    uint64
}

type ioUringParams struct {
	SQEntries    uint32
	CQEntries    uint32
	Flags        uint32
	SQThreadCPU  uint32
	SQThreadIdle uint32
	Features     uint32
	WQFD         uint32
	Reserved     [3]uint32
	SQOff        ioSqringOffsets
	CQOff        ioCqringOffsets
}

type ioUringSQE struct {
	Opcode      uint8
	Flags       uint8
	IOPriority  uint16
	FD          int32
	Offset      uint64
	Address     uint64
	Length      uint32
	Operation   uint32
	UserData    uint64
	BufferIndex uint16
	Personality uint16
	SpliceFDIn  int32
	Address3    uint64
	Pad         uint64
}

type ioUringCQE struct {
	UserData uint64
	Result   int32
	Flags    uint32
}

type ringOperation struct {
	opcode   uint8
	fd       int32
	offset   uint64
	data     []byte
	userData uint64
}

type ioUring struct {
	fd      int
	eventFD int

	sqRing []byte
	cqRing []byte
	sqes   []byte

	singleMmap bool

	sqHead    *uint32
	sqTail    *uint32
	sqMask    *uint32
	sqEntries *uint32
	sqArray   []uint32
	sqeArray  []ioUringSQE

	cqHead *uint32
	cqTail *uint32
	cqMask *uint32
	cqes   []ioUringCQE

	submitSyscalls int
	enterCall      func(toSubmit, minComplete, flags uint32) (int, error)
}

func RunProbe(requestedDepth uint32) (Report, error) {
	report := Report{
		Platform:      runtime.GOOS + "/" + runtime.GOARCH,
		KernelRelease: readTrimmed("/proc/sys/kernel/osrelease"),
	}
	if requestedDepth < 4 {
		return report, errors.New("requested queue depth must be at least 4")
	}

	ring, err := newIOUring(requestedDepth)
	if err != nil {
		report.RefusalReason = refusalReason(err)
		return report, fmt.Errorf("io_uring setup: %w", err)
	}
	defer ring.close()

	report.Supported = true
	report.QueueDepth = atomic.LoadUint32(ring.sqEntries)

	supported, err := ring.supportedOpcodes()
	if err != nil {
		report.RefusalReason = refusalReason(err)
		return report, fmt.Errorf("io_uring opcode probe: %w", err)
	}
	report.WriteOpcodeSupported = supported[ioUringOpWrite]
	report.FsyncOpcodeSupported = supported[ioUringOpFsync]
	if !report.WriteOpcodeSupported || !report.FsyncOpcodeSupported {
		report.RefusalReason = "required_opcode_missing"
		return report, fmt.Errorf(
			"required opcodes unavailable: write=%t fsync=%t",
			report.WriteOpcodeSupported,
			report.FsyncOpcodeSupported,
		)
	}

	dir, err := os.MkdirTemp("", "seaweed-block-iouring-probe-")
	if err != nil {
		return report, fmt.Errorf("create temp directory: %w", err)
	}
	defer os.RemoveAll(dir)

	path := dir + "/probe.dat"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return report, fmt.Errorf("create probe file: %w", err)
	}
	fileOpen := true
	defer func() {
		if fileOpen {
			_ = file.Close()
		}
	}()
	if err := file.Truncate(5 * probeBlockSize); err != nil {
		return report, fmt.Errorf("truncate probe file: %w", err)
	}

	payloads := [][]byte{
		bytes.Repeat([]byte{0x31}, probeBlockSize),
		bytes.Repeat([]byte{0x72}, probeBlockSize),
		bytes.Repeat([]byte{0xa5}, probeBlockSize),
	}
	offsets := []int64{0, 3 * probeBlockSize, probeBlockSize}
	operations := make([]ringOperation, 0, len(payloads))
	for i := range payloads {
		operations = append(operations, ringOperation{
			opcode:   ioUringOpWrite,
			fd:       int32(file.Fd()),
			offset:   uint64(offsets[i]),
			data:     payloads[i],
			userData: uint64(i + 1),
		})
	}

	writeCQEs, submitted, err := ring.submitAndWait(operations)
	runtime.KeepAlive(payloads)
	report.SubmittedOps += submitted
	report.WriteCompletions = len(writeCQEs)
	report.CompletionCount += len(writeCQEs)
	if err != nil {
		report.SubmitSyscalls = ring.submitSyscalls
		return report, fmt.Errorf("write round: %w", err)
	}
	seenWrites := make(map[uint64]bool, len(writeCQEs))
	for i, cqe := range writeCQEs {
		if cqe.Result != probeBlockSize {
			report.SubmitSyscalls = ring.submitSyscalls
			return report, fmt.Errorf(
				"write completion %d user_data=%d result=%d want=%d",
				i,
				cqe.UserData,
				cqe.Result,
				probeBlockSize,
			)
		}
		if cqe.UserData < 1 || cqe.UserData > uint64(len(payloads)) || seenWrites[cqe.UserData] {
			return report, fmt.Errorf("unexpected write completion user_data=%d", cqe.UserData)
		}
		seenWrites[cqe.UserData] = true
	}

	fsyncCQEs, submitted, err := ring.submitAndWait([]ringOperation{{
		opcode:   ioUringOpFsync,
		fd:       int32(file.Fd()),
		userData: 1000,
	}})
	report.SubmittedOps += submitted
	report.FsyncCompletions = len(fsyncCQEs)
	report.CompletionCount += len(fsyncCQEs)
	report.SubmitSyscalls = ring.submitSyscalls
	if err != nil {
		return report, fmt.Errorf("fsync round: %w", err)
	}
	if len(fsyncCQEs) != 1 || fsyncCQEs[0].UserData != 1000 || fsyncCQEs[0].Result != 0 {
		return report, fmt.Errorf("fsync completion=%+v want one successful completion", fsyncCQEs)
	}

	if err := file.Close(); err != nil {
		return report, fmt.Errorf("close probe file: %w", err)
	}
	fileOpen = false

	reopened, err := os.Open(path)
	if err != nil {
		return report, fmt.Errorf("reopen probe file: %w", err)
	}
	defer reopened.Close()
	for i := range payloads {
		got := make([]byte, len(payloads[i]))
		if _, err := reopened.ReadAt(got, offsets[i]); err != nil {
			return report, fmt.Errorf("read back offset %d: %w", offsets[i], err)
		}
		if !bytes.Equal(got, payloads[i]) {
			return report, fmt.Errorf("read back mismatch at offset %d", offsets[i])
		}
		report.VerifiedBytes += len(got)
	}
	return report, nil
}

func newIOUring(entries uint32) (_ *ioUring, retErr error) {
	var params ioUringParams
	fd, _, errno := unix.Syscall(
		unix.SYS_IO_URING_SETUP,
		uintptr(entries),
		uintptr(unsafe.Pointer(&params)),
		0,
	)
	runtime.KeepAlive(&params)
	if errno != 0 {
		return nil, errno
	}

	ring := &ioUring{fd: int(fd), eventFD: -1}
	defer func() {
		if retErr != nil {
			ring.close()
		}
	}()
	ring.eventFD, retErr = unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if retErr != nil {
		return nil, fmt.Errorf("eventfd: %w", retErr)
	}
	eventFD := int32(ring.eventFD)
	_, _, errno = unix.Syscall6(
		unix.SYS_IO_URING_REGISTER,
		uintptr(ring.fd),
		ioUringRegisterEventFD,
		uintptr(unsafe.Pointer(&eventFD)),
		1,
		0,
		0,
	)
	runtime.KeepAlive(&eventFD)
	if errno != 0 {
		return nil, fmt.Errorf("register eventfd: %w", errno)
	}

	sqRingSize := int(params.SQOff.Array + params.SQEntries*uint32(unsafe.Sizeof(uint32(0))))
	cqRingSize := int(params.CQOff.CQEs + params.CQEntries*uint32(unsafe.Sizeof(ioUringCQE{})))
	if params.Features&ioUringFeatSingleMmap != 0 {
		ring.singleMmap = true
		if cqRingSize > sqRingSize {
			sqRingSize = cqRingSize
		}
	}

	ring.sqRing, retErr = unix.Mmap(
		ring.fd,
		ioUringOffSQRing,
		sqRingSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if retErr != nil {
		return nil, fmt.Errorf("mmap SQ ring: %w", retErr)
	}
	if ring.singleMmap {
		ring.cqRing = ring.sqRing
	} else {
		ring.cqRing, retErr = unix.Mmap(
			ring.fd,
			ioUringOffCQRing,
			cqRingSize,
			unix.PROT_READ|unix.PROT_WRITE,
			unix.MAP_SHARED,
		)
		if retErr != nil {
			return nil, fmt.Errorf("mmap CQ ring: %w", retErr)
		}
	}
	ring.sqes, retErr = unix.Mmap(
		ring.fd,
		ioUringOffSQEs,
		int(params.SQEntries)*int(unsafe.Sizeof(ioUringSQE{})),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if retErr != nil {
		return nil, fmt.Errorf("mmap SQEs: %w", retErr)
	}

	ring.sqHead = uint32At(ring.sqRing, params.SQOff.Head)
	ring.sqTail = uint32At(ring.sqRing, params.SQOff.Tail)
	ring.sqMask = uint32At(ring.sqRing, params.SQOff.RingMask)
	ring.sqEntries = uint32At(ring.sqRing, params.SQOff.RingEntries)
	ring.sqArray = uint32SliceAt(ring.sqRing, params.SQOff.Array, params.SQEntries)
	ring.sqeArray = sqeSlice(ring.sqes, params.SQEntries)
	ring.cqHead = uint32At(ring.cqRing, params.CQOff.Head)
	ring.cqTail = uint32At(ring.cqRing, params.CQOff.Tail)
	ring.cqMask = uint32At(ring.cqRing, params.CQOff.RingMask)
	ring.cqes = cqeSlice(ring.cqRing, params.CQOff.CQEs, params.CQEntries)
	return ring, nil
}

func (ring *ioUring) close() {
	if ring.sqes != nil {
		_ = unix.Munmap(ring.sqes)
		ring.sqes = nil
	}
	if ring.cqRing != nil && !ring.singleMmap {
		_ = unix.Munmap(ring.cqRing)
		ring.cqRing = nil
	}
	if ring.sqRing != nil {
		_ = unix.Munmap(ring.sqRing)
		ring.sqRing = nil
	}
	if ring.fd >= 0 {
		_ = unix.Close(ring.fd)
		ring.fd = -1
	}
	if ring.eventFD >= 0 {
		_ = unix.Close(ring.eventFD)
		ring.eventFD = -1
	}
}

func (ring *ioUring) supportedOpcodes() (map[uint8]bool, error) {
	const probeHeaderSize = 16
	const probeOperationSize = 8
	buffer := make([]byte, probeHeaderSize+probeOpcodeCount*probeOperationSize)
	_, _, errno := unix.Syscall6(
		unix.SYS_IO_URING_REGISTER,
		uintptr(ring.fd),
		ioUringRegisterProbe,
		uintptr(unsafe.Pointer(&buffer[0])),
		probeOpcodeCount,
		0,
		0,
	)
	runtime.KeepAlive(buffer)
	if errno != 0 {
		return nil, errno
	}

	count := int(buffer[1])
	if count > probeOpcodeCount {
		count = probeOpcodeCount
	}
	supported := make(map[uint8]bool, count)
	for i := 0; i < count; i++ {
		offset := probeHeaderSize + i*probeOperationSize
		opcode := buffer[offset]
		flags := uint16(buffer[offset+2]) | uint16(buffer[offset+3])<<8
		if flags&ioUringOpSupported != 0 {
			supported[opcode] = true
		}
	}
	return supported, nil
}

func (ring *ioUring) submitAndWait(operations []ringOperation) ([]ioUringCQE, int, error) {
	if len(operations) == 0 {
		return nil, 0, errors.New("empty submission")
	}
	if len(operations) > int(atomic.LoadUint32(ring.sqEntries)) {
		return nil, 0, fmt.Errorf(
			"submission size %d exceeds queue depth %d",
			len(operations),
			atomic.LoadUint32(ring.sqEntries),
		)
	}

	for _, operation := range operations {
		if err := ring.enqueue(operation); err != nil {
			return nil, 0, err
		}
	}

	submissionHead := atomic.LoadUint32(ring.sqHead)
	submitted := 0
	for submitted < len(operations) {
		before := submitted
		_, err := ring.enter(uint32(len(operations)-submitted), 0, 0)
		submitted = int(atomic.LoadUint32(ring.sqHead) - submissionHead)
		if submitted > len(operations) {
			return nil, 0, fmt.Errorf(
				"submission head advanced by %d want at most %d",
				submitted,
				len(operations),
			)
		}
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if submitted == 0 {
				return nil, 0, err
			}
			completions := ring.waitForAccepted(submitted)
			return completions, submitted, err
		}
		if submitted == before {
			if submitted == 0 {
				return nil, 0, errors.New("io_uring_enter submitted zero operations")
			}
			completions := ring.waitForAccepted(submitted)
			return completions, submitted, errors.New("io_uring_enter submitted zero operations")
		}
	}

	completions := ring.waitForAccepted(len(operations))
	if len(completions) != len(operations) {
		return completions, submitted, fmt.Errorf(
			"completion count=%d want=%d",
			len(completions),
			len(operations),
		)
	}
	return completions, submitted, nil
}

// waitForAccepted does not return until every accepted SQE has a terminal CQE.
// The registered eventfd separates completion wakeup from io_uring_enter, so a
// GETEVENTS syscall failure cannot strand accepted buffers.
func (ring *ioUring) waitForAccepted(expected int) []ioUringCQE {
	completions := make([]ioUringCQE, 0, expected)
	for len(completions) < expected {
		completions = append(completions, ring.drainCompletions()...)
		if len(completions) >= expected {
			ring.consumeCompletionEvent()
			break
		}
		if err := ring.waitCompletionEvent(); err != nil {
			continue
		}
	}
	return completions
}

func (ring *ioUring) waitCompletionEvent() error {
	pollFDs := []unix.PollFd{{
		Fd:     int32(ring.eventFD),
		Events: unix.POLLIN,
	}}
	for {
		_, err := unix.Poll(pollFDs, -1)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if pollFDs[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			return fmt.Errorf("eventfd poll revents=%d", pollFDs[0].Revents)
		}
		if pollFDs[0].Revents&unix.POLLIN != 0 {
			ring.consumeCompletionEvent()
			return nil
		}
	}
}

func (ring *ioUring) consumeCompletionEvent() {
	var counter [8]byte
	_, _ = unix.Read(ring.eventFD, counter[:])
}

func (ring *ioUring) enqueue(operation ringOperation) error {
	head := atomic.LoadUint32(ring.sqHead)
	tail := atomic.LoadUint32(ring.sqTail)
	entries := atomic.LoadUint32(ring.sqEntries)
	if tail-head >= entries {
		return errors.New("submission queue full")
	}
	index := tail & atomic.LoadUint32(ring.sqMask)
	sqe := &ring.sqeArray[index]
	*sqe = ioUringSQE{
		Opcode:   operation.opcode,
		FD:       operation.fd,
		Offset:   operation.offset,
		UserData: operation.userData,
	}
	if len(operation.data) > 0 {
		sqe.Address = uint64(uintptr(unsafe.Pointer(&operation.data[0])))
		sqe.Length = uint32(len(operation.data))
	}
	ring.sqArray[index] = index
	atomic.StoreUint32(ring.sqTail, tail+1)
	return nil
}

func (ring *ioUring) enter(toSubmit, minComplete, flags uint32) (int, error) {
	for {
		ring.submitSyscalls++
		if ring.enterCall != nil {
			count, err := ring.enterCall(toSubmit, minComplete, flags)
			if toSubmit == 0 && errors.Is(err, unix.EINTR) {
				continue
			}
			return count, err
		}
		count, _, errno := unix.Syscall6(
			unix.SYS_IO_URING_ENTER,
			uintptr(ring.fd),
			uintptr(toSubmit),
			uintptr(minComplete),
			uintptr(flags),
			0,
			0,
		)
		if toSubmit == 0 && errno == unix.EINTR {
			continue
		}
		if errno != 0 {
			return int(count), errno
		}
		return int(count), nil
	}
}

func (ring *ioUring) drainCompletions() []ioUringCQE {
	head := atomic.LoadUint32(ring.cqHead)
	tail := atomic.LoadUint32(ring.cqTail)
	if head == tail {
		return nil
	}
	completions := make([]ioUringCQE, 0, tail-head)
	mask := atomic.LoadUint32(ring.cqMask)
	for head != tail {
		completions = append(completions, ring.cqes[head&mask])
		head++
	}
	atomic.StoreUint32(ring.cqHead, head)
	return completions
}

func uint32At(buffer []byte, offset uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&buffer[int(offset)]))
}

func uint32SliceAt(buffer []byte, offset, count uint32) []uint32 {
	return unsafe.Slice((*uint32)(unsafe.Pointer(&buffer[int(offset)])), int(count))
}

func sqeSlice(buffer []byte, count uint32) []ioUringSQE {
	return unsafe.Slice((*ioUringSQE)(unsafe.Pointer(&buffer[0])), int(count))
}

func cqeSlice(buffer []byte, offset, count uint32) []ioUringCQE {
	return unsafe.Slice((*ioUringCQE)(unsafe.Pointer(&buffer[int(offset)])), int(count))
}

func readTrimmed(path string) string {
	value, err := os.ReadFile(path)
	if err != nil {
		return "-"
	}
	return strings.TrimSpace(string(value))
}

func refusalReason(err error) string {
	var errno unix.Errno
	if errors.As(err, &errno) {
		return "errno_" + strconv.Itoa(int(errno))
	}
	return oneLine(err.Error())
}
