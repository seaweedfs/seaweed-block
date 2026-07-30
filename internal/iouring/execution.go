package iouring

import "errors"

var ErrUnsupported = errors.New("io_uring execution is unsupported on this platform")

type operationKind uint8

const (
	operationWrite operationKind = iota + 1
	operationFsync
)

type Operation struct {
	kind     operationKind
	fd       int
	offset   int64
	data     []byte
	userData uint64
}

func Write(fd int, offset int64, data []byte, userData uint64) Operation {
	return Operation{
		kind:     operationWrite,
		fd:       fd,
		offset:   offset,
		data:     data,
		userData: userData,
	}
}

func Fsync(fd int, userData uint64) Operation {
	return Operation{
		kind:     operationFsync,
		fd:       fd,
		userData: userData,
	}
}

type Completion struct {
	UserData uint64
	Result   int32
	Flags    uint32
}

type ExecutionStats struct {
	QueueDepth      uint32
	SubmittedOps    uint64
	SubmitSyscalls  uint64
	CompletionCount uint64
}
