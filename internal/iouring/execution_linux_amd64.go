//go:build linux && amd64

package iouring

import (
	"errors"
	"fmt"
	"sync"
)

type Executor struct {
	mu     sync.Mutex
	ring   *ioUring
	closed bool
	stats  ExecutionStats
}

func New(depth uint32) (*Executor, error) {
	ring, err := newIOUring(depth)
	if err != nil {
		return nil, fmt.Errorf("%w: setup: %v", ErrUnsupported, err)
	}
	supported, err := ring.supportedOpcodes()
	if err != nil {
		ring.close()
		return nil, fmt.Errorf("%w: opcode probe: %v", ErrUnsupported, err)
	}
	if !supported[ioUringOpWrite] || !supported[ioUringOpFsync] {
		ring.close()
		return nil, fmt.Errorf(
			"%w: required opcodes write=%t fsync=%t",
			ErrUnsupported,
			supported[ioUringOpWrite],
			supported[ioUringOpFsync],
		)
	}
	return &Executor{
		ring: ring,
		stats: ExecutionStats{
			QueueDepth: *ring.sqEntries,
		},
	}, nil
}

func (executor *Executor) SubmitAndWait(operations []Operation) ([]Completion, error) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.closed {
		return nil, errors.New("io_uring executor is closed")
	}

	raw := make([]ringOperation, len(operations))
	for i, operation := range operations {
		var opcode uint8
		switch operation.kind {
		case operationWrite:
			if len(operation.data) == 0 {
				return nil, fmt.Errorf("write operation %d has an empty buffer", i)
			}
			opcode = ioUringOpWrite
		case operationFsync:
			if len(operation.data) != 0 {
				return nil, fmt.Errorf("fsync operation %d unexpectedly owns data", i)
			}
			opcode = ioUringOpFsync
		default:
			return nil, fmt.Errorf("operation %d has unknown kind %d", i, operation.kind)
		}
		raw[i] = ringOperation{
			opcode:   opcode,
			fd:       int32(operation.fd),
			offset:   uint64(operation.offset),
			data:     operation.data,
			userData: operation.userData,
		}
	}

	beforeSyscalls := executor.ring.submitSyscalls
	cqes, submitted, err := executor.ring.submitAndWait(raw)
	executor.stats.SubmittedOps += uint64(submitted)
	executor.stats.SubmitSyscalls += uint64(executor.ring.submitSyscalls - beforeSyscalls)
	executor.stats.CompletionCount += uint64(len(cqes))

	completions := make([]Completion, len(cqes))
	for i, cqe := range cqes {
		completions[i] = Completion{
			UserData: cqe.UserData,
			Result:   cqe.Result,
			Flags:    cqe.Flags,
		}
	}
	return completions, err
}

func (executor *Executor) Stats() ExecutionStats {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.stats
}

func (executor *Executor) Close() error {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.closed {
		return nil
	}
	executor.closed = true
	executor.ring.close()
	return nil
}
