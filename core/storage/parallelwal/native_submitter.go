package parallelwal

import (
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/seaweedfs/seaweed-block/internal/iouring"
)

type NativeIOStats struct {
	Enabled           bool
	QueueDepth        uint32
	SubmissionRounds  uint64
	SQEs              uint64
	SubmitSyscalls    uint64
	CompletionCount   uint64
	ShortCompletions  uint64
	QueueFullRejects  uint64
	InflightHighWater uint64
	FallbackCount     uint64
}

type nativeWALBatch struct {
	lane     *lane
	requests []*writeRequest
	buffer   []byte
	offset   int64
}

type nativeWALSubmitter struct {
	store     *Store
	executor  *iouring.Executor
	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once

	mu    sync.Mutex
	stats NativeIOStats
}

func (s *Store) attachExecution(mode ExecutionMode, queueDepth int) error {
	if mode == ExecutionPositioned {
		return nil
	}
	if mode != ExecutionIOUring {
		return fmt.Errorf("parallelwal: execution mode %q invalid", mode)
	}
	depth := queueDepth
	if depth > s.laneCount {
		depth = s.laneCount
	}
	if depth < 1 {
		depth = 1
	}
	executor, err := iouring.New(uint32(depth))
	if err != nil {
		return fmt.Errorf("parallelwal: io_uring execution: %w", err)
	}
	submitter := &nativeWALSubmitter{
		store:    s,
		executor: executor,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		stats: NativeIOStats{
			Enabled:    true,
			QueueDepth: executor.Stats().QueueDepth,
		},
	}
	s.native = submitter
	go submitter.run()
	return nil
}

func (submitter *nativeWALSubmitter) notify() {
	select {
	case submitter.wake <- struct{}{}:
	default:
	}
}

func (submitter *nativeWALSubmitter) close() error {
	submitter.closeOnce.Do(func() {
		close(submitter.stop)
		submitter.notify()
	})
	<-submitter.done
	return submitter.executor.Close()
}

func (submitter *nativeWALSubmitter) recordQueueFull() {
	submitter.mu.Lock()
	submitter.stats.QueueFullRejects++
	submitter.mu.Unlock()
}

func (s *Store) NativeIOStats() NativeIOStats {
	if s.native == nil {
		return NativeIOStats{}
	}
	s.native.mu.Lock()
	defer s.native.mu.Unlock()
	return s.native.stats
}

func (submitter *nativeWALSubmitter) run() {
	defer close(submitter.done)
	for {
		select {
		case <-submitter.wake:
		case <-submitter.stop:
			return
		}
		for {
			batches, err := submitter.takeRound()
			if err != nil {
				submitter.failBatchesAndQueues(batches, err)
				break
			}
			if len(batches) == 0 {
				break
			}
			if err := submitter.submitRound(batches); err != nil {
				submitter.failBatchesAndQueues(batches, err)
				break
			}
		}
	}
}

func (submitter *nativeWALSubmitter) takeRound() ([]nativeWALBatch, error) {
	maxOperations := int(submitter.executor.Stats().QueueDepth)
	if maxOperations > len(submitter.store.lanes) {
		maxOperations = len(submitter.store.lanes)
	}
	batches := make([]nativeWALBatch, 0, maxOperations)
	for _, lane := range submitter.store.lanes {
		if len(batches) == maxOperations {
			break
		}
		batch, ok, err := submitter.takeLaneBatch(lane)
		if err != nil {
			if len(batch.requests) != 0 {
				batches = append(batches, batch)
			}
			return batches, err
		}
		if ok {
			batches = append(batches, batch)
		}
	}
	return batches, nil
}

func (submitter *nativeWALSubmitter) takeLaneBatch(lane *lane) (nativeWALBatch, bool, error) {
	lane.mu.Lock()
	if len(lane.queue) == 0 {
		lane.queue = nil
		lane.draining = false
		lane.mu.Unlock()
		return nativeWALBatch{}, false, nil
	}
	firstSeq := lane.queue[0].laneSeq
	slot := firstSeq % submitter.store.slotsPerLane
	maxRecords := maxWALIOBytes / int(submitter.store.recordSize)
	if maxRecords < 1 {
		maxRecords = 1
	}
	if untilWrap := int(submitter.store.slotsPerLane - slot); maxRecords > untilWrap {
		maxRecords = untilWrap
	}
	if maxRecords > len(lane.queue) {
		maxRecords = len(lane.queue)
	}
	requests := append([]*writeRequest(nil), lane.queue[:maxRecords]...)
	for i := 0; i < maxRecords; i++ {
		lane.queue[i] = nil
	}
	lane.queue = lane.queue[maxRecords:]
	beforeWrite := lane.beforeWrite
	lane.mu.Unlock()

	for _, request := range requests {
		if beforeWrite != nil {
			beforeWrite(request)
		}
	}
	bytesNeeded := len(requests) * int(submitter.store.recordSize)
	buffer := make([]byte, bytesNeeded)
	for i, request := range requests {
		recordBuffer := buffer[i*int(submitter.store.recordSize) : (i+1)*int(submitter.store.recordSize)]
		if err := encodeRecordInto(recordBuffer, walRecord{
			LSN: request.lsn, LBA: request.lba, Flags: request.flags, Data: request.data,
		}, int(submitter.store.blockSize)); err != nil {
			return nativeWALBatch{lane: lane, requests: requests}, false, err
		}
	}
	return nativeWALBatch{
		lane:     lane,
		requests: requests,
		buffer:   buffer,
		offset:   lane.base + int64(slot)*int64(submitter.store.recordSize),
	}, true, nil
}

func (submitter *nativeWALSubmitter) submitRound(batches []nativeWALBatch) error {
	operations := make([]iouring.Operation, len(batches))
	for i, batch := range batches {
		operations[i] = iouring.Write(
			int(submitter.store.fd.Fd()),
			batch.offset,
			batch.buffer,
			uint64(i+1),
		)
	}
	before := submitter.executor.Stats()
	completions, submitErr := submitter.executor.SubmitAndWait(operations)
	after := submitter.executor.Stats()

	submitter.mu.Lock()
	submitter.stats.SubmissionRounds++
	submitter.stats.SQEs += uint64(len(operations))
	submitter.stats.SubmitSyscalls += after.SubmitSyscalls - before.SubmitSyscalls
	submitter.stats.CompletionCount += uint64(len(completions))
	if uint64(len(operations)) > submitter.stats.InflightHighWater {
		submitter.stats.InflightHighWater = uint64(len(operations))
	}
	submitter.mu.Unlock()

	if submitErr != nil {
		return fmt.Errorf("parallelwal: native WAL submission: %w", submitErr)
	}
	byID := make(map[uint64]iouring.Completion, len(completions))
	for _, completion := range completions {
		if completion.UserData == 0 || completion.UserData > uint64(len(batches)) {
			return fmt.Errorf("parallelwal: native WAL completion user_data=%d out of range", completion.UserData)
		}
		if _, exists := byID[completion.UserData]; exists {
			return fmt.Errorf("parallelwal: duplicate native WAL completion user_data=%d", completion.UserData)
		}
		byID[completion.UserData] = completion
	}
	if len(byID) != len(batches) {
		return fmt.Errorf("parallelwal: native WAL completions=%d want=%d", len(byID), len(batches))
	}

	for i, batch := range batches {
		completion := byID[uint64(i+1)]
		expected := int32(len(batch.buffer))
		if completion.Result != expected {
			submitter.mu.Lock()
			submitter.stats.ShortCompletions++
			submitter.mu.Unlock()
			if completion.Result < 0 {
				return fmt.Errorf(
					"parallelwal: native lane %d append LSN range [%d,%d]: %w",
					batch.lane.id,
					batch.requests[0].lsn,
					batch.requests[len(batch.requests)-1].lsn,
					syscall.Errno(-completion.Result),
				)
			}
			return fmt.Errorf(
				"parallelwal: native lane %d short append LSN range [%d,%d]: got=%d want=%d",
				batch.lane.id,
				batch.requests[0].lsn,
				batch.requests[len(batch.requests)-1].lsn,
				completion.Result,
				expected,
			)
		}
	}

	for _, batch := range batches {
		submitter.completeBatch(batch, nil)
	}
	return nil
}

func (submitter *nativeWALSubmitter) completeBatch(batch nativeWALBatch, err error) {
	batch.lane.mu.Lock()
	batch.lane.completedSeq += uint64(len(batch.requests))
	batch.lane.mu.Unlock()
	submitter.store.mu.Lock()
	submitter.store.walWriteOps++
	submitter.store.mu.Unlock()
	for _, request := range batch.requests {
		submitter.store.complete(request, err)
	}
}

func (submitter *nativeWALSubmitter) failBatchesAndQueues(batches []nativeWALBatch, err error) {
	if err == nil {
		err = errors.New("parallelwal: native WAL execution failed")
	}
	for _, batch := range batches {
		submitter.completeBatch(batch, err)
	}
	for _, lane := range submitter.store.lanes {
		submitter.store.failQueuedLane(lane, err)
	}
}
