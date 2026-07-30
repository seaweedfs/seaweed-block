package parallelwal

import (
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/seaweedfs/seaweed-block/internal/iouring"
)

type NativeIOStats struct {
	Enabled            bool
	QueueDepth         uint32
	AdmittedRequests   uint64
	SubmissionRounds   uint64
	SQEs               uint64
	SubmitSyscalls     uint64
	CompletionCount    uint64
	DurabilityBarriers uint64
	FsyncCompletions   uint64
	ShortCompletions   uint64
	QueueFullRejects   uint64
	InflightHighWater  uint64
	BufferAllocations  uint64
	FallbackCount      uint64
}

type nativeWALBatch struct {
	lane      *lane
	requests  []*writeRequest
	buffer    []byte
	offset    int64
	attempted bool
}

type nativeIOExecutor interface {
	SubmitAndWait([]iouring.Operation) ([]iouring.Completion, error)
	Stats() iouring.ExecutionStats
	Close() error
}

var newNativeIOExecutor = func(depth uint32) (nativeIOExecutor, error) {
	return iouring.New(depth)
}

type nativeWALSubmitter struct {
	store     *Store
	executor  nativeIOExecutor
	wake      chan struct{}
	barriers  chan chan error
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	nextLane  int
	buffers   [][]byte

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
	executor, err := newNativeIOExecutor(uint32(depth))
	if err != nil {
		return fmt.Errorf("parallelwal: io_uring execution: %w", err)
	}
	submitter := &nativeWALSubmitter{
		store:    s,
		executor: executor,
		wake:     make(chan struct{}, 1),
		barriers: make(chan chan error),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		buffers:  make([][]byte, len(s.lanes)),
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

func (submitter *nativeWALSubmitter) recordAdmitted(count int) {
	submitter.mu.Lock()
	submitter.stats.AdmittedRequests += uint64(count)
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
		case result := <-submitter.barriers:
			submitter.handleBarrier(result)
			continue
		default:
		}
		select {
		case <-submitter.wake:
			more, err := submitter.processOneRound()
			if err == nil && more {
				submitter.notify()
			}
		case result := <-submitter.barriers:
			submitter.handleBarrier(result)
		case <-submitter.stop:
			return
		}
	}
}

func (submitter *nativeWALSubmitter) handleBarrier(result chan error) {
	// Store.Sync waits for its target LSN to publish before sending this
	// request, so queued later writes are outside the fence.
	err := submitter.submitBarrier()
	if err != nil {
		submitter.failBatchesAndQueues(nil, err)
	}
	result <- err
}

func (submitter *nativeWALSubmitter) processOneRound() (bool, error) {
	batches, err := submitter.takeRound()
	if err != nil {
		submitter.failBatchesAndQueues(batches, err)
		return false, err
	}
	if len(batches) == 0 {
		return false, nil
	}
	if err := submitter.submitRound(batches); err != nil {
		submitter.failBatchesAndQueues(batches, err)
		return false, err
	}
	return submitter.hasQueued(), nil
}

func (submitter *nativeWALSubmitter) hasQueued() bool {
	hasQueued := false
	for _, lane := range submitter.store.lanes {
		lane.mu.Lock()
		if len(lane.queue) == 0 {
			lane.queue = nil
			lane.draining = false
		} else {
			hasQueued = true
		}
		lane.mu.Unlock()
	}
	return hasQueued
}

func (submitter *nativeWALSubmitter) sync() error {
	result := make(chan error, 1)
	select {
	case submitter.barriers <- result:
	case <-submitter.done:
		return errors.New("parallelwal: native WAL owner stopped before durability barrier")
	}
	return <-result
}

func (submitter *nativeWALSubmitter) takeRound() ([]nativeWALBatch, error) {
	maxOperations := int(submitter.executor.Stats().QueueDepth)
	if maxOperations > len(submitter.store.lanes) {
		maxOperations = len(submitter.store.lanes)
	}
	batches := make([]nativeWALBatch, 0, maxOperations)
	startLane := submitter.nextLane
	for visited := 0; visited < len(submitter.store.lanes); visited++ {
		if len(batches) == maxOperations {
			break
		}
		laneID := (startLane + visited) % len(submitter.store.lanes)
		lane := submitter.store.lanes[laneID]
		batch, ok, err := submitter.takeLaneBatch(lane)
		if err != nil {
			if len(batch.requests) != 0 {
				batches = append(batches, batch)
			}
			return batches, err
		}
		if ok {
			batches = append(batches, batch)
			submitter.nextLane = (laneID + 1) % len(submitter.store.lanes)
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
	buffer := submitter.buffers[lane.id]
	if cap(buffer) < bytesNeeded {
		bufferRecords := maxWALIOBytes / int(submitter.store.recordSize)
		if lane.queueDepth < uint64(bufferRecords) {
			bufferRecords = int(lane.queueDepth)
		}
		buffer = make([]byte, bytesNeeded, bufferRecords*int(submitter.store.recordSize))
		submitter.mu.Lock()
		submitter.stats.BufferAllocations++
		submitter.mu.Unlock()
	} else {
		buffer = buffer[:bytesNeeded]
	}
	submitter.buffers[lane.id] = buffer
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
	submittedOps := after.SubmittedOps - before.SubmittedOps
	for i := uint64(0); i < submittedOps && i < uint64(len(batches)); i++ {
		batches[i].attempted = true
	}

	submitter.mu.Lock()
	submitter.stats.SubmissionRounds++
	submitter.stats.SQEs += submittedOps
	submitter.stats.SubmitSyscalls += after.SubmitSyscalls - before.SubmitSyscalls
	submitter.stats.CompletionCount += uint64(len(completions))
	if submittedOps > submitter.stats.InflightHighWater {
		submitter.stats.InflightHighWater = submittedOps
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

	var completionErr error
	var shortCompletions uint64
	for i, batch := range batches {
		completion := byID[uint64(i+1)]
		expected := int32(len(batch.buffer))
		if completion.Result != expected {
			shortCompletions++
			if completion.Result < 0 {
				completionErr = errors.Join(completionErr, fmt.Errorf(
					"parallelwal: native lane %d append LSN range [%d,%d]: %w",
					batch.lane.id,
					batch.requests[0].lsn,
					batch.requests[len(batch.requests)-1].lsn,
					syscall.Errno(-completion.Result),
				))
				continue
			}
			completionErr = errors.Join(completionErr, fmt.Errorf(
				"parallelwal: native lane %d short append LSN range [%d,%d]: got=%d want=%d",
				batch.lane.id,
				batch.requests[0].lsn,
				batch.requests[len(batch.requests)-1].lsn,
				completion.Result,
				expected,
			))
		}
	}
	if shortCompletions != 0 {
		submitter.mu.Lock()
		submitter.stats.ShortCompletions += shortCompletions
		submitter.mu.Unlock()
		return completionErr
	}

	for _, batch := range batches {
		submitter.completeBatch(batch, nil)
	}
	return nil
}

func (submitter *nativeWALSubmitter) submitBarrier() error {
	const barrierUserData = ^uint64(0)
	before := submitter.executor.Stats()
	completions, err := submitter.executor.SubmitAndWait([]iouring.Operation{
		iouring.Fsync(int(submitter.store.fd.Fd()), barrierUserData),
	})
	after := submitter.executor.Stats()
	submittedOps := after.SubmittedOps - before.SubmittedOps

	submitter.mu.Lock()
	submitter.stats.SQEs += submittedOps
	submitter.stats.SubmitSyscalls += after.SubmitSyscalls - before.SubmitSyscalls
	submitter.stats.CompletionCount += uint64(len(completions))
	submitter.stats.DurabilityBarriers += submittedOps
	if submittedOps > submitter.stats.InflightHighWater {
		submitter.stats.InflightHighWater = submittedOps
	}
	submitter.mu.Unlock()

	if err != nil {
		return fmt.Errorf("parallelwal: native WAL durability barrier: %w", err)
	}
	if len(completions) != 1 || completions[0].UserData != barrierUserData {
		return fmt.Errorf("parallelwal: native WAL barrier completions=%+v", completions)
	}
	if completions[0].Result != 0 {
		submitter.mu.Lock()
		submitter.stats.ShortCompletions++
		submitter.mu.Unlock()
		if completions[0].Result < 0 {
			return fmt.Errorf(
				"parallelwal: native WAL durability barrier: %w",
				syscall.Errno(-completions[0].Result),
			)
		}
		return fmt.Errorf(
			"parallelwal: native WAL durability barrier result=%d want=0",
			completions[0].Result,
		)
	}
	submitter.mu.Lock()
	submitter.stats.FsyncCompletions++
	submitter.mu.Unlock()
	return nil
}

func (submitter *nativeWALSubmitter) completeBatch(batch nativeWALBatch, err error) {
	batch.lane.mu.Lock()
	batch.lane.completedSeq += uint64(len(batch.requests))
	batch.lane.mu.Unlock()
	if batch.attempted {
		submitter.store.mu.Lock()
		submitter.store.walWriteOps++
		submitter.store.mu.Unlock()
	}
	for _, request := range batch.requests {
		submitter.store.complete(request, err)
	}
}

func (submitter *nativeWALSubmitter) failBatchesAndQueues(batches []nativeWALBatch, err error) {
	if err == nil {
		err = errors.New("parallelwal: native WAL execution failed")
	}
	submitter.store.markTerminal(err)
	for _, batch := range batches {
		submitter.completeBatch(batch, err)
	}
	for _, lane := range submitter.store.lanes {
		submitter.store.failQueuedLane(lane, err)
	}
}

func (s *Store) markTerminal(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	for _, pending := range s.pending {
		s.deliverLocked(pending, s.terminalErr)
	}
	s.cond.Broadcast()
}

func (s *Store) syncFile() error {
	if s.native != nil {
		return s.native.sync()
	}
	return s.fd.Sync()
}
