package parallelwal

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

var (
	errSegmentOwnerClosed = errors.New("parallelwal: segment owner closed")
	errSegmentQueueFull   = errors.New("parallelwal: segment owner queue full")
	errSegmentLogFull     = errors.New("parallelwal: segment log full")
)

const maxSegmentQueueDepth = 4096

type segmentOwnerConfig struct {
	BlockSize            uint32
	NumBlocks            uint32
	QueueDepth           int
	MaxEntriesPerSegment int
	LogOffset            int64
	MaxLogBytes          int64
}

func (c segmentOwnerConfig) validate() error {
	if c.BlockSize == 0 || c.NumBlocks == 0 || c.QueueDepth <= 0 ||
		c.QueueDepth > maxSegmentQueueDepth ||
		c.MaxEntriesPerSegment <= 0 || c.MaxEntriesPerSegment > maxSegmentEntries ||
		c.LogOffset < 0 || c.MaxLogBytes <= 0 ||
		c.LogOffset > int64(^uint64(0)>>1)-c.MaxLogBytes {
		return fmt.Errorf("parallelwal: invalid segment owner config %+v", c)
	}
	if _, err := segmentEncodedSize(uint32(c.MaxEntriesPerSegment), c.BlockSize); err != nil {
		return fmt.Errorf("parallelwal: invalid segment owner config: %w", err)
	}
	minimum, err := segmentEncodedSize(1, c.BlockSize)
	if err != nil {
		return fmt.Errorf("parallelwal: invalid segment owner config: %w", err)
	}
	if c.MaxLogBytes < int64(minimum) {
		return fmt.Errorf("parallelwal: segment log bytes %d below one-entry segment %d",
			c.MaxLogBytes, minimum)
	}
	return nil
}

type segmentOwnerRequest struct {
	record walRecord
	result chan error
}

type segmentOwnerMetrics struct {
	AdmittedRequests    uint64
	SegmentsWritten     uint64
	EntriesWritten      uint64
	BytesWritten        uint64
	QueueFullRejects    uint64
	QueueHighWater      uint64
	OwnedBytesHighWater uint64
}

type segmentCommitSnapshot struct {
	PublishedLSN   uint64
	CommittedBytes int64
	SegmentCount   uint64
	FirstSequence  uint64
	FirstLSN       uint64
	LastLSN        uint64
}

type segmentOwner struct {
	writer io.WriterAt
	config segmentOwnerConfig
	queue  []*segmentOwnerRequest
	done   chan struct{}

	mu                 sync.Mutex
	cond               *sync.Cond
	closeOnce          sync.Once
	closed             bool
	terminalErr        error
	nextLSN            uint64
	admitting          int
	queueHead          int
	queueLen           int
	ownedBytes         uint64
	publishedLSN       uint64
	writtenBytes       int64
	completedSegments  uint64
	publicationWaiters int
	durabilityBarrier  bool
	barrierWaiters     int
	admissionTokens    chan struct{}
	beforePublish      func()

	admittedRequests    atomic.Uint64
	segmentsWritten     atomic.Uint64
	entriesWritten      atomic.Uint64
	bytesWritten        atomic.Uint64
	queueFullRejects    atomic.Uint64
	queueHighWater      atomic.Uint64
	ownedBytesHighWater atomic.Uint64
}

func newSegmentOwner(writer io.WriterAt, config segmentOwnerConfig) (*segmentOwner, error) {
	if writer == nil {
		return nil, errors.New("parallelwal: nil segment writer")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	owner := &segmentOwner{
		writer:  writer,
		config:  config,
		queue:   make([]*segmentOwnerRequest, config.QueueDepth),
		done:    make(chan struct{}),
		nextLSN: 1,
		admissionTokens: make(chan struct{},
			config.QueueDepth+config.MaxEntriesPerSegment),
	}
	owner.cond = sync.NewCond(&owner.mu)
	go owner.run()
	return owner, nil
}

func (o *segmentOwner) Submit(lba uint32, data []byte) (uint64, error) {
	if lba >= o.config.NumBlocks {
		return 0, fmt.Errorf("parallelwal: segment owner LBA %d out of range", lba)
	}
	if len(data) != int(o.config.BlockSize) {
		return 0, fmt.Errorf("parallelwal: segment owner payload=%d blockSize=%d",
			len(data), o.config.BlockSize)
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return 0, errSegmentOwnerClosed
	}
	if o.terminalErr != nil {
		err := o.terminalErr
		o.mu.Unlock()
		return 0, err
	}
	select {
	case o.admissionTokens <- struct{}{}:
		o.admitting++
		o.ownedBytes += uint64(o.config.BlockSize)
		raiseAtomicMax(&o.ownedBytesHighWater, o.ownedBytes)
	default:
		o.queueFullRejects.Add(1)
		o.mu.Unlock()
		return 0, errSegmentQueueFull
	}
	o.mu.Unlock()

	request := &segmentOwnerRequest{
		record: walRecord{
			LBA:   lba,
			Flags: flagWrite,
			Data:  append([]byte(nil), data...),
		},
		result: make(chan error, 1),
	}

	o.mu.Lock()
	if o.closed {
		o.finishAdmissionLocked()
		o.releaseOwnedLocked()
		o.mu.Unlock()
		return 0, errSegmentOwnerClosed
	}
	if o.terminalErr != nil {
		err := o.terminalErr
		o.finishAdmissionLocked()
		o.releaseOwnedLocked()
		o.mu.Unlock()
		return 0, err
	}
	request.record.LSN = o.nextLSN
	if o.queueLen >= o.config.QueueDepth {
		o.queueFullRejects.Add(1)
		o.finishAdmissionLocked()
		o.releaseOwnedLocked()
		o.mu.Unlock()
		return 0, errSegmentQueueFull
	}
	queueIndex := (o.queueHead + o.queueLen) % len(o.queue)
	o.queue[queueIndex] = request
	o.queueLen++
	o.nextLSN++
	o.admittedRequests.Add(1)
	raiseAtomicMax(&o.queueHighWater, uint64(o.queueLen))
	o.finishAdmissionLocked()
	o.cond.Signal()
	o.mu.Unlock()

	if err := <-request.result; err != nil {
		return 0, err
	}
	return request.record.LSN, nil
}

func (o *segmentOwner) run() {
	defer close(o.done)
	offset := o.config.LogOffset
	var sequence uint64 = 1
	for {
		o.mu.Lock()
		for o.queueLen == 0 && !o.closed && o.terminalErr == nil {
			o.cond.Wait()
		}
		if o.terminalErr != nil || o.queueLen == 0 && o.closed {
			o.mu.Unlock()
			return
		}
		count := min(o.queueLen, o.config.MaxEntriesPerSegment)
		batch := make([]*segmentOwnerRequest, count)
		for i := range batch {
			batch[i] = o.queue[o.queueHead]
			o.queue[o.queueHead] = nil
			o.queueHead = (o.queueHead + 1) % len(o.queue)
		}
		o.queueLen -= count
		o.mu.Unlock()

		records := make([]walRecord, len(batch))
		for i, request := range batch {
			records[i] = request.record
		}
		encoded, err := encodeSegment(sequence, records, o.config.BlockSize, o.config.NumBlocks)
		if err == nil && int64(len(encoded)) > o.config.MaxLogBytes-(offset-o.config.LogOffset) {
			err = errSegmentLogFull
		}
		if err == nil {
			n, writeErr := o.writer.WriteAt(encoded, offset)
			if writeErr != nil {
				err = fmt.Errorf("parallelwal: write segment %d: %w", sequence, writeErr)
			} else if n != len(encoded) {
				err = fmt.Errorf("parallelwal: write segment %d: %w", sequence, io.ErrShortWrite)
			}
		}
		if err != nil {
			o.failTerminal(batch, err)
			return
		}

		offset += int64(len(encoded))
		if o.beforePublish != nil {
			o.beforePublish()
		}
		if err := o.publish(batch, records[len(records)-1].LSN,
			offset-o.config.LogOffset, len(encoded)); err != nil {
			o.failTerminal(batch, err)
			return
		}
		sequence++
	}
}

func (o *segmentOwner) publish(
	requests []*segmentOwnerRequest,
	lastLSN uint64,
	committedBytes int64,
	encodedBytes int,
) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.durabilityBarrier && o.terminalErr == nil {
		o.barrierWaiters++
		defer func() { o.barrierWaiters-- }()
	}
	for o.durabilityBarrier && o.terminalErr == nil {
		o.cond.Wait()
	}
	if o.terminalErr != nil {
		return o.terminalErr
	}
	o.publishedLSN = lastLSN
	o.writtenBytes = committedBytes
	o.completedSegments++
	o.segmentsWritten.Add(1)
	o.entriesWritten.Add(uint64(len(requests)))
	o.bytesWritten.Add(uint64(encodedBytes))
	for _, request := range requests {
		request.result <- nil
		close(request.result)
		request.record.Data = nil
		o.releaseOwnedLocked()
	}
	o.cond.Broadcast()
	return nil
}

func (o *segmentOwner) complete(requests []*segmentOwnerRequest, err error) {
	for _, request := range requests {
		request.result <- err
		close(request.result)
		request.record.Data = nil
	}
	o.mu.Lock()
	for range requests {
		o.releaseOwnedLocked()
	}
	o.mu.Unlock()
}

func (o *segmentOwner) failTerminal(active []*segmentOwnerRequest, err error) {
	o.mu.Lock()
	if o.terminalErr == nil {
		o.terminalErr = err
	}
	terminalErr := o.terminalErr
	queued := make([]*segmentOwnerRequest, o.queueLen)
	for i := range queued {
		queued[i] = o.queue[o.queueHead]
		o.queue[o.queueHead] = nil
		o.queueHead = (o.queueHead + 1) % len(o.queue)
	}
	o.queueLen = 0
	o.cond.Broadcast()
	o.mu.Unlock()
	o.complete(active, terminalErr)
	o.complete(queued, terminalErr)
}

func (o *segmentOwner) Fail(err error) {
	if err == nil {
		return
	}
	o.mu.Lock()
	if o.terminalErr == nil {
		o.terminalErr = err
	}
	terminalErr := o.terminalErr
	queued := make([]*segmentOwnerRequest, o.queueLen)
	for i := range queued {
		queued[i] = o.queue[o.queueHead]
		o.queue[o.queueHead] = nil
		o.queueHead = (o.queueHead + 1) % len(o.queue)
	}
	o.queueLen = 0
	o.cond.Broadcast()
	o.mu.Unlock()
	o.complete(queued, terminalErr)
}

func (o *segmentOwner) Fence() (uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.terminalErr != nil {
		return 0, o.terminalErr
	}
	if o.closed {
		return 0, errSegmentOwnerClosed
	}
	return o.nextLSN - 1, nil
}

func (o *segmentOwner) WaitPublished(targetLSN uint64) (segmentCommitSnapshot, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.publishedLSN < targetLSN && o.terminalErr == nil {
		o.publicationWaiters++
		defer func() { o.publicationWaiters-- }()
	}
	for o.publishedLSN < targetLSN && o.terminalErr == nil {
		o.cond.Wait()
	}
	if o.terminalErr != nil {
		return segmentCommitSnapshot{}, o.terminalErr
	}
	if o.publishedLSN < targetLSN {
		return segmentCommitSnapshot{}, fmt.Errorf(
			"parallelwal: segment owner stopped at LSN %d before target %d",
			o.publishedLSN, targetLSN)
	}
	return o.commitSnapshotLocked(), nil
}

func (o *segmentOwner) BeginDurability(targetLSN uint64) (segmentCommitSnapshot, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.durabilityBarrier {
		return segmentCommitSnapshot{}, errors.New("parallelwal: durability barrier already active")
	}
	if o.publishedLSN < targetLSN && o.terminalErr == nil {
		o.publicationWaiters++
		defer func() { o.publicationWaiters-- }()
	}
	for o.publishedLSN < targetLSN && o.terminalErr == nil {
		o.cond.Wait()
	}
	if o.terminalErr != nil {
		return segmentCommitSnapshot{}, o.terminalErr
	}
	if o.publishedLSN < targetLSN {
		return segmentCommitSnapshot{}, fmt.Errorf(
			"parallelwal: segment owner stopped at LSN %d before durability target %d",
			o.publishedLSN, targetLSN)
	}
	o.durabilityBarrier = true
	return o.commitSnapshotLocked(), nil
}

func (o *segmentOwner) EndDurability(err error) {
	if err != nil {
		o.Fail(err)
	}
	o.mu.Lock()
	o.durabilityBarrier = false
	o.cond.Broadcast()
	o.mu.Unlock()
}

func (o *segmentOwner) commitSnapshotLocked() segmentCommitSnapshot {
	snapshot := segmentCommitSnapshot{
		PublishedLSN:   o.publishedLSN,
		CommittedBytes: o.writtenBytes,
		SegmentCount:   o.completedSegments,
	}
	if snapshot.SegmentCount != 0 {
		snapshot.FirstSequence = 1
		snapshot.FirstLSN = 1
		snapshot.LastLSN = o.publishedLSN
	}
	return snapshot
}

func (o *segmentOwner) Close() error {
	o.closeOnce.Do(func() {
		o.mu.Lock()
		o.closed = true
		for o.admitting != 0 {
			o.cond.Wait()
		}
		o.cond.Broadcast()
		o.mu.Unlock()
		<-o.done
	})
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.terminalErr
}

func (o *segmentOwner) finishAdmissionLocked() {
	o.admitting--
	o.cond.Broadcast()
}

func (o *segmentOwner) releaseOwnedLocked() {
	<-o.admissionTokens
	o.ownedBytes -= uint64(o.config.BlockSize)
}

func (o *segmentOwner) Metrics() segmentOwnerMetrics {
	return segmentOwnerMetrics{
		AdmittedRequests:    o.admittedRequests.Load(),
		SegmentsWritten:     o.segmentsWritten.Load(),
		EntriesWritten:      o.entriesWritten.Load(),
		BytesWritten:        o.bytesWritten.Load(),
		QueueFullRejects:    o.queueFullRejects.Load(),
		QueueHighWater:      o.queueHighWater.Load(),
		OwnedBytesHighWater: o.ownedBytesHighWater.Load(),
	}
}

func raiseAtomicMax(value *atomic.Uint64, candidate uint64) {
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}
