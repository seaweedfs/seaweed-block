// Package parallelwal provides an opt-in single-file LogicalStorage backend
// with deterministic LBA-to-lane ownership and a global contiguous LSN
// publication frontier.
package parallelwal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	defaultQueueDepth       = 128
	maxCheckpointWriteBytes = 1 << 20
	maxWALIOBytes           = 1 << 20
)

var (
	ErrNotRecovered = errors.New("parallelwal: store must be recovered before use")
	ErrQueueFull    = errors.New("parallelwal: lane queue full")
	ErrWALFull      = errors.New("parallelwal: lane WAL full")
)

type Config struct {
	NumBlocks     uint32
	BlockSize     int
	LaneCount     int
	StripeBlocks  int
	SlotsPerLane  uint64
	RetainPerLane uint64
	QueueDepth    int
}

type blockVersion struct {
	lsn  uint64
	data []byte
}

type writeRequest struct {
	lsn                uint64
	lba                uint32
	flags              uint16
	data               []byte
	lane               int
	laneSeq            uint64
	publicationBaseLSN uint64
	done               bool
	delivered          bool
	err                error
	result             chan error
}

type lane struct {
	id      int
	base    int64
	nextSeq uint64

	mu           sync.Mutex
	completedSeq uint64
	queueDepth   uint64
	queue        []*writeRequest
	draining     bool
	buffer       []byte
	beforeWrite  func(*writeRequest)
}

type Store struct {
	path         string
	fd           *os.File
	hdr          fileHeader
	headerSlot   int
	extentBases  [2]int64
	activeExtent int

	numBlocks     uint32
	blockSize     uint32
	laneCount     int
	stripeBlocks  uint32
	recordSize    uint32
	slotsPerLane  uint64
	retainPerLane uint64

	mu                 sync.RWMutex
	cond               *sync.Cond
	syncMu             sync.Mutex
	closed             bool
	closing            bool
	recovered          bool
	terminalErr        error
	reuseFence         bool
	nextLSN            uint64
	stableLSN          uint64
	checkpointLSN      uint64
	pendingBaseLSN     uint64
	baseCommitPending  bool
	walTail            uint64
	publishedLSN       uint64
	publishedHeads     [maxLaneCount]uint64
	inflightAppends    int
	pending            map[uint64]*writeRequest
	latest             map[uint32]blockVersion
	applied            map[uint32]uint64
	history            map[uint64]walRecord
	baseStageActive    bool
	baseStageSlot      int
	baseStage          map[uint32][]byte
	checkpointWriteOps uint64
	walWriteOps        uint64
	recycleReadOps     uint64

	lanes       []*lane
	extentLocks [256]sync.RWMutex
}

func (s *Store) DurableStorageIdentity() storage.DurableStorageIdentity {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return storage.DurableStorageIdentity{
		Path:    s.path,
		StoreID: fmt.Sprintf("parallelwal:%d:%d:%d:%d:%d", s.hdr.CreatedAt, s.numBlocks, s.blockSize, s.laneCount, s.slotsPerLane),
	}
}

// InspectStoreIdentity reads the selected durable header and geometry without
// starting lane workers or opening the file for writes.
func InspectStoreIdentity(path string) (storage.DurableStorageIdentity, uint32, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return storage.DurableStorageIdentity{}, 0, 0, fmt.Errorf("parallelwal: inspect open %s: %w", path, err)
	}
	defer f.Close()
	h, _, err := readBestHeader(f)
	if err != nil {
		return storage.DurableStorageIdentity{}, 0, 0, err
	}
	required, err := fileSize(h)
	if err != nil {
		return storage.DurableStorageIdentity{}, 0, 0, err
	}
	stat, err := f.Stat()
	if err != nil || stat.Size() < required {
		return storage.DurableStorageIdentity{}, 0, 0, fmt.Errorf("%w: truncated parallelwal store", errBadGeometry)
	}
	return storage.DurableStorageIdentity{
		Path: path, StoreID: fmt.Sprintf("parallelwal:%d:%d:%d:%d:%d", h.CreatedAt, h.NumBlocks, h.BlockSize, h.LaneCount, h.SlotsPerLane),
	}, h.NumBlocks, int(h.BlockSize), nil
}

func CreateStore(path string, numBlocks uint32, blockSize int) (*Store, error) {
	return CreateStoreWithConfig(path, Config{
		NumBlocks:     numBlocks,
		BlockSize:     blockSize,
		LaneCount:     defaultLaneCount,
		StripeBlocks:  1,
		SlotsPerLane:  defaultLaneSlots,
		RetainPerLane: defaultLaneSlots / 2,
		QueueDepth:    defaultQueueDepth,
	})
}

func CreateStoreWithConfig(path string, cfg Config) (*Store, error) {
	cfg = normalizeConfig(cfg)
	h, err := headerForConfig(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("parallelwal: mkdir %s: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("parallelwal: create %s: %w", path, err)
	}
	totalSize, err := fileSize(h)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := f.Truncate(totalSize); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("parallelwal: preallocate %d bytes: %w", totalSize, err)
	}
	if err := writeHeaderAt(f, 0, h); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("parallelwal: fsync create: %w", err)
	}
	return newStore(path, f, h, 0, cfg.QueueDepth, true), nil
}

func OpenStore(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("parallelwal: open %s: %w", path, err)
	}
	h, slot, err := readBestHeader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	required, err := fileSize(h)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("parallelwal: stat %s: %w", path, err)
	}
	if st.Size() < required {
		_ = f.Close()
		return nil, fmt.Errorf("%w: truncated file size=%d required=%d", errBadGeometry, st.Size(), required)
	}
	return newStore(path, f, h, slot, defaultQueueDepth, false), nil
}

func normalizeConfig(cfg Config) Config {
	if cfg.BlockSize == 0 {
		cfg.BlockSize = storage.DefaultBlockSize
	}
	if cfg.LaneCount == 0 {
		cfg.LaneCount = defaultLaneCount
	}
	if cfg.StripeBlocks == 0 {
		cfg.StripeBlocks = 1
	}
	if cfg.SlotsPerLane == 0 {
		cfg.SlotsPerLane = defaultLaneSlots
	}
	if cfg.RetainPerLane == 0 {
		cfg.RetainPerLane = cfg.SlotsPerLane / 2
		if cfg.RetainPerLane == 0 {
			cfg.RetainPerLane = 1
		}
	}
	if cfg.QueueDepth == 0 {
		cfg.QueueDepth = defaultQueueDepth
	}
	return cfg
}

func headerForConfig(cfg Config) (fileHeader, error) {
	if cfg.NumBlocks == 0 || cfg.BlockSize <= 0 {
		return fileHeader{}, fmt.Errorf("%w: numBlocks=%d blockSize=%d", errBadGeometry, cfg.NumBlocks, cfg.BlockSize)
	}
	if uint64(cfg.BlockSize) > uint64(^uint32(0))-recordHeaderSize ||
		uint64(cfg.StripeBlocks) > uint64(^uint16(0)) {
		return fileHeader{}, fmt.Errorf("%w: blockSize=%d stripeBlocks=%d exceed persisted field width",
			errBadGeometry, cfg.BlockSize, cfg.StripeBlocks)
	}
	if cfg.LaneCount <= 0 || cfg.LaneCount > maxLaneCount || cfg.StripeBlocks <= 0 ||
		cfg.SlotsPerLane < 2 || cfg.RetainPerLane == 0 || cfg.RetainPerLane >= cfg.SlotsPerLane ||
		cfg.QueueDepth <= 0 {
		return fileHeader{}, fmt.Errorf("%w: lanes=%d stripes=%d slots=%d retain=%d queue=%d",
			errBadGeometry, cfg.LaneCount, cfg.StripeBlocks, cfg.SlotsPerLane, cfg.RetainPerLane, cfg.QueueDepth)
	}
	h := fileHeader{
		Generation:    1,
		CreatedAt:     uint64(time.Now().UnixNano()),
		BlockSize:     uint32(cfg.BlockSize),
		NumBlocks:     cfg.NumBlocks,
		LaneCount:     uint16(cfg.LaneCount),
		StripeBlocks:  uint16(cfg.StripeBlocks),
		RecordSize:    uint32(recordHeaderSize + cfg.BlockSize),
		SlotsPerLane:  cfg.SlotsPerLane,
		RetainPerLane: cfg.RetainPerLane,
		WALTail:       1,
	}
	return h, h.validate()
}

func fileSize(h fileHeader) (int64, error) {
	const maxSize = uint64(^uint64(0) >> 1)
	walRecords, ok := checkedMul(uint64(h.LaneCount), h.SlotsPerLane, maxSize)
	if !ok {
		return 0, fmt.Errorf("%w: WAL record count overflow", errBadGeometry)
	}
	walBytes, ok := checkedMul(walRecords, uint64(h.RecordSize), maxSize)
	if !ok {
		return 0, fmt.Errorf("%w: WAL size overflow", errBadGeometry)
	}
	extentBytes, ok := checkedMul(uint64(h.NumBlocks), uint64(h.BlockSize), maxSize)
	if !ok {
		return 0, fmt.Errorf("%w: extent size overflow", errBadGeometry)
	}
	allExtentBytes, ok := checkedMul(extentBytes, 2, maxSize)
	if !ok {
		return 0, fmt.Errorf("%w: dual extent size overflow", errBadGeometry)
	}
	total := uint64(headerSlots * headerSize)
	if walBytes > maxSize-total {
		return 0, fmt.Errorf("%w: file size overflow", errBadGeometry)
	}
	total += walBytes
	if allExtentBytes > maxSize-total {
		return 0, fmt.Errorf("%w: file size overflow", errBadGeometry)
	}
	total += allExtentBytes
	return int64(total), nil
}

func checkedMul(a, b, limit uint64) (uint64, bool) {
	if a != 0 && b > limit/a {
		return 0, false
	}
	return a * b, true
}

func newStore(path string, f *os.File, h fileHeader, headerSlot, queueDepth int, recovered bool) *Store {
	walBase := int64(headerSlots * headerSize)
	extentBase := walBase + int64(h.LaneCount)*int64(h.SlotsPerLane)*int64(h.RecordSize)
	extentBytes := int64(h.NumBlocks) * int64(h.BlockSize)
	s := &Store{
		path:          path,
		fd:            f,
		hdr:           h,
		headerSlot:    headerSlot,
		extentBases:   [2]int64{extentBase, extentBase + extentBytes},
		activeExtent:  int(h.ActiveExtent),
		numBlocks:     h.NumBlocks,
		blockSize:     h.BlockSize,
		laneCount:     int(h.LaneCount),
		stripeBlocks:  uint32(h.StripeBlocks),
		recordSize:    h.RecordSize,
		slotsPerLane:  h.SlotsPerLane,
		retainPerLane: h.RetainPerLane,
		recovered:     recovered,
		nextLSN:       h.DurableLSN + 1,
		stableLSN:     h.DurableLSN,
		checkpointLSN: h.CheckpointLSN,
		walTail:       h.WALTail,
		publishedLSN:  h.DurableLSN,
		pending:       make(map[uint64]*writeRequest),
		latest:        make(map[uint32]blockVersion),
		applied:       make(map[uint32]uint64),
		history:       make(map[uint64]walRecord),
		baseStageSlot: 1 - int(h.ActiveExtent),
	}
	if s.nextLSN == 0 {
		s.nextLSN = 1
	}
	s.cond = sync.NewCond(&s.mu)
	s.lanes = make([]*lane, int(h.LaneCount))
	laneBytes := int64(h.SlotsPerLane) * int64(h.RecordSize)
	for i := range s.lanes {
		l := &lane{
			id:           i,
			base:         walBase + int64(i)*laneBytes,
			nextSeq:      h.LaneHeads[i],
			completedSeq: h.LaneHeads[i],
			queueDepth:   uint64(queueDepth),
			buffer:       make([]byte, h.RecordSize),
		}
		s.publishedHeads[i] = h.LaneHeads[i]
		s.lanes[i] = l
	}
	return s
}

func (s *Store) queueRequest(l *lane, req *writeRequest) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.queue = append(l.queue, req)
	if l.draining {
		return false
	}
	l.draining = true
	return true
}

func (s *Store) drainLane(l *lane, handoffAfterBatch bool) {
	for {
		l.mu.Lock()
		if len(l.queue) == 0 {
			l.queue = nil
			l.draining = false
			l.mu.Unlock()
			return
		}
		firstSeq := l.queue[0].laneSeq
		slot := firstSeq % s.slotsPerLane
		maxRecords := maxWALIOBytes / int(s.recordSize)
		if maxRecords < 1 {
			maxRecords = 1
		}
		if untilWrap := int(s.slotsPerLane - slot); maxRecords > untilWrap {
			maxRecords = untilWrap
		}
		if maxRecords > len(l.queue) {
			maxRecords = len(l.queue)
		}
		batch := append([]*writeRequest(nil), l.queue[:maxRecords]...)
		for i := 0; i < maxRecords; i++ {
			l.queue[i] = nil
		}
		l.queue = l.queue[maxRecords:]
		beforeWrite := l.beforeWrite
		l.mu.Unlock()

		for _, req := range batch {
			if beforeWrite != nil {
				beforeWrite(req)
			}
		}
		bytesNeeded := len(batch) * int(s.recordSize)
		if cap(l.buffer) < bytesNeeded {
			l.buffer = make([]byte, bytesNeeded)
		}
		buf := l.buffer[:bytesNeeded]
		var err error
		for i, req := range batch {
			recordBuf := buf[i*int(s.recordSize) : (i+1)*int(s.recordSize)]
			err = encodeRecordInto(recordBuf,
				walRecord{LSN: req.lsn, LBA: req.lba, Flags: req.flags, Data: req.data},
				int(s.blockSize))
			if err != nil {
				break
			}
		}
		if err == nil {
			off := l.base + int64(slot)*int64(s.recordSize)
			_, err = s.fd.WriteAt(buf, off)
			s.mu.Lock()
			s.walWriteOps++
			s.mu.Unlock()
			if err != nil {
				err = fmt.Errorf("parallelwal: lane %d append LSN range [%d,%d]: %w",
					l.id, batch[0].lsn, batch[len(batch)-1].lsn, err)
			}
		}

		l.mu.Lock()
		l.completedSeq += uint64(len(batch))
		l.mu.Unlock()
		for _, req := range batch {
			s.complete(req, err)
		}
		if err != nil {
			s.failQueuedLane(l, err)
			return
		}
		if handoffAfterBatch {
			l.mu.Lock()
			if len(l.queue) == 0 {
				l.draining = false
				l.mu.Unlock()
			} else {
				l.mu.Unlock()
				go s.drainLane(l, false)
			}
			return
		}
	}
}

func (s *Store) failQueuedLane(l *lane, err error) {
	l.mu.Lock()
	queued := l.queue
	l.queue = nil
	l.completedSeq += uint64(len(queued))
	l.draining = false
	l.mu.Unlock()
	for _, req := range queued {
		s.complete(req, err)
	}
}

func (s *Store) complete(req *writeRequest, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflightAppends--
	req.done = true
	req.err = err
	if err != nil && s.terminalErr == nil {
		s.terminalErr = err
	}
	if s.terminalErr != nil {
		for _, pending := range s.pending {
			s.deliverLocked(pending, s.terminalErr)
		}
		s.cond.Broadcast()
		return
	}
	if req.publicationBaseLSN > s.publishedLSN {
		s.publishedLSN = req.publicationBaseLSN
		s.walTail = req.publicationBaseLSN + 1
		for historyLSN := range s.history {
			if historyLSN <= req.publicationBaseLSN {
				delete(s.history, historyLSN)
			}
		}
	}
	for {
		next := s.pending[s.publishedLSN+1]
		if next == nil || !next.done {
			break
		}
		if next.err != nil {
			s.terminalErr = next.err
			for _, pending := range s.pending {
				s.deliverLocked(pending, s.terminalErr)
			}
			break
		}
		data := next.data
		s.latest[next.lba] = blockVersion{lsn: next.lsn, data: data}
		if next.lsn > s.applied[next.lba] {
			s.applied[next.lba] = next.lsn
		}
		s.history[next.lsn] = walRecord{LSN: next.lsn, LBA: next.lba, Flags: next.flags, Data: data}
		s.publishedLSN = next.lsn
		s.publishedHeads[next.lane] = next.laneSeq + 1
		s.deliverLocked(next, nil)
	}
	s.cond.Broadcast()
}

func (s *Store) deliverLocked(req *writeRequest, err error) {
	if req.delivered {
		return
	}
	req.delivered = true
	delete(s.pending, req.lsn)
	req.result <- err
	close(req.result)
}

func (s *Store) laneFor(lba uint32) int {
	return int((lba / s.stripeBlocks) % uint32(s.laneCount))
}

func (s *Store) submit(lba uint32, data []byte, sourceLSN uint64) (uint64, error) {
	if lba >= s.numBlocks {
		return 0, fmt.Errorf("parallelwal: LBA %d out of range", lba)
	}
	if len(data) != int(s.blockSize) {
		return 0, fmt.Errorf("parallelwal: data size %d != block size %d", len(data), s.blockSize)
	}
	cp := append([]byte(nil), data...)
	s.mu.Lock()
	for s.reuseFence && s.terminalErr == nil && !s.closed && !s.closing {
		s.cond.Wait()
	}
	if s.closed || s.closing {
		s.mu.Unlock()
		return 0, errors.New("parallelwal: write after Close")
	}
	if !s.recovered {
		s.mu.Unlock()
		return 0, ErrNotRecovered
	}
	if s.terminalErr != nil {
		err := s.terminalErr
		s.mu.Unlock()
		return 0, err
	}
	lsn := s.nextLSN
	var publicationBaseLSN uint64
	if sourceLSN != 0 {
		if sourceLSN <= s.publishedLSN {
			if s.applied[lba] >= sourceLSN {
				s.mu.Unlock()
				return sourceLSN, nil
			}
			s.mu.Unlock()
			return 0, fmt.Errorf("parallelwal: apply LSN %d is behind published frontier %d", sourceLSN, s.publishedLSN)
		}
		if sourceLSN < s.nextLSN {
			s.mu.Unlock()
			return 0, fmt.Errorf("parallelwal: apply LSN %d collides with an in-flight LSN", sourceLSN)
		}
		if sourceLSN > s.nextLSN {
			if s.inflightAppends != 0 {
				s.mu.Unlock()
				return 0, fmt.Errorf("parallelwal: cannot jump apply frontier with writes in flight")
			}
			// LogicalStorage permits a replica stream to begin above LSN 1.
			// The unrepresented interval is an explicit retention gap, not
			// an extent checkpoint; it is not catch-up eligible.
			publicationBaseLSN = sourceLSN - 1
			s.nextLSN = sourceLSN
		}
		lsn = sourceLSN
	}
	laneID := s.laneFor(lba)
	l := s.lanes[laneID]
	if l.nextSeq-s.hdr.LaneTails[laneID] >= s.slotsPerLane {
		s.mu.Unlock()
		return 0, ErrWALFull
	}
	l.mu.Lock()
	completedSeq := l.completedSeq
	l.mu.Unlock()
	if l.nextSeq-completedSeq >= l.queueDepth {
		s.mu.Unlock()
		return 0, ErrQueueFull
	}
	req := &writeRequest{
		lsn:                lsn,
		lba:                lba,
		flags:              flagWrite,
		data:               cp,
		lane:               laneID,
		laneSeq:            l.nextSeq,
		publicationBaseLSN: publicationBaseLSN,
		result:             make(chan error, 1),
	}
	l.nextSeq++
	s.nextLSN = lsn + 1
	s.pending[lsn] = req
	s.inflightAppends++
	startDrainer := s.queueRequest(l, req)
	s.mu.Unlock()
	if startDrainer {
		s.drainLane(l, true)
	}
	if err := <-req.result; err != nil {
		return 0, err
	}
	return lsn, nil
}

func (s *Store) Write(lba uint32, data []byte) (uint64, error) {
	return s.submit(lba, data, 0)
}

func (s *Store) WriteBatch(startLBA uint32, blocks [][]byte) ([]uint64, error) {
	if len(blocks) == 0 {
		return nil, nil
	}
	if uint64(startLBA)+uint64(len(blocks)) > uint64(s.numBlocks) {
		return nil, fmt.Errorf("parallelwal: batch [%d,%d) out of range (max %d)",
			startLBA, uint64(startLBA)+uint64(len(blocks)), s.numBlocks)
	}
	copies := make([][]byte, len(blocks))
	for i, data := range blocks {
		if len(data) != int(s.blockSize) {
			return nil, fmt.Errorf("parallelwal: batch block %d data size %d != block size %d",
				i, len(data), s.blockSize)
		}
		copies[i] = append([]byte(nil), data...)
	}

	s.mu.Lock()
	for s.reuseFence && s.terminalErr == nil && !s.closed && !s.closing {
		s.cond.Wait()
	}
	if s.closed || s.closing {
		s.mu.Unlock()
		return nil, errors.New("parallelwal: WriteBatch after Close")
	}
	if !s.recovered {
		s.mu.Unlock()
		return nil, ErrNotRecovered
	}
	if s.terminalErr != nil {
		err := s.terminalErr
		s.mu.Unlock()
		return nil, err
	}
	perLane := make([]int, len(s.lanes))
	for i := range copies {
		perLane[s.laneFor(startLBA+uint32(i))]++
	}
	for laneID, count := range perLane {
		l := s.lanes[laneID]
		if l.nextSeq+uint64(count)-s.hdr.LaneTails[laneID] > s.slotsPerLane {
			s.mu.Unlock()
			return nil, ErrWALFull
		}
		l.mu.Lock()
		completedSeq := l.completedSeq
		l.mu.Unlock()
		if l.nextSeq+uint64(count)-completedSeq > l.queueDepth {
			s.mu.Unlock()
			return nil, ErrQueueFull
		}
	}

	reqs := make([]*writeRequest, len(copies))
	lsns := make([]uint64, len(copies))
	for i, data := range copies {
		lba := startLBA + uint32(i)
		lsn := s.nextLSN + uint64(i)
		laneID := s.laneFor(lba)
		l := s.lanes[laneID]
		req := &writeRequest{
			lsn:     lsn,
			lba:     lba,
			flags:   flagWrite,
			data:    data,
			lane:    laneID,
			laneSeq: l.nextSeq,
			result:  make(chan error, 1),
		}
		l.nextSeq++
		s.pending[lsn] = req
		reqs[i] = req
		lsns[i] = lsn
	}
	s.nextLSN += uint64(len(reqs))
	s.inflightAppends += len(reqs)
	startDrainers := make([]bool, len(s.lanes))
	for _, req := range reqs {
		if s.queueRequest(s.lanes[req.lane], req) {
			startDrainers[req.lane] = true
		}
	}
	s.mu.Unlock()

	for laneID, start := range startDrainers {
		if start {
			go s.drainLane(s.lanes[laneID], false)
		}
	}
	for i, req := range reqs {
		if err := <-req.result; err != nil {
			return lsns[:i], err
		}
	}
	return lsns, nil
}

func (s *Store) Read(lba uint32) ([]byte, error) {
	if lba >= s.numBlocks {
		return nil, fmt.Errorf("parallelwal: LBA %d out of range", lba)
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, errors.New("parallelwal: Read after Close")
	}
	if !s.recovered {
		s.mu.RUnlock()
		return nil, ErrNotRecovered
	}
	if latest, ok := s.latest[lba]; ok {
		out := append([]byte(nil), latest.data...)
		s.mu.RUnlock()
		return out, nil
	}
	if staged, ok := s.baseStage[lba]; ok {
		out := append([]byte(nil), staged...)
		s.mu.RUnlock()
		return out, nil
	}
	activeExtent := s.activeExtent
	s.mu.RUnlock()
	lock := &s.extentLocks[lba%uint32(len(s.extentLocks))]
	lock.RLock()
	defer lock.RUnlock()
	data := make([]byte, s.blockSize)
	if _, err := s.fd.ReadAt(data, s.extentOffsetFor(activeExtent, lba)); err != nil {
		return nil, fmt.Errorf("parallelwal: read extent LBA %d: %w", lba, err)
	}
	return data, nil
}

func (s *Store) Sync() (uint64, error) {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, errors.New("parallelwal: Sync after Close")
	}
	if !s.recovered {
		s.mu.Unlock()
		return 0, ErrNotRecovered
	}
	fence := s.nextLSN - 1
	for s.publishedLSN < fence && s.terminalErr == nil {
		s.cond.Wait()
	}
	if s.terminalErr != nil {
		err := s.terminalErr
		s.mu.Unlock()
		return 0, err
	}
	target := s.publishedLSN
	heads := s.publishedHeads
	checkpoint := s.checkpointLSN
	tails := s.hdr.LaneTails
	walTail := s.walTail
	activeExtent := s.activeExtent
	needsBaseCheckpoint := s.baseCommitPending
	needsCheckpoint := needsBaseCheckpoint
	checkpointExtent := activeExtent
	if needsBaseCheckpoint {
		if !s.baseStageActive {
			s.mu.Unlock()
			return 0, errors.New("parallelwal: BASE frontier has no prepared extent")
		}
		checkpointExtent = s.baseStageSlot
	}
	for laneID := 0; laneID < s.laneCount; laneID++ {
		if heads[laneID]-tails[laneID] > s.retainPerLane {
			needsCheckpoint = true
		}
	}
	var blocks map[uint32][]byte
	if needsCheckpoint {
		blocks = make(map[uint32][]byte)
		for lba, latest := range s.latest {
			if latest.lsn <= target {
				blocks[lba] = append([]byte(nil), latest.data...)
			}
		}
	}
	s.mu.Unlock()

	if err := s.fd.Sync(); err != nil {
		return 0, fmt.Errorf("parallelwal: sync lane WAL: %w", err)
	}
	if !needsBaseCheckpoint {
		if err := s.persistHeader(target, checkpoint, heads, tails, walTail, activeExtent); err != nil {
			return 0, err
		}
		s.mu.Lock()
		if target > s.stableLSN {
			s.stableLSN = target
		}
		s.mu.Unlock()
	}
	if !needsCheckpoint {
		return target, nil
	}

	checkpointWriteOps, err := s.writeCheckpointBlocks(checkpointExtent, blocks)
	if err != nil {
		return 0, err
	}
	if err := s.fd.Sync(); err != nil {
		return 0, fmt.Errorf("parallelwal: sync checkpoint extent: %w", err)
	}
	newTails, newWALTail, recycleReadOps, err := s.recycleStablePrefix(heads)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.reuseFence = true
	s.mu.Unlock()
	if err := s.persistHeader(target, target, heads, newTails, newWALTail, checkpointExtent); err != nil {
		s.mu.Lock()
		s.reuseFence = false
		s.cond.Broadcast()
		s.mu.Unlock()
		return 0, err
	}
	sealErr := s.persistHeader(target, target, heads, newTails, newWALTail, checkpointExtent)
	s.mu.Lock()
	if target > s.checkpointLSN {
		s.checkpointLSN = target
	}
	if s.pendingBaseLSN <= target {
		s.pendingBaseLSN = 0
	}
	s.baseCommitPending = false
	s.checkpointWriteOps = uint64(checkpointWriteOps)
	s.recycleReadOps = uint64(recycleReadOps)
	if target > s.stableLSN {
		s.stableLSN = target
	}
	if needsBaseCheckpoint {
		s.activeExtent = checkpointExtent
		s.baseStageActive = false
		s.baseStageSlot = 1 - checkpointExtent
		s.baseStage = nil
	}
	s.walTail = newWALTail
	for lsn := range s.history {
		if lsn < newWALTail {
			delete(s.history, lsn)
		}
	}
	if sealErr != nil && s.terminalErr == nil {
		s.terminalErr = fmt.Errorf("parallelwal: seal recycled checkpoint header: %w", sealErr)
	}
	s.reuseFence = false
	s.cond.Broadcast()
	s.mu.Unlock()
	if sealErr != nil {
		return 0, fmt.Errorf("parallelwal: seal recycled checkpoint header: %w", sealErr)
	}
	return target, nil
}

func (s *Store) writeCheckpointBlocks(extent int, blocks map[uint32][]byte) (int, error) {
	if len(blocks) == 0 {
		return 0, nil
	}
	lbas := make([]uint32, 0, len(blocks))
	for lba := range blocks {
		lbas = append(lbas, lba)
	}
	sort.Slice(lbas, func(i, j int) bool { return lbas[i] < lbas[j] })

	maxBlocks := maxCheckpointWriteBytes / int(s.blockSize)
	if maxBlocks < 1 {
		maxBlocks = 1
	}
	writeOps := 0
	for first := 0; first < len(lbas); {
		last := first + 1
		for last < len(lbas) &&
			last-first < maxBlocks &&
			lbas[last] == lbas[last-1]+1 {
			last++
		}

		lockIDs := make([]int, 0, last-first)
		seenLocks := make(map[int]struct{}, last-first)
		for _, lba := range lbas[first:last] {
			lockID := int(lba % uint32(len(s.extentLocks)))
			if _, exists := seenLocks[lockID]; !exists {
				seenLocks[lockID] = struct{}{}
				lockIDs = append(lockIDs, lockID)
			}
		}
		sort.Ints(lockIDs)
		for _, lockID := range lockIDs {
			s.extentLocks[lockID].Lock()
		}

		buf := make([]byte, (last-first)*int(s.blockSize))
		for i, lba := range lbas[first:last] {
			copy(buf[i*int(s.blockSize):], blocks[lba])
		}
		_, err := s.fd.WriteAt(buf, s.extentOffsetFor(extent, lbas[first]))
		for i := len(lockIDs) - 1; i >= 0; i-- {
			s.extentLocks[lockIDs[i]].Unlock()
		}
		if err != nil {
			return writeOps, fmt.Errorf("parallelwal: checkpoint extent LBA range [%d,%d]: %w",
				lbas[first], lbas[last-1], err)
		}
		writeOps++
		first = last
	}
	return writeOps, nil
}

func (s *Store) persistHeader(
	durable, checkpoint uint64,
	heads, tails [maxLaneCount]uint64,
	walTail uint64,
	activeExtent int,
) error {
	s.mu.Lock()
	h := s.hdr
	h.Generation++
	h.DurableLSN = durable
	h.CheckpointLSN = checkpoint
	h.WALTail = walTail
	h.LaneHeads = heads
	h.LaneTails = tails
	h.ActiveExtent = uint8(activeExtent)
	nextSlot := 1 - s.headerSlot
	s.mu.Unlock()
	if err := writeHeaderAt(s.fd, nextSlot, h); err != nil {
		return err
	}
	if err := s.fd.Sync(); err != nil {
		return fmt.Errorf("parallelwal: fsync header generation %d: %w", h.Generation, err)
	}
	s.mu.Lock()
	s.hdr = h
	s.headerSlot = nextSlot
	s.mu.Unlock()
	return nil
}

func (s *Store) recycleStablePrefix(heads [maxLaneCount]uint64) ([maxLaneCount]uint64, uint64, int, error) {
	s.mu.RLock()
	h := s.hdr
	walTail := s.walTail
	s.mu.RUnlock()
	tails := h.LaneTails
	maxRecords := maxWALIOBytes / int(h.RecordSize)
	if maxRecords < 1 {
		maxRecords = 1
	}
	buf := make([]byte, maxRecords*int(h.RecordSize))
	readOps := 0
	for laneID := 0; laneID < int(h.LaneCount); laneID++ {
		head := heads[laneID]
		keep := h.RetainPerLane
		if head-tails[laneID] <= keep {
			continue
		}
		newTail := head - keep
		for seq := tails[laneID]; seq < newTail; {
			slot := seq % h.SlotsPerLane
			count := maxRecords
			if untilWrap := int(h.SlotsPerLane - slot); count > untilWrap {
				count = untilWrap
			}
			if remaining := int(newTail - seq); count > remaining {
				count = remaining
			}
			chunk := buf[:count*int(h.RecordSize)]
			off := s.lanes[laneID].base + int64(slot)*int64(h.RecordSize)
			n, err := s.fd.ReadAt(chunk, off)
			readOps++
			if err != nil && !errors.Is(err, io.EOF) {
				return tails, walTail, readOps, storage.NewSubstrateIOFailure(err,
					fmt.Sprintf("recycle read lane=%d seq-range=[%d,%d)", laneID, seq, seq+uint64(count)))
			}
			if n != len(chunk) {
				return tails, walTail, readOps, storage.NewSubstrateIOFailure(io.ErrUnexpectedEOF,
					fmt.Sprintf("recycle short read lane=%d seq-range=[%d,%d) bytes=%d",
						laneID, seq, seq+uint64(count), n))
			}
			for i := 0; i < count; i++ {
				recordBuf := chunk[i*int(h.RecordSize) : (i+1)*int(h.RecordSize)]
				rec, err := decodeRecord(recordBuf, int(h.BlockSize))
				if err != nil {
					return tails, walTail, readOps, storage.NewWALIntegrityFailure(nil,
						fmt.Sprintf("recycle decode lane=%d seq=%d: %v", laneID, seq+uint64(i), err))
				}
				if rec.LSN+1 > walTail {
					walTail = rec.LSN + 1
				}
			}
			seq += uint64(count)
		}
		tails[laneID] = newTail
	}
	return tails, walTail, readOps, nil
}

func (s *Store) Recover() (uint64, error) {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.closing {
		return 0, errors.New("parallelwal: Recover after Close")
	}
	if len(s.pending) != 0 || s.inflightAppends != 0 {
		return 0, errors.New("parallelwal: Recover with writes in flight")
	}
	records, err := s.scanDurableRecords()
	if err != nil {
		return 0, err
	}
	s.latest = make(map[uint32]blockVersion)
	s.applied = make(map[uint32]uint64)
	s.history = make(map[uint64]walRecord)
	seen := make(map[uint64]struct{}, len(records))
	for _, rec := range records {
		if rec.LSN == 0 || rec.LSN > s.hdr.DurableLSN {
			return 0, storage.NewWALIntegrityFailure(nil,
				fmt.Sprintf("record LSN=%d outside durable frontier=%d", rec.LSN, s.hdr.DurableLSN))
		}
		if _, exists := seen[rec.LSN]; exists {
			return 0, storage.NewWALIntegrityFailure(nil, fmt.Sprintf("duplicate committed LSN=%d", rec.LSN))
		}
		seen[rec.LSN] = struct{}{}
		s.history[rec.LSN] = rec
		// Records at or below the checkpoint are retained only for
		// catch-up. Their bytes are already represented by the extent
		// and must not override a later BASE rebuild written there.
		if rec.LSN > s.hdr.CheckpointLSN && rec.LSN > s.applied[rec.LBA] {
			s.applied[rec.LBA] = rec.LSN
			s.latest[rec.LBA] = blockVersion{lsn: rec.LSN, data: append([]byte(nil), rec.Data...)}
		}
	}
	if s.hdr.CheckpointLSN < s.hdr.DurableLSN {
		replayFloor := s.hdr.CheckpointLSN + 1
		if s.hdr.WALTail > replayFloor {
			replayFloor = s.hdr.WALTail
		}
		if replayFloor <= s.hdr.DurableLSN {
			for lsn := replayFloor; ; lsn++ {
				if _, ok := seen[lsn]; !ok {
					return 0, storage.NewWALIntegrityFailure(nil,
						fmt.Sprintf("committed WAL hole LSN=%d checkpoint=%d durable=%d",
							lsn, s.hdr.CheckpointLSN, s.hdr.DurableLSN))
				}
				if lsn == s.hdr.DurableLSN {
					break
				}
			}
		}
	}
	s.nextLSN = s.hdr.DurableLSN + 1
	s.stableLSN = s.hdr.DurableLSN
	s.checkpointLSN = s.hdr.CheckpointLSN
	s.pendingBaseLSN = 0
	s.baseCommitPending = false
	s.walTail = s.hdr.WALTail
	s.publishedLSN = s.hdr.DurableLSN
	s.publishedHeads = s.hdr.LaneHeads
	s.activeExtent = int(s.hdr.ActiveExtent)
	s.baseStageActive = false
	s.baseStageSlot = 1 - s.activeExtent
	s.baseStage = nil
	for i, l := range s.lanes {
		l.nextSeq = s.hdr.LaneHeads[i]
		l.mu.Lock()
		l.completedSeq = s.hdr.LaneHeads[i]
		l.mu.Unlock()
	}
	s.recovered = true
	return s.stableLSN, nil
}

func (s *Store) scanDurableRecords() ([]walRecord, error) {
	records := make([]walRecord, 0)
	buf := make([]byte, s.hdr.RecordSize)
	for laneID := 0; laneID < int(s.hdr.LaneCount); laneID++ {
		l := s.lanes[laneID]
		for seq := s.hdr.LaneTails[laneID]; seq < s.hdr.LaneHeads[laneID]; seq++ {
			slot := seq % s.hdr.SlotsPerLane
			off := l.base + int64(slot)*int64(s.hdr.RecordSize)
			n, err := s.fd.ReadAt(buf, off)
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, storage.NewSubstrateIOFailure(err,
					fmt.Sprintf("read lane=%d seq=%d", laneID, seq))
			}
			if n != len(buf) {
				return nil, storage.NewSubstrateIOFailure(io.ErrUnexpectedEOF,
					fmt.Sprintf("short read lane=%d seq=%d bytes=%d want=%d", laneID, seq, n, len(buf)))
			}
			rec, err := decodeRecord(buf, int(s.hdr.BlockSize))
			if err != nil {
				return nil, storage.NewWALIntegrityFailure(nil,
					fmt.Sprintf("decode lane=%d seq=%d: %v", laneID, seq, err))
			}
			if rec.LBA >= s.numBlocks {
				return nil, storage.NewWALIntegrityFailure(nil,
					fmt.Sprintf("record LSN=%d LBA=%d outside numBlocks=%d", rec.LSN, rec.LBA, s.numBlocks))
			}
			if rec.Flags != flagWrite {
				return nil, storage.NewWALIntegrityFailure(nil,
					fmt.Sprintf("record LSN=%d has unsupported flags=%d", rec.LSN, rec.Flags))
			}
			if want := s.laneFor(rec.LBA); want != laneID {
				return nil, storage.NewWALIntegrityFailure(nil,
					fmt.Sprintf("lane mapping mismatch LSN=%d LBA=%d got=%d want=%d", rec.LSN, rec.LBA, laneID, want))
			}
			records = append(records, rec)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].LSN < records[j].LSN })
	return records, nil
}

func (s *Store) Boundaries() (R, S, H uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.publishedLSN == 0 {
		return s.stableLSN, 0, 0
	}
	return s.stableLSN, s.walTail, s.publishedLSN
}

func (s *Store) NextLSN() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextLSN
}

func (s *Store) NumBlocks() uint32 { return s.numBlocks }
func (s *Store) BlockSize() int    { return int(s.blockSize) }

func (s *Store) AdvanceFrontier(lsn uint64) {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	for (len(s.pending) != 0 || s.inflightAppends != 0) && s.terminalErr == nil {
		s.cond.Wait()
	}
	if s.terminalErr != nil {
		return
	}
	if lsn == ^uint64(0) {
		s.terminalErr = errors.New("parallelwal: BASE frontier overflows next LSN")
		s.cond.Broadcast()
		return
	}
	if lsn > s.publishedLSN {
		s.publishedLSN = lsn
		s.nextLSN = lsn + 1
	}
	committingBase := s.baseStageActive
	if committingBase {
		s.pendingBaseLSN = lsn
		s.baseCommitPending = true
	}
	if lsn+1 > s.walTail {
		s.walTail = lsn + 1
	}
	for historyLSN := range s.history {
		if historyLSN <= lsn {
			delete(s.history, historyLSN)
		}
	}
	if committingBase {
		for lba, latest := range s.latest {
			if latest.lsn <= lsn {
				delete(s.latest, lba)
			}
		}
		for lba, appliedLSN := range s.applied {
			if appliedLSN <= lsn {
				delete(s.applied, lba)
			}
		}
	}
}

func (s *Store) AdvanceWALTail(newTail uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newTail > s.walTail {
		s.walTail = newTail
	}
}

func (s *Store) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	_, err := s.submit(lba, data, lsn)
	return err
}

// BeginBaseInstall resets the inactive extent used for the next rebuild.
// The active extent remains authoritative until Sync publishes a header that
// atomically switches ActiveExtent.
func (s *Store) BeginBaseInstall() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	return s.resetBaseStageUnderSync()
}

func (s *Store) WriteExtentDirect(lba uint32, data []byte) error {
	if lba >= s.numBlocks {
		return fmt.Errorf("parallelwal: WriteExtentDirect LBA %d out of range", lba)
	}
	if len(data) != int(s.blockSize) {
		return fmt.Errorf("parallelwal: WriteExtentDirect data size %d != block size %d", len(data), s.blockSize)
	}
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if err := s.ensureBaseStageUnderSync(); err != nil {
		return err
	}
	s.mu.RLock()
	if s.closed || s.closing {
		s.mu.RUnlock()
		return errors.New("parallelwal: WriteExtentDirect after Close")
	}
	if !s.recovered {
		s.mu.RUnlock()
		return ErrNotRecovered
	}
	stageSlot := s.baseStageSlot
	s.mu.RUnlock()
	lock := &s.extentLocks[lba%uint32(len(s.extentLocks))]
	lock.Lock()
	defer lock.Unlock()
	if _, err := s.fd.WriteAt(data, s.extentOffsetFor(stageSlot, lba)); err != nil {
		return fmt.Errorf("parallelwal: WriteExtentDirect LBA %d: %w", lba, err)
	}
	s.mu.Lock()
	s.baseStage[lba] = append([]byte(nil), data...)
	delete(s.latest, lba)
	delete(s.applied, lba)
	s.mu.Unlock()
	return nil
}

func (s *Store) ensureBaseStageUnderSync() error {
	s.mu.RLock()
	active := s.baseStageActive
	s.mu.RUnlock()
	if active {
		return nil
	}
	return s.resetBaseStageUnderSync()
}

func (s *Store) resetBaseStageUnderSync() error {
	s.mu.RLock()
	if s.closed || s.closing {
		s.mu.RUnlock()
		return errors.New("parallelwal: BeginBaseInstall after Close")
	}
	if !s.recovered {
		s.mu.RUnlock()
		return ErrNotRecovered
	}
	if s.terminalErr != nil {
		err := s.terminalErr
		s.mu.RUnlock()
		return err
	}
	stageSlot := 1 - s.activeExtent
	stageBase := s.extentBases[stageSlot]
	extentBytes := int64(s.numBlocks) * int64(s.blockSize)
	h := s.hdr
	activeExtent := s.activeExtent
	s.mu.RUnlock()

	// Make both valid header generations reference the current active extent
	// before reusing the inactive one. This preserves dual-header fallback if
	// the newest header is later damaged while the next BASE stage is partial.
	if err := s.persistHeader(
		h.DurableLSN,
		h.CheckpointLSN,
		h.LaneHeads,
		h.LaneTails,
		h.WALTail,
		activeExtent,
	); err != nil {
		return fmt.Errorf("parallelwal: seal active extent before BASE stage: %w", err)
	}

	s.mu.Lock()
	s.baseStageActive = false
	s.baseStage = nil
	s.mu.Unlock()

	const zeroChunkSize = 1 << 20
	chunkSize := int64(zeroChunkSize)
	if extentBytes < chunkSize {
		chunkSize = extentBytes
	}
	zero := make([]byte, int(chunkSize))
	for offset := int64(0); offset < extentBytes; offset += chunkSize {
		n := chunkSize
		if remaining := extentBytes - offset; remaining < n {
			n = remaining
		}
		if _, err := s.fd.WriteAt(zero[:int(n)], stageBase+offset); err != nil {
			return fmt.Errorf("parallelwal: reset BASE stage at offset %d: %w", offset, err)
		}
	}

	s.mu.Lock()
	s.baseStageActive = true
	s.baseStageSlot = stageSlot
	s.baseStage = make(map[uint32][]byte)
	s.mu.Unlock()
	return nil
}

func (s *Store) AllBlocks() map[uint32][]byte {
	out := make(map[uint32][]byte)
	zero := make([]byte, s.blockSize)
	for lba := uint32(0); lba < s.numBlocks; lba++ {
		data, err := s.Read(lba)
		if err == nil && !bytes.Equal(data, zero) {
			out[lba] = data
		}
	}
	return out
}

func (s *Store) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	if fn == nil {
		return errors.New("parallelwal: ScanLBAs nil callback")
	}
	s.mu.RLock()
	if fromLSN+1 < s.walTail {
		floor := s.walTail
		s.mu.RUnlock()
		return storage.NewWALRecycledFailure(nil,
			fmt.Sprintf("fromLSN=%d walTail=%d", fromLSN, floor))
	}
	head := s.publishedLSN
	records := make([]walRecord, 0, len(s.history))
	for lsn, rec := range s.history {
		if lsn > fromLSN && lsn <= head {
			records = append(records, walRecord{
				LSN: rec.LSN, LBA: rec.LBA, Flags: rec.Flags, Data: append([]byte(nil), rec.Data...),
			})
		}
	}
	s.mu.RUnlock()
	sort.Slice(records, func(i, j int) bool { return records[i].LSN < records[j].LSN })
	for _, rec := range records {
		if err := fn(storage.RecoveryEntry{
			LSN: rec.LSN, LBA: rec.LBA, Flags: storage.RecoveryEntryWrite, Data: rec.Data,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) RecoveryMode() storage.RecoveryMode {
	return storage.RecoveryModeWALReplay
}

func (s *Store) AppliedLSNs() (map[uint32]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[uint32]uint64, len(s.applied))
	for lba, lsn := range s.applied {
		out[lba] = lsn
	}
	return out, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	if s.closing {
		for !s.closed {
			s.cond.Wait()
		}
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	for len(s.pending) != 0 || s.inflightAppends != 0 {
		s.cond.Wait()
	}
	s.mu.Unlock()

	_, syncErr := s.Sync()
	s.mu.Lock()
	s.closed = true
	s.closing = false
	s.cond.Broadcast()
	s.mu.Unlock()
	closeErr := s.fd.Close()
	return errors.Join(syncErr, closeErr)
}

func (s *Store) extentOffsetFor(extent int, lba uint32) int64 {
	return s.extentBases[extent] + int64(lba)*int64(s.blockSize)
}

var (
	_ storage.LogicalStorage      = (*Store)(nil)
	_ storage.BaseInstallPreparer = (*Store)(nil)
)
