package parallelwal

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

type segmentRecordingWriter struct {
	mu         sync.Mutex
	writes     [][]byte
	offsets    []int64
	firstWrite chan struct{}
	release    chan struct{}
	once       sync.Once
}

func (w *segmentRecordingWriter) WriteAt(data []byte, offset int64) (int, error) {
	w.once.Do(func() {
		if w.firstWrite != nil {
			close(w.firstWrite)
		}
		if w.release != nil {
			<-w.release
		}
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, append([]byte(nil), data...))
	w.offsets = append(w.offsets, offset)
	return len(data), nil
}

func (w *segmentRecordingWriter) snapshot() ([][]byte, []int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	writes := make([][]byte, len(w.writes))
	for i := range w.writes {
		writes[i] = append([]byte(nil), w.writes[i]...)
	}
	return writes, append([]int64(nil), w.offsets...)
}

func segmentOwnerTestConfig() segmentOwnerConfig {
	return segmentOwnerConfig{
		BlockSize:            512,
		NumBlocks:            32,
		QueueDepth:           8,
		MaxEntriesPerSegment: 4,
		MaxLogBytes:          1 << 20,
	}
}

func waitForSegmentOwnerMetric(t *testing.T, fn func(segmentOwnerMetrics) bool, owner *segmentOwner) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn(owner.Metrics()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("segment owner metrics did not converge: %+v", owner.Metrics())
}

func waitForSegmentOwnerClosed(t *testing.T, owner *segmentOwner) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		owner.mu.Lock()
		closed := owner.closed
		owner.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("segment owner did not enter closed state")
}

func TestSegmentOwnerGroupsAlreadyQueuedWritesWithoutDelay(t *testing.T) {
	writer := &segmentRecordingWriter{
		firstWrite: make(chan struct{}),
		release:    make(chan struct{}),
	}
	owner, err := newSegmentOwner(writer, segmentOwnerTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		lsn uint64
		err error
	}
	first := make(chan result, 1)
	go func() {
		lsn, err := owner.Submit(3, testBlock(0x01, 512))
		first <- result{lsn: lsn, err: err}
	}()
	select {
	case <-writer.firstWrite:
	case <-time.After(2 * time.Second):
		t.Fatal("isolated write waited for a batching timer")
	}

	results := make(chan result, 4)
	for i := 0; i < 4; i++ {
		fill := byte(i + 2)
		go func() {
			lsn, err := owner.Submit(3, testBlock(fill, 512))
			results <- result{lsn: lsn, err: err}
		}()
	}
	waitForSegmentOwnerMetric(t, func(metrics segmentOwnerMetrics) bool {
		return metrics.AdmittedRequests == 5 && metrics.QueueHighWater >= 4
	}, owner)
	close(writer.release)

	if got := <-first; got.err != nil || got.lsn != 1 {
		t.Fatalf("first result=%+v", got)
	}
	seen := make(map[uint64]bool)
	for i := 0; i < 4; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		seen[got.lsn] = true
	}
	for lsn := uint64(2); lsn <= 5; lsn++ {
		if !seen[lsn] {
			t.Fatalf("missing LSN %d from results %v", lsn, seen)
		}
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}

	writes, offsets := writer.snapshot()
	if len(writes) != 2 {
		t.Fatalf("WriteAt calls=%d want=2", len(writes))
	}
	if offsets[0] != 0 || offsets[1] != int64(len(writes[0])) {
		t.Fatalf("WriteAt offsets=%v", offsets)
	}
	firstSegment, err := decodeSegment(writes[0], 512, 32)
	if err != nil {
		t.Fatal(err)
	}
	secondSegment, err := decodeSegment(writes[1], 512, 32)
	if err != nil {
		t.Fatal(err)
	}
	if firstSegment.Sequence != 1 || len(firstSegment.Records) != 1 ||
		secondSegment.Sequence != 2 || len(secondSegment.Records) != 4 {
		t.Fatalf("segment shapes=%d/%d and %d/%d",
			firstSegment.Sequence, len(firstSegment.Records),
			secondSegment.Sequence, len(secondSegment.Records))
	}
	for i, record := range secondSegment.Records {
		if record.LSN != uint64(i+2) || record.LBA != 3 {
			t.Fatalf("second segment record %d=%+v", i, record)
		}
	}
	metrics := owner.Metrics()
	if metrics.SegmentsWritten != 2 || metrics.EntriesWritten != 5 ||
		metrics.AdmittedRequests != 5 || metrics.QueueFullRejects != 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestSegmentOwnerQueueIsBoundedAndRejectsWithoutLSNHole(t *testing.T) {
	writer := &segmentRecordingWriter{
		firstWrite: make(chan struct{}),
		release:    make(chan struct{}),
	}
	config := segmentOwnerTestConfig()
	config.QueueDepth = 2
	config.MaxEntriesPerSegment = 1
	owner, err := newSegmentOwner(writer, config)
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 3)
	go func() {
		_, err := owner.Submit(0, testBlock(1, 512))
		results <- err
	}()
	<-writer.firstWrite
	for i := 0; i < 2; i++ {
		go func(fill byte) {
			_, err := owner.Submit(uint32(fill), testBlock(fill, 512))
			results <- err
		}(byte(i + 1))
	}
	waitForSegmentOwnerMetric(t, func(metrics segmentOwnerMetrics) bool {
		return metrics.AdmittedRequests == 3 && metrics.QueueHighWater == 2
	}, owner)
	if _, err := owner.Submit(7, testBlock(7, 512)); !errors.Is(err, errSegmentQueueFull) {
		t.Fatalf("queue-full error=%v", err)
	}
	close(writer.release)
	for i := 0; i < 3; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if lsn, err := owner.Submit(8, testBlock(8, 512)); err != nil || lsn != 4 {
		t.Fatalf("post-rejection submit=(%d,%v) want=(4,nil)", lsn, err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	metrics := owner.Metrics()
	if metrics.AdmittedRequests != 4 || metrics.QueueFullRejects != 1 || metrics.QueueHighWater != 2 ||
		metrics.OwnedBytesHighWater > uint64(config.QueueDepth+config.MaxEntriesPerSegment)*uint64(config.BlockSize) {
		t.Fatalf("bounded metrics=%+v", metrics)
	}

	writes, _ := writer.snapshot()
	var lsns []uint64
	for _, encoded := range writes {
		segment, err := decodeSegment(encoded, config.BlockSize, config.NumBlocks)
		if err != nil {
			t.Fatal(err)
		}
		lsns = append(lsns, segment.Records[0].LSN)
	}
	for i, lsn := range lsns {
		if want := uint64(i + 1); lsn != want {
			t.Fatalf("written LSNs=%v; index %d want=%d", lsns, i, want)
		}
	}
}

type shortSegmentWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *shortSegmentWriter) WriteAt(data []byte, _ int64) (int, error) {
	w.once.Do(func() {
		close(w.entered)
		<-w.release
	})
	return len(data) - 1, nil
}

func TestSegmentOwnerShortWriteTerminallyFailsQueuedRequests(t *testing.T) {
	config := segmentOwnerTestConfig()
	writer := &shortSegmentWriter{entered: make(chan struct{}), release: make(chan struct{})}
	owner, err := newSegmentOwner(writer, config)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 3)
	go func() {
		_, err := owner.Submit(0, testBlock(1, 512))
		results <- err
	}()
	<-writer.entered
	for i := 0; i < 2; i++ {
		go func(lba uint32) {
			_, err := owner.Submit(lba, testBlock(byte(lba+1), 512))
			results <- err
		}(uint32(i + 1))
	}
	waitForSegmentOwnerMetric(t, func(metrics segmentOwnerMetrics) bool {
		return metrics.AdmittedRequests == 3
	}, owner)
	close(writer.release)
	for i := 0; i < 3; i++ {
		if err := <-results; !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("admitted request %d short-write error=%v", i, err)
		}
	}
	if _, err := owner.Submit(1, testBlock(2, 512)); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("post-terminal error=%v", err)
	}
	if err := owner.Close(); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Close error=%v", err)
	}
	metrics := owner.Metrics()
	if metrics.SegmentsWritten != 0 || metrics.EntriesWritten != 0 {
		t.Fatalf("short-write metrics=%+v", metrics)
	}
}

func TestSegmentOwnerLogByteCeilingFailsClosed(t *testing.T) {
	writer := &segmentRecordingWriter{}
	config := segmentOwnerTestConfig()
	oneSegmentBytes, err := segmentEncodedSize(1, config.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxLogBytes = int64(oneSegmentBytes)
	owner, err := newSegmentOwner(writer, config)
	if err != nil {
		t.Fatal(err)
	}
	if lsn, err := owner.Submit(0, testBlock(1, 512)); err != nil || lsn != 1 {
		t.Fatalf("first submit=(%d,%v)", lsn, err)
	}
	if _, err := owner.Submit(1, testBlock(2, 512)); !errors.Is(err, errSegmentLogFull) {
		t.Fatalf("log-full error=%v", err)
	}
	if _, err := owner.Submit(2, testBlock(3, 512)); !errors.Is(err, errSegmentLogFull) {
		t.Fatalf("post-terminal error=%v", err)
	}
	if err := owner.Close(); !errors.Is(err, errSegmentLogFull) {
		t.Fatalf("Close error=%v", err)
	}
	writes, _ := writer.snapshot()
	if len(writes) != 1 {
		t.Fatalf("WriteAt calls=%d want=1", len(writes))
	}
}

func TestSegmentOwnerCloseDrainsAdmittedRequests(t *testing.T) {
	writer := &segmentRecordingWriter{
		firstWrite: make(chan struct{}),
		release:    make(chan struct{}),
	}
	owner, err := newSegmentOwner(writer, segmentOwnerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 3)
	go func() {
		_, err := owner.Submit(0, testBlock(1, 512))
		results <- err
	}()
	<-writer.firstWrite
	for i := 0; i < 2; i++ {
		go func(lba uint32) {
			_, err := owner.Submit(lba, testBlock(byte(lba+1), 512))
			results <- err
		}(uint32(i + 1))
	}
	waitForSegmentOwnerMetric(t, func(metrics segmentOwnerMetrics) bool {
		return metrics.AdmittedRequests == 3
	}, owner)

	closeResult := make(chan error, 1)
	go func() { closeResult <- owner.Close() }()
	waitForSegmentOwnerClosed(t, owner)
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned before admitted writes drained: %v", err)
	default:
	}
	if _, err := owner.Submit(4, testBlock(4, 512)); !errors.Is(err, errSegmentOwnerClosed) {
		t.Fatalf("submit during Close error=%v", err)
	}
	close(writer.release)
	for i := 0; i < 3; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	metrics := owner.Metrics()
	if metrics.AdmittedRequests != 3 || metrics.EntriesWritten != 3 {
		t.Fatalf("drain metrics=%+v", metrics)
	}
}

func TestSegmentOwnerValidatesConfigAndInput(t *testing.T) {
	if _, err := newSegmentOwner(nil, segmentOwnerTestConfig()); err == nil {
		t.Fatal("nil writer accepted")
	}
	config := segmentOwnerTestConfig()
	config.MaxEntriesPerSegment = maxSegmentEntries + 1
	if _, err := newSegmentOwner(&segmentRecordingWriter{}, config); err == nil {
		t.Fatal("oversized segment entry count accepted")
	}
	config = segmentOwnerTestConfig()
	config.QueueDepth = maxSegmentQueueDepth + 1
	if _, err := newSegmentOwner(&segmentRecordingWriter{}, config); err == nil {
		t.Fatal("oversized queue depth accepted")
	}
	owner, err := newSegmentOwner(&segmentRecordingWriter{}, segmentOwnerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Submit(32, testBlock(1, 512)); err == nil {
		t.Fatal("out-of-range LBA accepted")
	}
	if _, err := owner.Submit(0, []byte{1}); err == nil {
		t.Fatal("short payload accepted")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Submit(0, testBlock(1, 512)); !errors.Is(err, errSegmentOwnerClosed) {
		t.Fatalf("post-close error=%v", err)
	}
}
