package parallelwal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func segmentDurableTestConfig() segmentOwnerConfig {
	return segmentOwnerConfig{
		BlockSize:            512,
		NumBlocks:            32,
		QueueDepth:           8,
		MaxEntriesPerSegment: 4,
		MaxLogBytes:          1 << 20,
	}
}

func createSegmentDurableTestFile(t *testing.T, config segmentOwnerConfig) (*os.File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "segmented-wal.bin")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(segmentDurableLogOffset + config.MaxLogBytes); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	return file, path
}

func TestSegmentDurableHeaderFallsBackToPriorValidGeneration(t *testing.T) {
	config := segmentDurableTestConfig()
	file, _ := createSegmentDurableTestFile(t, config)
	defer file.Close()
	first := segmentDurableHeader{
		Generation:  1,
		BlockSize:   config.BlockSize,
		NumBlocks:   config.NumBlocks,
		LogOffset:   segmentDurableLogOffset,
		MaxLogBytes: config.MaxLogBytes,
	}
	second := first
	second.Generation = 2
	second.CommittedBytes = 608
	second.SegmentCount = 1
	second.FirstSequence = 1
	second.FirstLSN = 1
	second.LastLSN = 1
	if err := writeSegmentDurableHeaderAt(file, 0, first); err != nil {
		t.Fatal(err)
	}
	if err := writeSegmentDurableHeaderAt(file, 1, second); err != nil {
		t.Fatal(err)
	}
	if got, slot, err := readBestSegmentDurableHeader(file); err != nil ||
		got.Generation != 2 || slot != 1 {
		t.Fatalf("best header=(generation=%d,slot=%d,err=%v)", got.Generation, slot, err)
	}
	if _, err := file.WriteAt([]byte{0xff}, segmentDurableHeaderSize+16); err != nil {
		t.Fatal(err)
	}
	if got, slot, err := readBestSegmentDurableHeader(file); err != nil ||
		got.Generation != 1 || slot != 0 {
		t.Fatalf("fallback header=(generation=%d,slot=%d,err=%v)", got.Generation, slot, err)
	}
}

func TestSegmentDurableHeaderRejectsResealedReservedAndManifestFields(t *testing.T) {
	header := segmentDurableHeader{
		Generation: 1, BlockSize: 512, NumBlocks: 8,
		LogOffset: segmentDurableLogOffset, MaxLogBytes: 1 << 20,
	}
	encoded, err := encodeSegmentDurableHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	reseal := func(buf []byte) {
		binary.LittleEndian.PutUint32(buf[segmentDurableHeaderCRCOffset:],
			crc32.ChecksumIEEE(buf[:segmentDurableHeaderCRCOffset]))
	}
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{
			name: "reserved byte",
			mutate: func(buf []byte) {
				buf[80] = 1
			},
		},
		{
			name: "manifest missing anchors",
			mutate: func(buf []byte) {
				binary.LittleEndian.PutUint64(buf[40:48], 608)
			},
		},
		{
			name: "committed bytes exceed log",
			mutate: func(buf []byte) {
				binary.LittleEndian.PutUint64(buf[40:48], 2<<20)
			},
		},
		{
			name: "physical range overflow",
			mutate: func(buf []byte) {
				binary.LittleEndian.PutUint64(buf[24:32], uint64(^uint64(0)>>1))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := append([]byte(nil), encoded[:]...)
			tc.mutate(buf)
			reseal(buf)
			if _, err := decodeSegmentDurableHeader(buf); !errors.Is(err, errBadSegmentDurableHeader) {
				t.Fatalf("decode error=%v", err)
			}
		})
	}
}

func TestSegmentDurableSyncPersistsOnlyTrustedPrefix(t *testing.T) {
	config := segmentDurableTestConfig()
	file, path := createSegmentDurableTestFile(t, config)
	engine, err := newSegmentDurableEngine(file, config)
	if err != nil {
		t.Fatal(err)
	}
	for lba := uint32(0); lba < 3; lba++ {
		if lsn, err := engine.Submit(lba, testBlock(byte(lba+1), 512)); err != nil ||
			lsn != uint64(lba+1) {
			t.Fatalf("Submit(%d)=(%d,%v)", lba, lsn, err)
		}
	}
	if durable, err := engine.Sync(); err != nil || durable != 3 {
		t.Fatalf("Sync=(%d,%v)", durable, err)
	}
	if lsn, err := engine.Submit(3, testBlock(4, 512)); err != nil || lsn != 4 {
		t.Fatalf("unsynced Submit=(%d,%v)", lsn, err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	header, slot, err := readBestSegmentDurableHeader(reopened)
	if err != nil {
		t.Fatal(err)
	}
	if header.Generation != 2 || slot != 1 || header.LastLSN != 3 ||
		header.SegmentCount != 3 || header.CommittedBytes == 0 {
		t.Fatalf("durable header=%+v slot=%d", header, slot)
	}
	staged := make(map[uint32]walRecord)
	if err := scanCommittedSegments(
		reopened,
		header.recoveryWindow(),
		header.BlockSize,
		header.NumBlocks,
		func(segment walSegment) error {
			for _, record := range segment.Records {
				staged[record.LBA] = record
			}
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if len(staged) != 3 {
		t.Fatalf("recovered blocks=%d want=3", len(staged))
	}
	if _, exists := staged[3]; exists {
		t.Fatal("physical uncommitted tail became recoverable")
	}
}

type blockingSegmentDurableFile struct {
	*os.File
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingSegmentDurableFile) WriteAt(data []byte, offset int64) (int, error) {
	if offset >= segmentDurableLogOffset {
		f.once.Do(func() {
			close(f.entered)
			<-f.release
		})
	}
	return f.File.WriteAt(data, offset)
}

func TestSegmentDurableSyncFencesAdmittedWrite(t *testing.T) {
	config := segmentDurableTestConfig()
	raw, _ := createSegmentDurableTestFile(t, config)
	file := &blockingSegmentDurableFile{
		File: raw, entered: make(chan struct{}), release: make(chan struct{}),
	}
	engine, err := newSegmentDurableEngine(file, config)
	if err != nil {
		t.Fatal(err)
	}
	writeResult := make(chan error, 1)
	go func() {
		_, err := engine.Submit(0, testBlock(1, 512))
		writeResult <- err
	}()
	<-file.entered
	syncResult := make(chan error, 1)
	go func() {
		durable, err := engine.Sync()
		if err == nil && durable != 1 {
			err = errors.New("unexpected durable LSN")
		}
		syncResult <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		engine.owner.mu.Lock()
		waiters := engine.owner.publicationWaiters
		engine.owner.mu.Unlock()
		if waiters == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	engine.owner.mu.Lock()
	waiters := engine.owner.publicationWaiters
	engine.owner.mu.Unlock()
	if waiters != 1 {
		t.Fatal("Sync did not wait on the admitted target LSN")
	}
	select {
	case err := <-syncResult:
		t.Fatalf("Sync returned before admitted write: %v", err)
	default:
	}
	close(file.release)
	if err := <-writeResult; err != nil {
		t.Fatal(err)
	}
	if err := <-syncResult; err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}

var (
	errInjectedSegmentWrite  = errors.New("injected segmented write failure")
	errInjectedSegmentSync   = errors.New("injected segmented sync failure")
	errInjectedSegmentHeader = errors.New("injected segmented header failure")
)

type faultSegmentDurableFile struct {
	*os.File
	mu               sync.Mutex
	syncCalls        int
	failSyncCall     int
	blockSyncCall    int
	syncEntered      chan struct{}
	releaseSync      chan struct{}
	failHeaderOffset int64
}

type errorSegmentWriterAt struct{}

func (errorSegmentWriterAt) WriteAt([]byte, int64) (int, error) {
	return 0, errInjectedSegmentWrite
}

func TestSegmentOwnerWriteErrorTerminallyFaultsOwner(t *testing.T) {
	owner, err := newSegmentOwner(errorSegmentWriterAt{}, segmentOwnerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Submit(0, testBlock(1, 512)); !errors.Is(err, errInjectedSegmentWrite) {
		t.Fatalf("write error=%v", err)
	}
	if _, err := owner.Submit(1, testBlock(2, 512)); !errors.Is(err, errInjectedSegmentWrite) {
		t.Fatalf("post-write-error submit=%v", err)
	}
	if err := owner.Close(); !errors.Is(err, errInjectedSegmentWrite) {
		t.Fatalf("Close error=%v", err)
	}
}

func (f *faultSegmentDurableFile) WriteAt(data []byte, offset int64) (int, error) {
	f.mu.Lock()
	fail := f.failHeaderOffset >= 0 && offset == f.failHeaderOffset
	f.mu.Unlock()
	if fail {
		return 0, errInjectedSegmentHeader
	}
	return f.File.WriteAt(data, offset)
}

func (f *faultSegmentDurableFile) Sync() error {
	f.mu.Lock()
	f.syncCalls++
	call := f.syncCalls
	block := f.blockSyncCall != 0 && call == f.blockSyncCall
	fail := f.failSyncCall != 0 && call == f.failSyncCall
	f.mu.Unlock()
	if block {
		close(f.syncEntered)
		<-f.releaseSync
	}
	if fail {
		return errInjectedSegmentSync
	}
	return f.File.Sync()
}

func TestSegmentDurableSyncFailureTerminallyFaultsOwner(t *testing.T) {
	config := segmentDurableTestConfig()
	raw, _ := createSegmentDurableTestFile(t, config)
	file := &faultSegmentDurableFile{File: raw, failSyncCall: 2, failHeaderOffset: -1}
	engine, err := newSegmentDurableEngine(file, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(0, testBlock(1, 512)); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Sync(); !errors.Is(err, errInjectedSegmentSync) {
		t.Fatalf("Sync error=%v", err)
	}
	if _, err := engine.Submit(1, testBlock(2, 512)); !errors.Is(err, errInjectedSegmentSync) {
		t.Fatalf("post-Sync submit error=%v", err)
	}
	if err := engine.Close(); !errors.Is(err, errInjectedSegmentSync) {
		t.Fatalf("Close error=%v", err)
	}
	header, _, err := readBestSegmentDurableHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	if header.Generation != 1 || header.LastLSN != 0 {
		t.Fatalf("failed Sync advanced header=%+v", header)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentDurableFailureBarrierBlocksFuturePublication(t *testing.T) {
	config := segmentDurableTestConfig()
	raw, _ := createSegmentDurableTestFile(t, config)
	file := &faultSegmentDurableFile{
		File:             raw,
		failSyncCall:     2,
		blockSyncCall:    2,
		syncEntered:      make(chan struct{}),
		releaseSync:      make(chan struct{}),
		failHeaderOffset: -1,
	}
	engine, err := newSegmentDurableEngine(file, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(0, testBlock(1, 512)); err != nil {
		t.Fatal(err)
	}
	syncResult := make(chan error, 1)
	go func() {
		_, err := engine.Sync()
		syncResult <- err
	}()
	<-file.syncEntered

	futureResult := make(chan error, 1)
	go func() {
		_, err := engine.Submit(1, testBlock(2, 512))
		futureResult <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		engine.owner.mu.Lock()
		waiters := engine.owner.barrierWaiters
		engine.owner.mu.Unlock()
		if waiters == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	engine.owner.mu.Lock()
	waiters := engine.owner.barrierWaiters
	engine.owner.mu.Unlock()
	if waiters != 1 {
		t.Fatal("future write did not reach the durability publication barrier")
	}
	select {
	case err := <-futureResult:
		t.Fatalf("future write escaped active durability barrier: %v", err)
	default:
	}

	close(file.releaseSync)
	if err := <-syncResult; !errors.Is(err, errInjectedSegmentSync) {
		t.Fatalf("Sync error=%v", err)
	}
	if err := <-futureResult; !errors.Is(err, errInjectedSegmentSync) {
		t.Fatalf("future write error=%v", err)
	}
	if err := engine.Close(); !errors.Is(err, errInjectedSegmentSync) {
		t.Fatalf("Close error=%v", err)
	}
	if metrics := engine.owner.Metrics(); metrics.EntriesWritten != 1 {
		t.Fatalf("future write published after Sync failure: %+v", metrics)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentDurableHeaderWriteFailureTerminallyFaultsOwner(t *testing.T) {
	config := segmentDurableTestConfig()
	raw, _ := createSegmentDurableTestFile(t, config)
	file := &faultSegmentDurableFile{
		File: raw, failHeaderOffset: segmentDurableHeaderSize,
	}
	engine, err := newSegmentDurableEngine(file, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(0, testBlock(1, 512)); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Sync(); !errors.Is(err, errInjectedSegmentHeader) {
		t.Fatalf("Sync error=%v", err)
	}
	if _, err := engine.Submit(1, testBlock(2, 512)); !errors.Is(err, errInjectedSegmentHeader) {
		t.Fatalf("post-header-failure submit error=%v", err)
	}
	if err := engine.Close(); !errors.Is(err, errInjectedSegmentHeader) {
		t.Fatalf("Close error=%v", err)
	}
	header, _, err := readBestSegmentDurableHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	if header.Generation != 1 || header.LastLSN != 0 {
		t.Fatalf("failed header write advanced header=%+v", header)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentDurableHeaderSyncFailureTerminallyFaultsOwner(t *testing.T) {
	config := segmentDurableTestConfig()
	raw, _ := createSegmentDurableTestFile(t, config)
	file := &faultSegmentDurableFile{File: raw, failSyncCall: 3, failHeaderOffset: -1}
	engine, err := newSegmentDurableEngine(file, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Submit(0, testBlock(1, 512)); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Sync(); !errors.Is(err, errInjectedSegmentSync) {
		t.Fatalf("Sync error=%v", err)
	}
	if _, err := engine.Submit(1, testBlock(2, 512)); !errors.Is(err, errInjectedSegmentSync) {
		t.Fatalf("post-header-Sync submit error=%v", err)
	}
	if err := engine.Close(); !errors.Is(err, errInjectedSegmentSync) {
		t.Fatalf("Close error=%v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentOwnerExternalFailureCannotPublishActiveWrite(t *testing.T) {
	writer := &segmentRecordingWriter{}
	owner, err := newSegmentOwner(writer, segmentOwnerTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	beforePublish := make(chan struct{})
	releasePublish := make(chan struct{})
	owner.beforePublish = func() {
		close(beforePublish)
		<-releasePublish
	}
	result := make(chan error, 1)
	go func() {
		_, err := owner.Submit(0, testBlock(1, 512))
		result <- err
	}()
	<-beforePublish
	injected := errors.New("injected external durability failure")
	owner.Fail(injected)
	close(releasePublish)
	if err := <-result; !errors.Is(err, injected) {
		t.Fatalf("active result=%v", err)
	}
	if _, err := owner.WaitPublished(1); !errors.Is(err, injected) {
		t.Fatalf("WaitPublished error=%v", err)
	}
	if err := owner.Close(); !errors.Is(err, injected) {
		t.Fatalf("Close error=%v", err)
	}
	if metrics := owner.Metrics(); metrics.SegmentsWritten != 0 || metrics.EntriesWritten != 0 {
		t.Fatalf("externally failed write metrics=%+v", metrics)
	}
}

func TestSegmentDurableHeaderRejectsShortWrite(t *testing.T) {
	header := segmentDurableHeader{
		Generation: 1, BlockSize: 512, NumBlocks: 8,
		LogOffset: segmentDurableLogOffset, MaxLogBytes: 1 << 20,
	}
	if err := writeSegmentDurableHeaderAt(shortSegmentWriterAt{}, 0, header); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short header write error=%v", err)
	}
}

type shortSegmentWriterAt struct{}

func (shortSegmentWriterAt) WriteAt(data []byte, _ int64) (int, error) {
	return len(data) - 1, nil
}

type sequencedBlockingSegmentFile struct {
	*os.File
	mu       sync.Mutex
	logCalls int
	entered  [2]chan struct{}
	release  [2]chan struct{}
}

func (f *sequencedBlockingSegmentFile) WriteAt(data []byte, offset int64) (int, error) {
	if offset >= segmentDurableLogOffset {
		f.mu.Lock()
		index := f.logCalls
		f.logCalls++
		f.mu.Unlock()
		if index < len(f.entered) {
			close(f.entered[index])
			<-f.release[index]
		}
	}
	return f.File.WriteAt(data, offset)
}

func TestSegmentDurableSyncDoesNotWaitForFutureAdmission(t *testing.T) {
	config := segmentDurableTestConfig()
	raw, _ := createSegmentDurableTestFile(t, config)
	file := &sequencedBlockingSegmentFile{File: raw}
	for i := range file.entered {
		file.entered[i] = make(chan struct{})
		file.release[i] = make(chan struct{})
	}
	engine, err := newSegmentDurableEngine(file, config)
	if err != nil {
		t.Fatal(err)
	}
	firstResult := make(chan error, 1)
	go func() {
		_, err := engine.Submit(0, testBlock(1, 512))
		firstResult <- err
	}()
	<-file.entered[0]

	syncResult := make(chan struct {
		lsn uint64
		err error
	}, 1)
	go func() {
		lsn, err := engine.Sync()
		syncResult <- struct {
			lsn uint64
			err error
		}{lsn: lsn, err: err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		engine.owner.mu.Lock()
		waiters := engine.owner.publicationWaiters
		engine.owner.mu.Unlock()
		if waiters == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	engine.owner.mu.Lock()
	waiters := engine.owner.publicationWaiters
	engine.owner.mu.Unlock()
	if waiters != 1 {
		t.Fatal("Sync did not establish its target fence before future admission")
	}

	secondResult := make(chan error, 1)
	go func() {
		_, err := engine.Submit(1, testBlock(2, 512))
		secondResult <- err
	}()
	waitForSegmentOwnerMetric(t, func(metrics segmentOwnerMetrics) bool {
		return metrics.AdmittedRequests == 2
	}, engine.owner)
	close(file.release[0])
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
	<-file.entered[1]

	select {
	case got := <-syncResult:
		if got.err != nil || got.lsn != 1 {
			t.Fatalf("target Sync=(%d,%v)", got.lsn, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Sync waited for a write admitted after its target fence")
	}
	close(file.release[1])
	if err := <-secondResult; err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}
