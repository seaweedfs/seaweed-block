package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

const (
	phase173ControlStoreEnv = "SW_BLOCK_PHASE173_ARCH_CONTROL_STORE_DIR"
	phase173ControlRuns     = 5
	phase173ControlAPIOps   = 2320
	phase173ControlWarmup   = 80
	phase173ScratchBlocks   = 14500
	phase173ScratchPasses   = 3
)

type phase173ArchitectureControlResult struct {
	Contract               string  `json:"contract"`
	Control                string  `json:"control"`
	Scope                  string  `json:"scope"`
	Run                    int     `json:"run"`
	Writers                int     `json:"writers"`
	LogicalBlocks          uint64  `json:"logical_blocks"`
	LogicalBytes           uint64  `json:"logical_bytes"`
	DurationNanos          int64   `json:"duration_ns"`
	MiBPerSecond           float64 `json:"mib_per_second"`
	P99Nanos               int64   `json:"p99_ns,omitempty"`
	FinalSyncNanos         int64   `json:"final_sync_ns,omitempty"`
	FinalDrainNanos        int64   `json:"final_drain_ns,omitempty"`
	CommitLockWaitOps      uint64  `json:"commit_lock_wait_ops,omitempty"`
	CommitLockWaitNanos    uint64  `json:"commit_lock_wait_ns,omitempty"`
	WALAppendLockWaitNanos uint64  `json:"wal_append_lock_wait_ns,omitempty"`
	WALWriteAtCalls        uint64  `json:"wal_writeat_calls,omitempty"`
	FlushCycles            uint64  `json:"flush_cycles,omitempty"`
	FlushCycleNanos        uint64  `json:"flush_cycle_ns,omitempty"`
	FlushRecordReads       uint64  `json:"flush_record_reads,omitempty"`
	FlushDecodeOps         uint64  `json:"flush_decode_ops,omitempty"`
	ExtentWriteOps         uint64  `json:"extent_write_ops,omitempty"`
	ExtentSyncOps          uint64  `json:"extent_sync_ops,omitempty"`
	CheckpointSyncOps      uint64  `json:"checkpoint_sync_ops,omitempty"`
	ScratchPReadOps        uint64  `json:"scratch_pread_ops,omitempty"`
	ScratchPWriteOps       uint64  `json:"scratch_pwrite_ops,omitempty"`
	ScratchSyncOps         uint64  `json:"scratch_sync_ops,omitempty"`
	CorrectnessSamples     int     `json:"correctness_samples"`
}

func TestPhase173ArchitectureControlContract(t *testing.T) {
	for _, writers := range []int{1, 4} {
		cfg := phase173ControlConfig(writers)
		if cfg.APILogicalBlocks() != phase173ScratchBlocks {
			t.Fatalf("writers=%d logical blocks=%d want %d", writers, cfg.APILogicalBlocks(), phase173ScratchBlocks)
		}
		if cfg.WarmupLogicalBlocks() != 500 {
			t.Fatalf("writers=%d warmup blocks=%d want 500", writers, cfg.WarmupLogicalBlocks())
		}
	}
	for worker := 0; worker < 4; worker++ {
		start, end := phase173BalancedOperationRange(worker, 4, phase173ControlAPIOps)
		if end-start != phase173ControlAPIOps/4 || start%4 != 0 || end%4 != 0 {
			t.Fatalf("worker=%d range=[%d,%d) is not a balanced mixed-pattern partition", worker, start, end)
		}
	}
}

func TestPhase173ArchitectureControls(t *testing.T) {
	dir := os.Getenv(phase173ControlStoreEnv)
	if dir == "" {
		t.Skip("formal architecture controls require " + phase173ControlStoreEnv)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		control string
		writers int
		live    bool
	}{
		{control: "shipped_concurrent", writers: 4, live: true},
		{control: "deferred_foreground", writers: 1, live: false},
		{control: "deferred_foreground", writers: 4, live: false},
	} {
		path := filepath.Join(dir, fmt.Sprintf("phase173-control-%s-writers%d.store", tc.control, tc.writers))
		s := preparePhase173ControlStore(t, path, tc.writers)
		for run := 1; run <= phase173ControlRuns; run++ {
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
			foreground, flusher := runPhase173WALStoreControl(t, s, tc.control, tc.writers, tc.live, run)
			emitPhase173ArchitectureControl(t, foreground)
			if tc.control == "deferred_foreground" && tc.writers == 4 {
				emitPhase173ArchitectureControl(t, flusher)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}

	scratch := preparePhase173FileLayoutScratch(t, dir)
	defer scratch.close(t)
	for run := 1; run <= phase173ControlRuns; run++ {
		order := []bool{false, true}
		if run%2 == 0 {
			order = []bool{true, false}
		}
		for _, split := range order {
			runtime.GC()
			time.Sleep(100 * time.Millisecond)
			emitPhase173ArchitectureControl(t, scratch.run(t, split, run))
		}
	}
}

func phase173ControlConfig(writers int) phase173FixedWorkConfig {
	return phase173FixedWorkConfig{
		Shape:         "mounted_mixed",
		Writers:       writers,
		APIOperations: phase173ControlAPIOps,
		WarmupAPIOps:  phase173ControlWarmup,
	}
}

func preparePhase173ControlStore(t *testing.T, path string, writers int) *WALStore {
	t.Helper()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	s, err := CreateWALStore(path, phase173FixedWorkNumBlocks, phase173FixedWorkBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	cfg := phase173ControlConfig(writers)
	payloads := phase173Payloads(writers)
	if _, _, err := runPhase173Operations(s, cfg, cfg.APIOperations, 0, payloads, false); err != nil {
		t.Fatal(err)
	}
	synced, err := s.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.Stop(); err != nil {
		t.Fatal(err)
	}
	assertPhase173ControlDrained(t, s, synced)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recover(); err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	if err := s.flusher.Stop(); err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	return s
}

func runPhase173WALStoreControl(
	t *testing.T,
	s *WALStore,
	control string,
	writers int,
	live bool,
	run int,
) (phase173ArchitectureControlResult, phase173ArchitectureControlResult) {
	t.Helper()
	cfg := phase173ControlConfig(writers)
	payloads := phase173Payloads(writers)

	startPhase173ControlFlusher(s)
	if _, _, err := runPhase173BalancedOperations(s, cfg, cfg.WarmupAPIOps, phase173FixedWorkRegionBlocks, payloads, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.Stop(); err != nil {
		t.Fatal(err)
	}
	if s.dm.len() != 0 {
		t.Fatalf("warmup dirty entries=%d", s.dm.len())
	}

	if live {
		startPhase173ControlFlusher(s)
	}
	writeBefore := s.WriteInstrumentation()
	foreground, latencies, err := runPhase173BalancedOperations(s, cfg, cfg.APIOperations, 0, payloads, true)
	if err != nil {
		t.Fatal(err)
	}
	syncStart := time.Now()
	synced, err := s.Sync()
	finalSync := time.Since(syncStart)
	if err != nil {
		t.Fatal(err)
	}
	if !live {
		startPhase173ControlFlusher(s)
	}
	drainStart := time.Now()
	if err := s.flusher.Stop(); err != nil {
		t.Fatal(err)
	}
	finalDrain := time.Since(drainStart)
	assertPhase173ControlDrained(t, s, synced)
	correctness, err := verifyPhase173BalancedSamples(s, cfg, payloads)
	if err != nil {
		t.Fatal(err)
	}

	writeAfter := s.WriteInstrumentation()
	flush := s.FlusherInstrumentation()
	if flush.CycleDurationNanos == 0 {
		t.Fatalf("control=%s writers=%d recorded no flusher duration", control, writers)
	}
	logicalBlocks := cfg.APILogicalBlocks()
	logicalBytes := logicalBlocks * phase173FixedWorkBlockSize
	if got := writeAfter.WALEncodeOps - writeBefore.WALEncodeOps; got != logicalBlocks {
		t.Fatalf("control=%s writers=%d encoded=%d want %d", control, writers, got, logicalBlocks)
	}
	if flush.ValidatedRecords != logicalBlocks || flush.ExtentWriteOps != logicalBlocks ||
		flush.ValidationFailures != 0 || flush.WALRecordDecodeFailures != 0 {
		t.Fatalf("control=%s writers=%d flush=%+v", control, writers, flush)
	}
	_, _, p99 := phase173Percentiles(latencies)
	foregroundResult := phase173ArchitectureControlResult{
		Contract:               "phase173-architecture-controls-v1",
		Control:                control,
		Scope:                  map[bool]string{true: "shipped_durable_path", false: "non_product_deferred_writeback"}[live],
		Run:                    run,
		Writers:                writers,
		LogicalBlocks:          logicalBlocks,
		LogicalBytes:           logicalBytes,
		DurationNanos:          foreground.Nanoseconds(),
		MiBPerSecond:           phase173Rate(logicalBytes, foreground),
		P99Nanos:               p99,
		FinalSyncNanos:         finalSync.Nanoseconds(),
		FinalDrainNanos:        finalDrain.Nanoseconds(),
		CommitLockWaitOps:      writeAfter.WriteCommitLockWaitOps - writeBefore.WriteCommitLockWaitOps,
		CommitLockWaitNanos:    writeAfter.WriteCommitLockWaitNanos - writeBefore.WriteCommitLockWaitNanos,
		WALAppendLockWaitNanos: writeAfter.WALAppendLockWaitNanos - writeBefore.WALAppendLockWaitNanos,
		WALWriteAtCalls:        writeAfter.WALAppendWriteAtCalls - writeBefore.WALAppendWriteAtCalls,
		FlushCycles:            flush.CyclesStarted,
		FlushCycleNanos:        flush.CycleDurationNanos,
		FlushRecordReads:       flush.WALRecordReadOps,
		FlushDecodeOps:         flush.WALRecordDecodeOps,
		ExtentWriteOps:         flush.ExtentWriteOps,
		ExtentSyncOps:          flush.ExtentSyncOps,
		CheckpointSyncOps:      flush.CheckpointMetadataSyncOps,
		CorrectnessSamples:     correctness,
	}
	flusherResult := phase173ArchitectureControlResult{
		Contract:           "phase173-architecture-controls-v1",
		Control:            "prefilled_flusher",
		Scope:              "real_flusher_without_foreground_writers",
		Run:                run,
		Writers:            0,
		LogicalBlocks:      logicalBlocks,
		LogicalBytes:       logicalBytes,
		DurationNanos:      int64(flush.CycleDurationNanos),
		MiBPerSecond:       phase173Rate(logicalBytes, time.Duration(flush.CycleDurationNanos)),
		FlushCycles:        flush.CyclesStarted,
		FlushCycleNanos:    flush.CycleDurationNanos,
		FlushRecordReads:   flush.WALRecordReadOps,
		FlushDecodeOps:     flush.WALRecordDecodeOps,
		ExtentWriteOps:     flush.ExtentWriteOps,
		ExtentSyncOps:      flush.ExtentSyncOps,
		CheckpointSyncOps:  flush.CheckpointMetadataSyncOps,
		CorrectnessSamples: correctness,
	}
	return foregroundResult, flusherResult
}

func runPhase173BalancedOperations(
	s *WALStore,
	cfg phase173FixedWorkConfig,
	operations int,
	base uint32,
	payloads []map[int][][]byte,
	recordLatency bool,
) (time.Duration, []int64, error) {
	latencies := make([]int64, operations)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	for worker := 0; worker < cfg.Writers; worker++ {
		first, last := phase173BalancedOperationRange(worker, cfg.Writers, operations)
		wg.Add(1)
		ready.Add(1)
		go func(worker, first, last int) {
			defer wg.Done()
			ready.Done()
			<-start
			for index := first; index < last; index++ {
				lba, blocks := cfg.operation(index, base)
				opStart := time.Now()
				var err error
				if blocks == 1 {
					_, err = s.Write(lba, payloads[worker][1][0])
				} else {
					_, err = s.WriteBatch(lba, payloads[worker][blocks])
				}
				if recordLatency {
					latencies[index] = time.Since(opStart).Nanoseconds()
				}
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
			}
		}(worker, first, last)
	}
	ready.Wait()
	begin := time.Now()
	close(start)
	wg.Wait()
	return time.Since(begin), latencies, firstErr
}

func phase173BalancedOperationRange(worker, writers, operations int) (int, int) {
	return worker * operations / writers, (worker + 1) * operations / writers
}

func verifyPhase173BalancedSamples(
	s *WALStore,
	cfg phase173FixedWorkConfig,
	payloads []map[int][][]byte,
) (int, error) {
	indexes := map[int]struct{}{0: {}, cfg.APIOperations / 2: {}, cfg.APIOperations - 1: {}}
	for worker := 0; worker < cfg.Writers; worker++ {
		first, _ := phase173BalancedOperationRange(worker, cfg.Writers, cfg.APIOperations)
		indexes[first] = struct{}{}
	}
	ordered := make([]int, 0, len(indexes))
	for index := range indexes {
		ordered = append(ordered, index)
	}
	sort.Ints(ordered)
	samples := 0
	for _, index := range ordered {
		worker := index * cfg.Writers / cfg.APIOperations
		if worker >= cfg.Writers {
			worker = cfg.Writers - 1
		}
		lba, blocks := cfg.operation(index, 0)
		for _, block := range []int{0, blocks - 1} {
			data, err := s.Read(lba + uint32(block))
			if err != nil {
				return samples, err
			}
			want := payloads[worker][blocks][block]
			if len(data) != len(want) || data[0] != want[0] || data[1] != want[1] || data[2] != want[2] {
				return samples, fmt.Errorf("phase173 balanced data mismatch op=%d block=%d", index, block)
			}
			samples++
			if blocks == 1 {
				break
			}
		}
	}
	return samples, nil
}

func startPhase173ControlFlusher(s *WALStore) {
	s.flusher = newFlusher(s, flusherConfig{Interval: 100 * time.Millisecond})
	started := make(chan struct{})
	go s.flusher.runWithStartSignal(started)
	<-started
}

func assertPhase173ControlDrained(t *testing.T, s *WALStore, synced uint64) {
	t.Helper()
	_, _, head := s.Boundaries()
	if s.dm.len() != 0 || s.CheckpointLSN() != synced || head != synced {
		t.Fatalf("dirty=%d checkpoint=%d head=%d synced=%d", s.dm.len(), s.CheckpointLSN(), head, synced)
	}
}

func phase173Rate(logicalBytes uint64, duration time.Duration) float64 {
	return float64(logicalBytes) / (1024 * 1024) / duration.Seconds()
}

func emitPhase173ArchitectureControl(t *testing.T, result phase173ArchitectureControlResult) {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("phase173_architecture_control_result=%s\n", encoded)
}

type phase173FileLayoutScratch struct {
	shared      *os.File
	wal         *os.File
	extent      *os.File
	recordSize  int
	walBase     int64
	extentBase  int64
	logicalData []byte
	paths       []string
}

func preparePhase173FileLayoutScratch(t *testing.T, dir string) *phase173FileLayoutScratch {
	t.Helper()
	paths := []string{
		filepath.Join(dir, "phase173-control-shared.scratch"),
		filepath.Join(dir, "phase173-control-split-wal.scratch"),
		filepath.Join(dir, "phase173-control-split-extent.scratch"),
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	open := func(path string) *os.File {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		return file
	}
	scratch := &phase173FileLayoutScratch{
		shared:      open(paths[0]),
		wal:         open(paths[1]),
		extent:      open(paths[2]),
		walBase:     4096,
		logicalData: make([]byte, phase173FixedWorkBlockSize),
		paths:       paths,
	}
	for index := range scratch.logicalData {
		scratch.logicalData[index] = byte(index*17 + 3)
	}

	for index := 0; index < phase173ScratchBlocks; index++ {
		entry := walEntry{
			LSN:    uint64(index + 1),
			Type:   walEntryWrite,
			LBA:    uint64(index),
			Length: phase173FixedWorkBlockSize,
			Data:   scratch.logicalData,
		}
		encoded, err := entry.encodeWithInstrumentation(nil)
		if err != nil {
			t.Fatal(err)
		}
		if scratch.recordSize == 0 {
			scratch.recordSize = len(encoded)
			scratch.extentBase = scratch.walBase + int64(scratch.recordSize*phase173ScratchBlocks)
		}
		offset := scratch.walBase + int64(index*scratch.recordSize)
		for _, file := range []*os.File{scratch.shared, scratch.wal} {
			if _, err := file.WriteAt(encoded, offset); err != nil {
				t.Fatal(err)
			}
		}
	}
	zeroExtent := make([]byte, phase173ScratchBlocks*phase173FixedWorkBlockSize)
	if _, err := scratch.shared.WriteAt(zeroExtent, scratch.extentBase); err != nil {
		t.Fatal(err)
	}
	if _, err := scratch.extent.WriteAt(zeroExtent, 0); err != nil {
		t.Fatal(err)
	}
	for _, file := range []*os.File{scratch.shared, scratch.wal, scratch.extent} {
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	return scratch
}

func (s *phase173FileLayoutScratch) run(t *testing.T, split bool, run int) phase173ArchitectureControlResult {
	t.Helper()
	walFile := s.shared
	extentFile := s.shared
	extentBase := s.extentBase
	control := "shared_file_scratch"
	if split {
		walFile = s.wal
		extentFile = s.extent
		extentBase = 0
		control = "split_file_scratch"
	}
	start := time.Now()
	for pass := 0; pass < phase173ScratchPasses; pass++ {
		for index := 0; index < phase173ScratchBlocks; index++ {
			offset := s.walBase + int64(index*s.recordSize)
			header := make([]byte, walEntryHeaderSize)
			if _, err := walFile.ReadAt(header, offset); err != nil {
				t.Fatal(err)
			}
			full := make([]byte, s.recordSize)
			if _, err := walFile.ReadAt(full, offset); err != nil {
				t.Fatal(err)
			}
			entry, err := decodeWALEntry(full)
			if err != nil {
				t.Fatal(err)
			}
			if entry.LBA != uint64(index) || !bytes.Equal(entry.Data, s.logicalData) {
				t.Fatalf("scratch record %d mismatch", index)
			}
			if _, err := extentFile.WriteAt(entry.Data, extentBase+int64(index*phase173FixedWorkBlockSize)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := extentFile.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := walFile.WriteAt(make([]byte, 4096), 0); err != nil {
		t.Fatal(err)
	}
	if err := walFile.Sync(); err != nil {
		t.Fatal(err)
	}
	duration := time.Since(start)

	samples := 0
	for _, index := range []int{0, phase173ScratchBlocks / 2, phase173ScratchBlocks - 1} {
		got := make([]byte, phase173FixedWorkBlockSize)
		if _, err := extentFile.ReadAt(got, extentBase+int64(index*phase173FixedWorkBlockSize)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, s.logicalData) {
			t.Fatalf("scratch extent %d mismatch", index)
		}
		samples++
	}
	logicalBlocks := uint64(phase173ScratchBlocks * phase173ScratchPasses)
	logicalBytes := logicalBlocks * phase173FixedWorkBlockSize
	return phase173ArchitectureControlResult{
		Contract:           "phase173-architecture-controls-v1",
		Control:            control,
		Scope:              "same_device_scratch_no_recovery_contract",
		Run:                run,
		LogicalBlocks:      logicalBlocks,
		LogicalBytes:       logicalBytes,
		DurationNanos:      duration.Nanoseconds(),
		MiBPerSecond:       phase173Rate(logicalBytes, duration),
		ScratchPReadOps:    phase173ScratchBlocks * phase173ScratchPasses * 2,
		ScratchPWriteOps:   phase173ScratchBlocks*phase173ScratchPasses + 1,
		ScratchSyncOps:     2,
		CorrectnessSamples: samples,
	}
}

func (s *phase173FileLayoutScratch) close(t *testing.T) {
	t.Helper()
	var errs []error
	for _, file := range []*os.File{s.shared, s.wal, s.extent} {
		if err := file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, path := range s.paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
		t.Fatal(errs)
	}
}
