package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	phase173FixedWorkShapeEnv   = "SW_BLOCK_PHASE173_SHAPE"
	phase173FixedWorkWritersEnv = "SW_BLOCK_PHASE173_WRITERS"
	phase173FixedWorkRunIDEnv   = "SW_BLOCK_PHASE173_RUN_ID"
	phase173FixedWorkStoreEnv   = "SW_BLOCK_PHASE173_STORE_DIR"
	phase173FixedWorkStoreIDEnv = "SW_BLOCK_PHASE173_STORE_ID"
	phase173FixedWorkReuseEnv   = "SW_BLOCK_PHASE173_REUSE_STORE"

	phase173FixedWorkBlockSize    = 4096
	phase173FixedWorkNumBlocks    = 65536
	phase173FixedWorkRegionBlocks = phase173FixedWorkNumBlocks / 2
)

type phase173FixedWorkConfig struct {
	Shape         string
	Writers       int
	APIOperations int
	WarmupAPIOps  int
}

type phase173FixedWorkResult struct {
	Contract               string  `json:"contract"`
	RunID                  string  `json:"run_id"`
	StoreID                string  `json:"store_id"`
	StoreReused            bool    `json:"store_reused"`
	Shape                  string  `json:"shape"`
	Writers                int     `json:"writers"`
	APIOperations          int     `json:"api_operations"`
	LogicalBlocks          uint64  `json:"logical_blocks"`
	LogicalBytes           uint64  `json:"logical_bytes"`
	WarmupAPIOperations    int     `json:"warmup_api_operations"`
	WarmupLogicalBlocks    uint64  `json:"warmup_logical_blocks"`
	BlockSize              int     `json:"block_size"`
	NumBlocks              int     `json:"num_blocks"`
	FlusherIntervalMillis  int64   `json:"flusher_interval_ms"`
	ForegroundNanos        int64   `json:"foreground_ns"`
	ForegroundMiBPerSecond float64 `json:"foreground_mib_per_second"`
	P50Nanos               int64   `json:"p50_ns"`
	P95Nanos               int64   `json:"p95_ns"`
	P99Nanos               int64   `json:"p99_ns"`
	FinalSyncNanos         int64   `json:"final_sync_ns"`
	FinalDrainNanos        int64   `json:"final_drain_ns"`
	FinalSyncCalls         uint64  `json:"final_sync_calls"`
	SyncedLSN              uint64  `json:"synced_lsn"`
	CheckpointLSN          uint64  `json:"checkpoint_lsn"`
	HeadLSN                uint64  `json:"head_lsn"`
	DirtyEntries           int     `json:"dirty_entries"`
	WALCopyOps             uint64  `json:"wal_copy_ops"`
	WALEncodeOps           uint64  `json:"wal_encode_ops"`
	WALChecksumOps         uint64  `json:"wal_checksum_ops"`
	WALAppendOps           uint64  `json:"wal_append_ops"`
	WALWriteAtCalls        uint64  `json:"wal_writeat_calls"`
	WALWriteAtBytes        uint64  `json:"wal_writeat_bytes"`
	WALWraps               uint64  `json:"wal_wraps"`
	WALPaddingBytes        uint64  `json:"wal_padding_bytes"`
	WALAppendNanos         uint64  `json:"wal_append_ns"`
	WALAppendLockWaitNanos uint64  `json:"wal_append_lock_wait_ns"`
	CommitLockWaitOps      uint64  `json:"commit_lock_wait_ops"`
	CommitLockWaitNanos    uint64  `json:"commit_lock_wait_ns"`
	DirtyMapUpdateNanos    uint64  `json:"dirty_map_update_ns"`
	FlushCycles            uint64  `json:"flush_cycles"`
	FlushRecordReads       uint64  `json:"flush_record_reads"`
	FlushRecordReadBytes   uint64  `json:"flush_record_read_bytes"`
	ExtentWriteOps         uint64  `json:"extent_write_ops"`
	ExtentWriteBytes       uint64  `json:"extent_write_bytes"`
	ExtentSyncOps          uint64  `json:"extent_sync_ops"`
	CheckpointWriteOps     uint64  `json:"checkpoint_write_ops"`
	CheckpointSyncOps      uint64  `json:"checkpoint_sync_ops"`
	ValidationFailures     uint64  `json:"validation_failures"`
	CorrectnessSamples     int     `json:"correctness_samples"`
}

func TestPhase173FixedWorkContract(t *testing.T) {
	for _, writers := range []int{1, 2, 4, 8} {
		for _, shape := range []string{"sequential_4k", "scattered_4k", "batch_16", "mounted_mixed"} {
			cfg, err := newPhase173FixedWorkConfig(shape, writers)
			if err != nil {
				t.Fatalf("shape=%s writers=%d: %v", shape, writers, err)
			}
			if cfg.APILogicalBlocks() < 16000 || cfg.APILogicalBlocks() > 16384 {
				t.Fatalf("shape=%s logical blocks=%d want [16000,16384]", shape, cfg.APILogicalBlocks())
			}
			if cfg.WarmupLogicalBlocks() < 1000 || cfg.WarmupLogicalBlocks() > 1024 {
				t.Fatalf("shape=%s warmup logical blocks=%d want [1000,1024]", shape, cfg.WarmupLogicalBlocks())
			}
			for index := 0; index < cfg.APIOperations; index++ {
				start, blocks := cfg.operation(index, 0)
				if start+uint32(blocks) > phase173FixedWorkRegionBlocks {
					t.Fatalf("shape=%s op=%d range=[%d,%d) exceeds measured region", shape, index, start, start+uint32(blocks))
				}
			}
		}
	}

	if _, err := newPhase173FixedWorkConfig("unknown", 4); err == nil {
		t.Fatal("unknown shape accepted")
	}
	if _, err := newPhase173FixedWorkConfig("sequential_4k", 3); err == nil {
		t.Fatal("unsupported writer count accepted")
	}
}

func TestPhase173WALStoreFixedWork(t *testing.T) {
	shape := os.Getenv(phase173FixedWorkShapeEnv)
	if shape == "" {
		t.Skip("formal fixed-work run requires SW_BLOCK_PHASE173_SHAPE")
	}
	writers, err := strconv.Atoi(os.Getenv(phase173FixedWorkWritersEnv))
	if err != nil {
		t.Fatalf("parse %s: %v", phase173FixedWorkWritersEnv, err)
	}
	cfg, err := newPhase173FixedWorkConfig(shape, writers)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runPhase173FixedWork(
		t,
		cfg,
		os.Getenv(phase173FixedWorkRunIDEnv),
		os.Getenv(phase173FixedWorkStoreIDEnv),
		os.Getenv(phase173FixedWorkStoreEnv),
		os.Getenv(phase173FixedWorkReuseEnv) == "true",
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("phase173_fixed_work_result=%s\n", encoded)
}

func newPhase173FixedWorkConfig(shape string, writers int) (phase173FixedWorkConfig, error) {
	if writers != 1 && writers != 2 && writers != 4 && writers != 8 {
		return phase173FixedWorkConfig{}, fmt.Errorf("phase173: writers=%d want one of 1,2,4,8", writers)
	}
	cfg := phase173FixedWorkConfig{Shape: shape, Writers: writers}
	switch shape {
	case "sequential_4k", "scattered_4k":
		cfg.APIOperations = 16384
		cfg.WarmupAPIOps = 1024
	case "batch_16":
		cfg.APIOperations = 1024
		cfg.WarmupAPIOps = 64
	case "mounted_mixed":
		// 640 fixed cycles of 4K, 16K, 64K, and 16K requests.
		cfg.APIOperations = 2560
		cfg.WarmupAPIOps = 160
	default:
		return phase173FixedWorkConfig{}, fmt.Errorf("phase173: unsupported shape %q", shape)
	}
	return cfg, nil
}

func (c phase173FixedWorkConfig) APILogicalBlocks() uint64 {
	return phase173LogicalBlocks(c.Shape, c.APIOperations)
}

func (c phase173FixedWorkConfig) WarmupLogicalBlocks() uint64 {
	return phase173LogicalBlocks(c.Shape, c.WarmupAPIOps)
}

func phase173LogicalBlocks(shape string, operations int) uint64 {
	var total uint64
	for index := 0; index < operations; index++ {
		_, blocks := phase173Operation(shape, index, 0)
		total += uint64(blocks)
	}
	return total
}

func (c phase173FixedWorkConfig) operation(index int, base uint32) (uint32, int) {
	return phase173Operation(c.Shape, index, base)
}

func phase173Operation(shape string, index int, base uint32) (uint32, int) {
	switch shape {
	case "sequential_4k":
		return base + uint32(index), 1
	case "scattered_4k":
		return base + uint32((index*7919)%phase173FixedWorkRegionBlocks), 1
	case "batch_16":
		return base + uint32(index*16), 16
	case "mounted_mixed":
		pattern := [...]int{1, 4, 16, 4}
		cycle := index / len(pattern)
		position := index % len(pattern)
		start := cycle * 25
		for i := 0; i < position; i++ {
			start += pattern[i]
		}
		return base + uint32(start), pattern[position]
	default:
		panic("validated phase173 shape reached operation planner: " + shape)
	}
}

func runPhase173FixedWork(
	t *testing.T,
	cfg phase173FixedWorkConfig,
	runID string,
	storeID string,
	storeDir string,
	reuseStore bool,
) (phase173FixedWorkResult, error) {
	t.Helper()
	if storeDir == "" {
		storeDir = t.TempDir()
	}
	if storeID == "" {
		storeID = runID
	}
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return phase173FixedWorkResult{}, err
	}
	storePath := filepath.Join(storeDir, fmt.Sprintf("phase173-%s-%d-%s.store", cfg.Shape, cfg.Writers, storeID))
	var s *WALStore
	var err error
	storeReused := false
	if reuseStore {
		if _, statErr := os.Stat(storePath); statErr == nil {
			s, err = OpenWALStore(storePath)
			if err == nil {
				_, err = s.Recover()
			}
			storeReused = err == nil
		} else if !os.IsNotExist(statErr) {
			return phase173FixedWorkResult{}, statErr
		}
	}
	if s == nil && err == nil {
		if removeErr := os.Remove(storePath); removeErr != nil && !os.IsNotExist(removeErr) {
			return phase173FixedWorkResult{}, removeErr
		}
		s, err = CreateWALStore(storePath, phase173FixedWorkNumBlocks, phase173FixedWorkBlockSize)
	}
	if err != nil {
		if s != nil {
			_ = s.Close()
		}
		return phase173FixedWorkResult{}, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.Close()
		}
		if !reuseStore {
			_ = os.Remove(storePath)
		}
	}()
	if s.flusher == nil || s.flusher.interval != 100*time.Millisecond {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173: flusher interval=%v want 100ms", s.flusher.interval)
	}

	payloads := phase173Payloads(cfg.Writers)
	if _, _, err := runPhase173Operations(s, cfg, cfg.WarmupAPIOps, phase173FixedWorkRegionBlocks, payloads, false); err != nil {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 warmup: %w", err)
	}
	warmupLSN, err := s.Sync()
	if err != nil {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 warmup sync: %w", err)
	}
	s.flusher.Notify()
	if err := waitPhase173Drained(s, warmupLSN, 5*time.Second); err != nil {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 warmup drain: %w", err)
	}

	writeBefore := s.WriteInstrumentation()
	flushBefore := s.FlusherInstrumentation()
	syncBefore := s.syncs.Load()
	foreground, latencies, err := runPhase173Operations(s, cfg, cfg.APIOperations, 0, payloads, true)
	if err != nil {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 foreground: %w", err)
	}
	syncStart := time.Now()
	syncedLSN, err := s.Sync()
	finalSync := time.Since(syncStart)
	if err != nil {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 final sync: %w", err)
	}
	drainStart := time.Now()
	if err := s.flusher.Stop(); err != nil {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 final drain: %w", err)
	}
	finalDrain := time.Since(drainStart)

	writeAfter := s.WriteInstrumentation()
	flushAfter := s.FlusherInstrumentation()
	_, _, headLSN := s.Boundaries()
	checkpointLSN := s.CheckpointLSN()
	dirtyEntries := s.dm.len()
	if dirtyEntries != 0 || checkpointLSN != syncedLSN || headLSN != syncedLSN {
		return phase173FixedWorkResult{}, fmt.Errorf(
			"phase173 incomplete drain dirty=%d checkpoint=%d head=%d synced=%d",
			dirtyEntries, checkpointLSN, headLSN, syncedLSN,
		)
	}
	logicalBlocks := cfg.APILogicalBlocks()
	if got := writeAfter.WALEncodeOps - writeBefore.WALEncodeOps; got != logicalBlocks {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 WAL encode ops=%d want logical blocks=%d", got, logicalBlocks)
	}
	appendOps := writeAfter.WALAppendOps - writeBefore.WALAppendOps
	if appendOps < uint64(cfg.APIOperations) {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 WAL append ops=%d want at least API operations=%d", appendOps, cfg.APIOperations)
	}
	writeAtCalls := writeAfter.WALAppendWriteAtCalls - writeBefore.WALAppendWriteAtCalls
	wraps := writeAfter.WALAppendWrapCount - writeBefore.WALAppendWrapCount
	if writeAtCalls < appendOps || writeAtCalls-appendOps > wraps {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 WAL writeat calls=%d append ops=%d wraps=%d", writeAtCalls, appendOps, wraps)
	}
	if got := writeAfter.WriteCommitLockWaitOps - writeBefore.WriteCommitLockWaitOps; got != uint64(cfg.APIOperations) {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 commit lock ops=%d want API operations=%d", got, cfg.APIOperations)
	}
	if got := s.syncs.Load() - syncBefore; got != 1 {
		return phase173FixedWorkResult{}, fmt.Errorf("phase173 final sync calls=%d want 1", got)
	}
	correctnessSamples, err := verifyPhase173Samples(s, cfg, payloads)
	if err != nil {
		return phase173FixedWorkResult{}, err
	}
	if err := s.Close(); err != nil {
		return phase173FixedWorkResult{}, err
	}
	closed = true
	if !reuseStore {
		if err := os.Remove(storePath); err != nil && !os.IsNotExist(err) {
			return phase173FixedWorkResult{}, err
		}
	}

	p50, p95, p99 := phase173Percentiles(latencies)
	logicalBytes := logicalBlocks * phase173FixedWorkBlockSize
	result := phase173FixedWorkResult{
		Contract:               "phase173-fixed-work-v1",
		RunID:                  runID,
		StoreID:                storeID,
		StoreReused:            storeReused,
		Shape:                  cfg.Shape,
		Writers:                cfg.Writers,
		APIOperations:          cfg.APIOperations,
		LogicalBlocks:          logicalBlocks,
		LogicalBytes:           logicalBytes,
		WarmupAPIOperations:    cfg.WarmupAPIOps,
		WarmupLogicalBlocks:    cfg.WarmupLogicalBlocks(),
		BlockSize:              phase173FixedWorkBlockSize,
		NumBlocks:              phase173FixedWorkNumBlocks,
		FlusherIntervalMillis:  s.flusher.interval.Milliseconds(),
		ForegroundNanos:        foreground.Nanoseconds(),
		ForegroundMiBPerSecond: float64(logicalBytes) / (1024 * 1024) / foreground.Seconds(),
		P50Nanos:               p50,
		P95Nanos:               p95,
		P99Nanos:               p99,
		FinalSyncNanos:         finalSync.Nanoseconds(),
		FinalDrainNanos:        finalDrain.Nanoseconds(),
		FinalSyncCalls:         s.syncs.Load() - syncBefore,
		SyncedLSN:              syncedLSN,
		CheckpointLSN:          checkpointLSN,
		HeadLSN:                headLSN,
		DirtyEntries:           dirtyEntries,
		WALCopyOps:             writeAfter.WALCopyOps - writeBefore.WALCopyOps,
		WALEncodeOps:           writeAfter.WALEncodeOps - writeBefore.WALEncodeOps,
		WALChecksumOps:         writeAfter.WALChecksumOps - writeBefore.WALChecksumOps,
		WALAppendOps:           writeAfter.WALAppendOps - writeBefore.WALAppendOps,
		WALWriteAtCalls:        writeAfter.WALAppendWriteAtCalls - writeBefore.WALAppendWriteAtCalls,
		WALWriteAtBytes:        writeAfter.WALAppendWriteAtBytes - writeBefore.WALAppendWriteAtBytes,
		WALWraps:               wraps,
		WALPaddingBytes:        writeAfter.WALAppendPaddingBytes - writeBefore.WALAppendPaddingBytes,
		WALAppendNanos:         writeAfter.WALAppendDurationNanos - writeBefore.WALAppendDurationNanos,
		WALAppendLockWaitNanos: writeAfter.WALAppendLockWaitNanos - writeBefore.WALAppendLockWaitNanos,
		CommitLockWaitOps:      writeAfter.WriteCommitLockWaitOps - writeBefore.WriteCommitLockWaitOps,
		CommitLockWaitNanos:    writeAfter.WriteCommitLockWaitNanos - writeBefore.WriteCommitLockWaitNanos,
		DirtyMapUpdateNanos:    writeAfter.DirtyMapUpdateDurationNanos - writeBefore.DirtyMapUpdateDurationNanos,
		FlushCycles:            flushAfter.CyclesStarted - flushBefore.CyclesStarted,
		FlushRecordReads:       flushAfter.WALRecordReadOps - flushBefore.WALRecordReadOps,
		FlushRecordReadBytes:   flushAfter.WALRecordReadBytes - flushBefore.WALRecordReadBytes,
		ExtentWriteOps:         flushAfter.ExtentWriteOps - flushBefore.ExtentWriteOps,
		ExtentWriteBytes:       flushAfter.ExtentWriteBytes - flushBefore.ExtentWriteBytes,
		ExtentSyncOps:          flushAfter.ExtentSyncOps - flushBefore.ExtentSyncOps,
		CheckpointWriteOps:     flushAfter.CheckpointMetadataWriteOps - flushBefore.CheckpointMetadataWriteOps,
		CheckpointSyncOps:      flushAfter.CheckpointMetadataSyncOps - flushBefore.CheckpointMetadataSyncOps,
		ValidationFailures:     flushAfter.ValidationFailures - flushBefore.ValidationFailures,
		CorrectnessSamples:     correctnessSamples,
	}
	return result, nil
}

func phase173Payloads(writers int) []map[int][][]byte {
	payloads := make([]map[int][][]byte, writers)
	for worker := 0; worker < writers; worker++ {
		payloads[worker] = make(map[int][][]byte)
		for _, count := range []int{1, 4, 16} {
			blocks := make([][]byte, count)
			for block := range blocks {
				blocks[block] = make([]byte, phase173FixedWorkBlockSize)
				blocks[block][0] = byte(worker + 1)
				blocks[block][1] = byte(count)
				blocks[block][2] = byte(block)
			}
			payloads[worker][count] = blocks
		}
	}
	return payloads
}

func runPhase173Operations(
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
		wg.Add(1)
		ready.Add(1)
		go func(worker int) {
			defer wg.Done()
			ready.Done()
			<-start
			for index := worker; index < operations; index += cfg.Writers {
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
		}(worker)
	}
	ready.Wait()
	begin := time.Now()
	close(start)
	wg.Wait()
	return time.Since(begin), latencies, firstErr
}

func waitPhase173Drained(s *WALStore, syncedLSN uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, headLSN := s.Boundaries()
		if s.dm.len() == 0 && s.CheckpointLSN() == syncedLSN && headLSN == syncedLSN {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _, headLSN := s.Boundaries()
	return fmt.Errorf("timeout dirty=%d checkpoint=%d head=%d synced=%d", s.dm.len(), s.CheckpointLSN(), headLSN, syncedLSN)
}

func verifyPhase173Samples(s *WALStore, cfg phase173FixedWorkConfig, payloads []map[int][][]byte) (int, error) {
	indexes := map[int]struct{}{0: {}, cfg.APIOperations / 2: {}, cfg.APIOperations - 1: {}}
	for worker := 0; worker < cfg.Writers; worker++ {
		indexes[worker] = struct{}{}
	}
	ordered := make([]int, 0, len(indexes))
	for index := range indexes {
		ordered = append(ordered, index)
	}
	sort.Ints(ordered)
	samples := 0
	for _, index := range ordered {
		if index < 0 || index >= cfg.APIOperations {
			continue
		}
		worker := index % cfg.Writers
		lba, blocks := cfg.operation(index, 0)
		for _, block := range []int{0, blocks - 1} {
			data, err := s.Read(lba + uint32(block))
			if err != nil {
				return samples, err
			}
			want := payloads[worker][blocks][block]
			if len(data) != len(want) || data[0] != want[0] || data[1] != want[1] || data[2] != want[2] {
				return samples, fmt.Errorf("phase173 data mismatch shape=%s op=%d block=%d", cfg.Shape, index, block)
			}
			samples++
			if blocks == 1 {
				break
			}
		}
	}
	return samples, nil
}

func phase173Percentiles(samples []int64) (int64, int64, int64) {
	ordered := append([]int64(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	at := func(percent int) int64 {
		index := (len(ordered)*percent + 99) / 100
		if index > 0 {
			index--
		}
		return ordered[index]
	}
	return at(50), at(95), at(99)
}
