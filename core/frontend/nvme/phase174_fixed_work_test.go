//go:build swblock_testtools

package nvme_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	phase174NVMeStoreDirEnv = "SW_BLOCK_PHASE174_NVME_STORE_DIR"
	phase174NVMeWritersEnv  = "SW_BLOCK_PHASE174_NVME_WRITERS"
	phase174NVMeRunEnv      = "SW_BLOCK_PHASE174_NVME_RUN"
	phase174NVMeSetEnv      = "SW_BLOCK_PHASE174_NVME_SET"

	phase174NVMeBlockSize               = 4096
	phase174NVMENumBlocks               = 65536
	phase174NVMeRegionBlocks            = phase174NVMENumBlocks / 2
	phase174NVMeAPIOperations           = 16384
	phase174NVMeWarmupOperations        = 1024
	phase174NVMERuns                    = 5
	phase174NVMESectorsPerBlock  uint16 = uint16(phase174NVMeBlockSize / int(nvme.DefaultBlockSize))
)

type phase174NVMeView struct {
	projection frontend.Projection
}

func (v *phase174NVMeView) Projection() frontend.Projection {
	return v.projection
}

type phase174NVMeFixedWorkResult struct {
	Contract                      string  `json:"contract"`
	Layer                         string  `json:"layer"`
	Scope                         string  `json:"scope"`
	AckProfile                    string  `json:"ack_profile"`
	Set                           int     `json:"set"`
	Run                           int     `json:"run"`
	Writers                       int     `json:"writers"`
	APIOperations                 int     `json:"api_operations"`
	LogicalBytes                  uint64  `json:"logical_bytes"`
	ForegroundNanos               int64   `json:"foreground_ns"`
	MiBPerSecond                  float64 `json:"mib_per_second"`
	P50Nanos                      int64   `json:"p50_ns"`
	P95Nanos                      int64   `json:"p95_ns"`
	P99Nanos                      int64   `json:"p99_ns"`
	ClientWriteLatencyNanos       uint64  `json:"client_write_latency_ns"`
	NVMeRoundTripNonBackendNanos  uint64  `json:"nvme_round_trip_nonbackend_ns"`
	FinalFlushNanos               int64   `json:"final_flush_ns"`
	NVMeWriteCommands             uint64  `json:"nvme_write_commands"`
	NVMeR2TWriteCommands          uint64  `json:"nvme_r2t_write_commands"`
	NVMeR2TWriteBytes             uint64  `json:"nvme_r2t_write_bytes"`
	NVMeH2CDataPDUs               uint64  `json:"nvme_h2c_data_pdus"`
	NVMeH2CDataBytes              uint64  `json:"nvme_h2c_data_bytes"`
	TargetWriteOps                uint64  `json:"target_write_ops"`
	TargetWriteBytes              uint64  `json:"target_write_bytes"`
	TargetWriteNanos              uint64  `json:"target_write_ns"`
	AdapterRequestOps             uint64  `json:"adapter_request_ops"`
	AdapterRequestBytes           uint64  `json:"adapter_request_bytes"`
	AdapterWriteOps               uint64  `json:"adapter_write_ops"`
	AdapterWriteBytes             uint64  `json:"adapter_write_bytes"`
	AdapterWriteNanos             uint64  `json:"adapter_write_ns"`
	AdapterStorageCalls           uint64  `json:"adapter_storage_write_calls"`
	AdapterStorageBlocks          uint64  `json:"adapter_storage_write_blocks"`
	PrimaryWALWrites              uint64  `json:"primary_wal_write_ops"`
	PrimaryWALCopyNanos           uint64  `json:"primary_wal_copy_ns"`
	PrimaryWALEncodeNanos         uint64  `json:"primary_wal_encode_ns"`
	PrimaryWALChecksumNanos       uint64  `json:"primary_wal_checksum_ns"`
	PrimaryWALAppendNanos         uint64  `json:"primary_wal_append_ns"`
	PrimaryWALAppendLockWaitNanos uint64  `json:"primary_wal_append_lock_wait_ns"`
	PrimaryWriteCommitWaitNanos   uint64  `json:"primary_write_commit_lock_wait_ns"`
	PrimaryDirtyMapNanos          uint64  `json:"primary_dirty_map_ns"`
	PrimaryStableLSN              uint64  `json:"primary_stable_lsn"`
	PrimaryHeadLSN                uint64  `json:"primary_head_lsn"`
	FlusherPhaseReset             bool    `json:"flusher_phase_reset"`
	CloseRecoverComplete          bool    `json:"close_recover_complete"`
	CorrectnessSamples            int     `json:"correctness_samples"`
	MountedShapeComparable        bool    `json:"mounted_shape_comparable"`
	MountedThroughputRatioAllowed bool    `json:"mounted_throughput_ratio_allowed"`
}

func TestPhase174NVMeFixedWorkContract(t *testing.T) {
	for _, writers := range []int{1, 4, 8} {
		if err := phase174NVMeValidateWriters(writers); err != nil {
			t.Fatal(err)
		}
		seen := make(map[uint32]struct{}, phase174NVMeAPIOperations)
		for operation := 0; operation < phase174NVMeAPIOperations; operation++ {
			lba := phase174NVMeOperationLBA(operation, 0)
			if _, exists := seen[lba]; exists {
				t.Fatalf("writers=%d duplicate measured LBA %d", writers, lba)
			}
			seen[lba] = struct{}{}
		}
	}
	if err := phase174NVMeValidateWriters(3); err == nil {
		t.Fatal("writers=3 accepted")
	}
	if phase174NVMESectorsPerBlock != 8 {
		t.Fatalf("NVMe sectors per logical block=%d want 8", phase174NVMESectorsPerBlock)
	}
}

func TestPhase174NVMeFixedWorkPipeline(t *testing.T) {
	storeDir := os.Getenv(phase174NVMeStoreDirEnv)
	if storeDir == "" {
		t.Skip("formal NVMe fixed-work run requires " + phase174NVMeStoreDirEnv)
	}
	writers := phase174NVMeEnvInt(t, phase174NVMeWritersEnv, 1, 8)
	if err := phase174NVMeValidateWriters(writers); err != nil {
		t.Fatal(err)
	}
	run := phase174NVMeEnvInt(t, phase174NVMeRunEnv, 0, phase174NVMERuns)
	set := phase174NVMeEnvInt(t, phase174NVMeSetEnv, 1, 2)
	oldLogWriter := log.Writer()
	if os.Getenv("SW_BLOCK_BENCH_VERBOSE") == "" {
		log.SetOutput(io.Discard)
		defer log.SetOutput(oldLogWriter)
	}

	groupDir := filepath.Join(storeDir, fmt.Sprintf("set%d-nvme-tcp-rf1-writers%d", set, writers))
	if err := os.MkdirAll(groupDir, 0o755); err != nil {
		t.Fatal(err)
	}
	storePath := filepath.Join(groupDir, "primary.store")
	if run == 0 {
		if err := os.Remove(storePath); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	result := phase174RunNVMeFixedWork(t, storePath, run == 0, set, run, writers)
	if run == 0 {
		return
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("phase174_nvme_fixed_work_result=%s\n", encoded)
}

func phase174RunNVMeFixedWork(
	t *testing.T,
	storePath string,
	create bool,
	set, run, writers int,
) phase174NVMeFixedWorkResult {
	t.Helper()
	store := phase174NVMeOpenStore(t, storePath, create)
	id := frontend.Identity{VolumeID: "phase174", ReplicaID: "primary", Epoch: 1, EndpointVersion: 1}
	view := &phase174NVMeView{projection: frontend.Projection{
		VolumeID: id.VolumeID, ReplicaID: id.ReplicaID, Epoch: id.Epoch,
		EndpointVersion: id.EndpointVersion, Healthy: true,
	}}
	backend := durable.NewStorageBackend(store, view, id)
	backend.SetOperational(true, "phase174 NVMe fixed-work")
	target := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.io.seaweedfs:phase174",
		VolumeID:  id.VolumeID,
		Provider:  testback.NewStaticProvider(backend),
		Handler: nvme.HandlerConfig{
			BlockSize:  nvme.DefaultBlockSize,
			VolumeSize: uint64(phase174NVMENumBlocks * phase174NVMeBlockSize),
		},
	})
	addr, err := target.Start()
	if err != nil {
		t.Fatal(err)
	}
	client := newMultiQueueClient(t, addr, "nqn.2026-04.io.seaweedfs:phase174",
		"nqn.2026-04.io.seaweedfs:phase174-host", writers)

	warmup := phase174NVMePayloads(phase174NVMeWarmupOperations, set, run, 0x1740)
	if _, _, err := phase174NVMeWrites(t, client, writers, phase174NVMeRegionBlocks, warmup, false); err != nil {
		t.Fatalf("warmup writes: %v", err)
	}
	if err := phase174NVMeFlush(t, client); err != nil {
		t.Fatalf("warmup flush: %v", err)
	}
	if err := store.ResetFlusherForMeasurement(); err != nil {
		t.Fatalf("reset flusher: %v", err)
	}

	targetBefore := target.Stats()
	adapterBefore := backend.WriteProfile()
	storageBefore := store.WriteInstrumentation()
	payloads := phase174NVMePayloads(phase174NVMeAPIOperations, set, run, 0x1741)
	foreground, latencies, err := phase174NVMeWrites(t, client, writers, 0, payloads, true)
	if err != nil {
		t.Fatalf("measured writes: %v", err)
	}
	targetAfter := target.Stats()
	adapterAfter := backend.WriteProfile()
	storageAfter := store.WriteInstrumentation()
	flushStart := time.Now()
	if err := phase174NVMeFlush(t, client); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	finalFlush := time.Since(flushStart)

	phase174CloseNVMeClient(client)
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	stable, head, samples := phase174NVMeRecoverAndVerify(t, storePath, payloads)

	logicalBytes := uint64(phase174NVMeAPIOperations * phase174NVMeBlockSize)
	clientLatencyNanos := phase174NVMeSumLatencies(latencies)
	targetWriteNanos := adapterAfter.TargetWriteDurationNanos - adapterBefore.TargetWriteDurationNanos
	nonBackendNanos := uint64(0)
	if clientLatencyNanos > targetWriteNanos {
		nonBackendNanos = clientLatencyNanos - targetWriteNanos
	}
	result := phase174NVMeFixedWorkResult{
		Contract: "phase174-fixed-work-v1", Layer: "nvme_tcp_rf1",
		Scope: "nvme_tcp_target_durable_adapter", AckProfile: "local_durable",
		Set: set, Run: run, Writers: writers, APIOperations: phase174NVMeAPIOperations,
		LogicalBytes: logicalBytes, ForegroundNanos: foreground.Nanoseconds(),
		MiBPerSecond:                  (float64(logicalBytes) / (1024 * 1024)) / foreground.Seconds(),
		P50Nanos:                      phase174NVMePercentile(latencies, 50),
		P95Nanos:                      phase174NVMePercentile(latencies, 95),
		P99Nanos:                      phase174NVMePercentile(latencies, 99),
		ClientWriteLatencyNanos:       clientLatencyNanos,
		NVMeRoundTripNonBackendNanos:  nonBackendNanos,
		FinalFlushNanos:               finalFlush.Nanoseconds(),
		NVMeWriteCommands:             targetAfter.WriteCommands - targetBefore.WriteCommands,
		NVMeR2TWriteCommands:          targetAfter.R2TWriteCommands - targetBefore.R2TWriteCommands,
		NVMeR2TWriteBytes:             targetAfter.R2TWriteBytes - targetBefore.R2TWriteBytes,
		NVMeH2CDataPDUs:               targetAfter.H2CDataPDUs - targetBefore.H2CDataPDUs,
		NVMeH2CDataBytes:              targetAfter.H2CDataBytes - targetBefore.H2CDataBytes,
		TargetWriteOps:                adapterAfter.TargetWriteOps - adapterBefore.TargetWriteOps,
		TargetWriteBytes:              adapterAfter.TargetWriteBytes - adapterBefore.TargetWriteBytes,
		TargetWriteNanos:              targetWriteNanos,
		AdapterRequestOps:             adapterAfter.BackendWriteRequestOps - adapterBefore.BackendWriteRequestOps,
		AdapterRequestBytes:           adapterAfter.BackendWriteRequestBytes - adapterBefore.BackendWriteRequestBytes,
		AdapterWriteOps:               adapterAfter.BackendWriteOps - adapterBefore.BackendWriteOps,
		AdapterWriteBytes:             adapterAfter.BackendWriteBytes - adapterBefore.BackendWriteBytes,
		AdapterWriteNanos:             adapterAfter.BackendWriteDurationNanos - adapterBefore.BackendWriteDurationNanos,
		AdapterStorageCalls:           adapterAfter.BackendStorageWriteCalls - adapterBefore.BackendStorageWriteCalls,
		AdapterStorageBlocks:          adapterAfter.BackendStorageWriteBlocks - adapterBefore.BackendStorageWriteBlocks,
		PrimaryWALWrites:              storageAfter.WALEncodeOps - storageBefore.WALEncodeOps,
		PrimaryWALCopyNanos:           storageAfter.WALCopyDurationNanos - storageBefore.WALCopyDurationNanos,
		PrimaryWALEncodeNanos:         storageAfter.WALEncodeDurationNanos - storageBefore.WALEncodeDurationNanos,
		PrimaryWALChecksumNanos:       storageAfter.WALChecksumDurationNanos - storageBefore.WALChecksumDurationNanos,
		PrimaryWALAppendNanos:         storageAfter.WALAppendDurationNanos - storageBefore.WALAppendDurationNanos,
		PrimaryWALAppendLockWaitNanos: storageAfter.WALAppendLockWaitNanos - storageBefore.WALAppendLockWaitNanos,
		PrimaryWriteCommitWaitNanos:   storageAfter.WriteCommitLockWaitNanos - storageBefore.WriteCommitLockWaitNanos,
		PrimaryDirtyMapNanos:          storageAfter.DirtyMapUpdateDurationNanos - storageBefore.DirtyMapUpdateDurationNanos,
		PrimaryStableLSN:              stable, PrimaryHeadLSN: head, FlusherPhaseReset: true,
		CloseRecoverComplete: true, CorrectnessSamples: samples,
		MountedShapeComparable: false, MountedThroughputRatioAllowed: false,
	}
	phase174NVMeValidateResult(t, result)
	return result
}

func phase174NVMeWrites(
	t *testing.T,
	client *multiQueueClient,
	writers int,
	base uint32,
	payloads [][]byte,
	measureLatency bool,
) (time.Duration, []int64, error) {
	t.Helper()
	latencies := make([]int64, len(payloads))
	start := make(chan struct{})
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	wallStart := time.Now()
	for worker := 0; worker < writers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			queue := client.queues[worker]
			for operation := worker; operation < len(payloads); operation += writers {
				opStart := time.Now()
				storageLBA := phase174NVMeOperationLBA(operation, base)
				status, err := client.chunkedWriteOnQueue(t, queue,
					uint64(storageLBA)*uint64(phase174NVMESectorsPerBlock), phase174NVMESectorsPerBlock,
					payloads[operation], 1)
				if measureLatency {
					latencies[operation] = time.Since(opStart).Nanoseconds()
				}
				if err == nil && status != 0 {
					err = fmt.Errorf("operation %d status=0x%04x", operation, status)
				}
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	return time.Since(wallStart), latencies, firstErr
}

func phase174NVMeFlush(t *testing.T, client *multiQueueClient) error {
	t.Helper()
	queue := client.queues[0]
	cid := uint16(client.cidSeq.Add(1))
	cmd := nvme.CapsuleCommand{OpCode: 0x00, CID: cid, NSID: 1}
	if err := queue.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		return err
	}
	resp := recvCapsuleResp(t, queue.r)
	if resp.CID != cid {
		return fmt.Errorf("flush response CID=%d want %d", resp.CID, cid)
	}
	if resp.Status != 0 {
		return fmt.Errorf("flush status=0x%04x", resp.Status)
	}
	return nil
}

func phase174CloseNVMeClient(client *multiQueueClient) {
	for _, queue := range client.queues {
		_ = queue.conn.Close()
	}
	_ = client.admin.Close()
}

func phase174NVMeOpenStore(t *testing.T, path string, create bool) *storage.WALStore {
	t.Helper()
	var (
		store *storage.WALStore
		err   error
	)
	if create {
		store, err = storage.CreateWALStore(path, phase174NVMENumBlocks, phase174NVMeBlockSize)
	} else {
		store, err = storage.OpenWALStore(path)
		if err == nil {
			_, err = store.Recover()
		}
	}
	if err != nil {
		if store != nil {
			_ = store.Close()
		}
		t.Fatal(err)
	}
	return store
}

func phase174NVMeRecoverAndVerify(t *testing.T, path string, payloads [][]byte) (uint64, uint64, int) {
	t.Helper()
	store, err := storage.OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if _, err := store.Recover(); err != nil {
		t.Fatal(err)
	}
	stable, _, head := store.Boundaries()
	if stable != head {
		t.Fatalf("recovered stable=%d head=%d", stable, head)
	}
	samples := 0
	for _, operation := range phase174NVMESampleOperations(len(payloads)) {
		got, err := store.Read(phase174NVMeOperationLBA(operation, 0))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payloads[operation]) {
			t.Fatalf("operation=%d recovered data mismatch", operation)
		}
		samples++
	}
	return stable, head, samples
}

func phase174NVMeValidateResult(t *testing.T, result phase174NVMeFixedWorkResult) {
	t.Helper()
	wantOps := uint64(phase174NVMeAPIOperations)
	wantBytes := uint64(phase174NVMeAPIOperations * phase174NVMeBlockSize)
	for name, got := range map[string]uint64{
		"nvme_write_commands":          result.NVMeWriteCommands,
		"nvme_r2t_write_commands":      result.NVMeR2TWriteCommands,
		"nvme_h2c_data_pdus":           result.NVMeH2CDataPDUs,
		"target_write_ops":             result.TargetWriteOps,
		"adapter_request_ops":          result.AdapterRequestOps,
		"adapter_write_ops":            result.AdapterWriteOps,
		"adapter_storage_write_calls":  result.AdapterStorageCalls,
		"adapter_storage_write_blocks": result.AdapterStorageBlocks,
		"primary_wal_write_ops":        result.PrimaryWALWrites,
	} {
		if got != wantOps {
			t.Fatalf("%s=%d want %d", name, got, wantOps)
		}
	}
	for name, got := range map[string]uint64{
		"nvme_r2t_write_bytes":  result.NVMeR2TWriteBytes,
		"nvme_h2c_data_bytes":   result.NVMeH2CDataBytes,
		"target_write_bytes":    result.TargetWriteBytes,
		"adapter_request_bytes": result.AdapterRequestBytes,
		"adapter_write_bytes":   result.AdapterWriteBytes,
	} {
		if got != wantBytes {
			t.Fatalf("%s=%d want %d", name, got, wantBytes)
		}
	}
	if result.PrimaryStableLSN != result.PrimaryHeadLSN || result.CorrectnessSamples < 5 {
		t.Fatalf("recovery stable=%d head=%d samples=%d",
			result.PrimaryStableLSN, result.PrimaryHeadLSN, result.CorrectnessSamples)
	}
}

func phase174NVMeValidateWriters(writers int) error {
	switch writers {
	case 1, 4, 8:
		return nil
	default:
		return fmt.Errorf("phase174 NVMe: writers=%d want one of 1,4,8", writers)
	}
}

func phase174NVMeEnvInt(t *testing.T, name string, min, max int) int {
	t.Helper()
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value < min || value > max {
		t.Fatalf("%s must be in [%d,%d]", name, min, max)
	}
	return value
}

func phase174NVMeOperationLBA(operation int, base uint32) uint32 {
	return base + uint32((operation*7919)%phase174NVMeRegionBlocks)
}

func phase174NVMePayloads(operations, set, run int, marker uint64) [][]byte {
	payloads := make([][]byte, operations)
	for operation := range payloads {
		payload := make([]byte, phase174NVMeBlockSize)
		for offset := 0; offset < len(payload); offset += 8 {
			value := marker<<48 | uint64(set)<<40 | uint64(run)<<32 |
				uint64(operation+1)<<12 | uint64(offset/8)
			binary.LittleEndian.PutUint64(payload[offset:offset+8], value)
		}
		payloads[operation] = payload
	}
	return payloads
}

func phase174NVMESampleOperations(operations int) []int {
	samples := []int{0, operations / 7, operations / 3, operations / 2, operations - 1}
	sort.Ints(samples)
	return samples
}

func phase174NVMePercentile(samples []int64, percent int) int64 {
	ordered := append([]int64(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := (len(ordered)*percent + 99) / 100
	if index > 0 {
		index--
	}
	return ordered[index]
}

func phase174NVMeSumLatencies(samples []int64) uint64 {
	var total uint64
	for _, sample := range samples {
		if sample > 0 {
			total += uint64(sample)
		}
	}
	return total
}
