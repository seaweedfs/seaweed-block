package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

const (
	phase174LayerEnv    = "SW_BLOCK_PHASE174_LAYER"
	phase174WritersEnv  = "SW_BLOCK_PHASE174_WRITERS"
	phase174RunEnv      = "SW_BLOCK_PHASE174_RUN"
	phase174SetEnv      = "SW_BLOCK_PHASE174_SET"
	phase174StoreDirEnv = "SW_BLOCK_PHASE174_STORE_DIR"

	phase174BlockSize        = 4096
	phase174NumBlocks        = 65536
	phase174RegionBlocks     = phase174NumBlocks / 2
	phase174APIOperations    = 16384
	phase174WarmupOperations = 1024
	phase174Runs             = 5
)

type phase174Layer string

const (
	phase174DirectWALStore phase174Layer = "direct_walstore"
	phase174AdapterRF1     phase174Layer = "adapter_rf1"
	phase174RF3TCP         phase174Layer = "rf3_tcp"
)

type phase174FixedWorkResult struct {
	Contract                string  `json:"contract"`
	Layer                   string  `json:"layer"`
	Scope                   string  `json:"scope"`
	AckProfile              string  `json:"ack_profile"`
	Set                     int     `json:"set"`
	Run                     int     `json:"run"`
	Writers                 int     `json:"writers"`
	APIOperations           int     `json:"api_operations"`
	LogicalBytes            uint64  `json:"logical_bytes"`
	ForegroundNanos         int64   `json:"foreground_ns"`
	MiBPerSecond            float64 `json:"mib_per_second"`
	P50Nanos                int64   `json:"p50_ns"`
	P95Nanos                int64   `json:"p95_ns"`
	P99Nanos                int64   `json:"p99_ns"`
	FinalSyncNanos          int64   `json:"final_sync_ns"`
	CloseRecoverNanos       int64   `json:"close_recover_ns"`
	PrimaryWALWrites        uint64  `json:"primary_wal_write_ops"`
	PrimaryStableLSN        uint64  `json:"primary_stable_lsn"`
	PrimaryHeadLSN          uint64  `json:"primary_head_lsn"`
	ReplicationWrites       uint64  `json:"replication_write_ops,omitempty"`
	ReplicationFanoutNanos  uint64  `json:"replication_fanout_ns,omitempty"`
	ReplicationAckWaitNanos uint64  `json:"replication_ack_wait_ns,omitempty"`
	PeerQueueMaxDepth       uint64  `json:"peer_queue_max_depth,omitempty"`
	PeerQueueSaturated      uint64  `json:"peer_queue_saturated,omitempty"`
	ReplicaCount            int     `json:"replica_count"`
	ReplicaDurableCount     int     `json:"replica_durable_count"`
	ReplicaFrontiersEqual   bool    `json:"replica_frontiers_equal"`
	CloseRecoverComplete    bool    `json:"close_recover_complete"`
	CorrectnessSamples      int     `json:"correctness_samples"`
}

type phase174HealthyView struct {
	projection frontend.Projection
}

func (v *phase174HealthyView) Projection() frontend.Projection {
	return v.projection
}

type phase174Pipeline struct {
	layer        phase174Layer
	primaryPath  string
	replicaPaths []string
	primary      *storage.WALStore
	replicas     []*storage.WALStore
	backend      *durable.StorageBackend
	replication  *ReplicationVolume
	listeners    []*transport.ReplicaListener
	closed       bool
}

func TestPhase174FixedWorkContract(t *testing.T) {
	for _, layer := range []phase174Layer{phase174DirectWALStore, phase174AdapterRF1, phase174RF3TCP} {
		if _, _, err := phase174LayerContract(layer); err != nil {
			t.Fatalf("layer %q: %v", layer, err)
		}
	}
	for _, writers := range []int{1, 4, 8} {
		if err := phase174ValidateWriters(writers); err != nil {
			t.Fatal(err)
		}
		seen := make(map[uint32]struct{}, phase174APIOperations)
		for operation := 0; operation < phase174APIOperations; operation++ {
			lba := phase174OperationLBA(operation, 0)
			if _, exists := seen[lba]; exists {
				t.Fatalf("writers=%d duplicate measured LBA %d", writers, lba)
			}
			seen[lba] = struct{}{}
		}
	}
	if _, _, err := phase174LayerContract("unknown"); err == nil {
		t.Fatal("unknown layer accepted")
	}
	if err := phase174ValidateWriters(3); err == nil {
		t.Fatal("writers=3 accepted")
	}
}

func TestPhase174FixedWorkPipeline(t *testing.T) {
	storeRoot := os.Getenv(phase174StoreDirEnv)
	if storeRoot == "" {
		t.Skip("formal fixed-work run requires " + phase174StoreDirEnv)
	}
	layer := phase174Layer(os.Getenv(phase174LayerEnv))
	if _, _, err := phase174LayerContract(layer); err != nil {
		t.Fatal(err)
	}
	writers, err := strconv.Atoi(os.Getenv(phase174WritersEnv))
	if err != nil {
		t.Fatalf("parse %s: %v", phase174WritersEnv, err)
	}
	if err := phase174ValidateWriters(writers); err != nil {
		t.Fatal(err)
	}
	run, err := strconv.Atoi(os.Getenv(phase174RunEnv))
	if err != nil || run < 0 || run > phase174Runs {
		t.Fatalf("%s must be in [0,%d]", phase174RunEnv, phase174Runs)
	}
	set, err := strconv.Atoi(os.Getenv(phase174SetEnv))
	if err != nil || set < 1 || set > 2 {
		t.Fatalf("%s must be 1 or 2", phase174SetEnv)
	}

	oldLogWriter := log.Writer()
	if os.Getenv("SW_BLOCK_BENCH_VERBOSE") == "" {
		log.SetOutput(io.Discard)
		defer log.SetOutput(oldLogWriter)
	}

	groupDir := filepath.Join(storeRoot, fmt.Sprintf("set%d-%s-writers%d", set, layer, writers))
	create := run == 0
	if layer == phase174RF3TCP {
		// Same-host durable RF3 is diagnostic: sync-quorum may let one peer
		// lag, so every sample needs independent stores rather than inheriting
		// an intentionally incomplete replica from the previous sample.
		groupDir = filepath.Join(groupDir, fmt.Sprintf("run%d", run))
		create = true
	}
	if create {
		for _, name := range []string{"primary.store", "replica-1.store", "replica-2.store"} {
			if err := os.Remove(filepath.Join(groupDir, name)); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
		}
	}
	if err := os.MkdirAll(groupDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pipeline := newPhase174Pipeline(t, layer, groupDir, create)
	result := runPhase174FixedWork(t, pipeline, set, run, writers)
	if run == 0 {
		return
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("phase174_fixed_work_result=%s\n", encoded)
}

func phase174ValidateWriters(writers int) error {
	switch writers {
	case 1, 4, 8:
		return nil
	default:
		return fmt.Errorf("phase174: writers=%d want one of 1,4,8", writers)
	}
}

func phase174LayerContract(layer phase174Layer) (scope, ackProfile string, err error) {
	switch layer {
	case phase174DirectWALStore:
		return "walstore_engine", "local_durable", nil
	case phase174AdapterRF1:
		return "durable_adapter", "local_durable", nil
	case phase174RF3TCP:
		return "durable_adapter_replication_real_tcp", "sync_quorum_rf3", nil
	default:
		return "", "", fmt.Errorf("phase174: unsupported layer %q", layer)
	}
}

func newPhase174Pipeline(t *testing.T, layer phase174Layer, dir string, create bool) *phase174Pipeline {
	t.Helper()
	p := &phase174Pipeline{
		layer:       layer,
		primaryPath: filepath.Join(dir, "primary.store"),
	}
	p.primary = phase174OpenWALStore(t, p.primaryPath, create)
	if layer != phase174DirectWALStore {
		id := frontend.Identity{VolumeID: "phase174", ReplicaID: "primary", Epoch: 1, EndpointVersion: 1}
		view := &phase174HealthyView{projection: frontend.Projection{
			VolumeID: id.VolumeID, ReplicaID: id.ReplicaID, Epoch: id.Epoch,
			EndpointVersion: id.EndpointVersion, Healthy: true,
		}}
		p.backend = durable.NewStorageBackend(p.primary, view, id)
		p.backend.SetOperational(true, "phase174 fixed-work")
	}
	if layer == phase174RF3TCP {
		p.replication = NewReplicationVolume("phase174", p.primary)
		p.replication.SetDurabilityMode(DurabilitySyncQuorum)
		targets := make([]ReplicaTarget, 0, 2)
		for replica := 1; replica <= 2; replica++ {
			path := filepath.Join(dir, fmt.Sprintf("replica-%d.store", replica))
			store := phase174OpenWALStore(t, path, create)
			listener, err := transport.NewReplicaListener("127.0.0.1:0", store)
			if err != nil {
				t.Fatal(err)
			}
			listener.Serve()
			p.replicaPaths = append(p.replicaPaths, path)
			p.replicas = append(p.replicas, store)
			p.listeners = append(p.listeners, listener)
			targets = append(targets, ReplicaTarget{
				ReplicaID: fmt.Sprintf("r%d", replica), DataAddr: listener.Addr(),
				ControlAddr: listener.Addr(), Epoch: 1, EndpointVersion: 1,
			})
		}
		if err := p.replication.UpdateReplicaSet(1, targets); err != nil {
			t.Fatal(err)
		}
		p.backend.SetWriteObserver(p.replication)
		p.backend.SetWriteAckPolicy(durable.WriteAckRequireObserverAck)
	}
	t.Cleanup(func() { _ = p.close() })
	return p
}

func phase174OpenWALStore(t *testing.T, path string, create bool) *storage.WALStore {
	t.Helper()
	var (
		store *storage.WALStore
		err   error
	)
	if create {
		store, err = storage.CreateWALStore(path, phase174NumBlocks, phase174BlockSize)
	} else {
		store, err = storage.OpenWALStore(path)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !create {
		if _, err := store.Recover(); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	return store
}

func runPhase174FixedWork(t *testing.T, p *phase174Pipeline, set, run, writers int) phase174FixedWorkResult {
	t.Helper()
	warmupPayloads := phase174Payloads(phase174WarmupOperations, set, run, 0x1740)
	if _, _, err := phase174RunWrites(p, writers, phase174RegionBlocks, warmupPayloads, false); err != nil {
		t.Fatalf("warmup writes: %v", err)
	}
	if err := p.sync(context.Background()); err != nil {
		t.Fatalf("warmup sync: %v", err)
	}
	if reached := p.waitForReplicaFrontier(time.Second); len(p.replicas) > 0 && reached < 1 {
		t.Fatal("no replica reached warmup frontier")
	}

	writeBefore := p.primary.WriteInstrumentation()
	replicationBefore := p.replicationStats()
	payloads := phase174Payloads(phase174APIOperations, set, run, 0x1741)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	foreground, latencies, err := phase174RunWrites(p, writers, 0, payloads, true)
	if err != nil {
		t.Fatalf("measured writes: %v", err)
	}
	syncStart := time.Now()
	if err := p.sync(context.Background()); err != nil {
		t.Fatalf("final sync: %v", err)
	}
	finalSync := time.Since(syncStart)
	if reached := p.waitForReplicaFrontier(2 * time.Second); len(p.replicas) > 0 && reached < 1 {
		t.Fatalf("no replica reached final frontier")
	}
	writeAfter := p.primary.WriteInstrumentation()
	replicationAfter := p.replicationStats()
	if got := writeAfter.WALEncodeOps - writeBefore.WALEncodeOps; got != phase174APIOperations {
		t.Fatalf("primary WAL writes=%d want %d", got, phase174APIOperations)
	}
	if p.replication != nil && replicationAfter.WriteOps-replicationBefore.WriteOps != phase174APIOperations {
		t.Fatalf("replication writes=%d want %d", replicationAfter.WriteOps-replicationBefore.WriteOps, phase174APIOperations)
	}

	closeStart := time.Now()
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	primaryR, primaryH, durableReplicas, replicasEqual, correctness := phase174RecoverAndVerify(
		t, p.primaryPath, p.replicaPaths, payloads,
	)
	closeRecover := time.Since(closeStart)
	if primaryR != primaryH {
		t.Fatalf("recovered primary stable=%d head=%d", primaryR, primaryH)
	}

	scope, ackProfile, err := phase174LayerContract(p.layer)
	if err != nil {
		t.Fatal(err)
	}
	logicalBytes := uint64(phase174APIOperations * phase174BlockSize)
	p50, p95, p99 := phase174Percentiles(latencies)
	result := phase174FixedWorkResult{
		Contract: "phase174-fixed-work-v1", Layer: string(p.layer), Scope: scope,
		AckProfile: ackProfile, Set: set, Run: run, Writers: writers,
		APIOperations: phase174APIOperations, LogicalBytes: logicalBytes,
		ForegroundNanos: foreground.Nanoseconds(),
		MiBPerSecond:    (float64(logicalBytes) / (1024 * 1024)) / foreground.Seconds(),
		P50Nanos:        p50, P95Nanos: p95, P99Nanos: p99,
		FinalSyncNanos: finalSync.Nanoseconds(), CloseRecoverNanos: closeRecover.Nanoseconds(),
		PrimaryWALWrites: writeAfter.WALEncodeOps - writeBefore.WALEncodeOps,
		PrimaryStableLSN: primaryR, PrimaryHeadLSN: primaryH,
		ReplicaCount: len(p.replicaPaths), ReplicaDurableCount: durableReplicas,
		ReplicaFrontiersEqual: replicasEqual,
		CloseRecoverComplete:  true, CorrectnessSamples: correctness,
	}
	if p.replication != nil {
		result.ReplicationWrites = replicationAfter.WriteOps - replicationBefore.WriteOps
		result.ReplicationFanoutNanos = replicationAfter.WriteFanoutNanos - replicationBefore.WriteFanoutNanos
		result.ReplicationAckWaitNanos = replicationAfter.WriteAckWaitNanos - replicationBefore.WriteAckWaitNanos
		result.PeerQueueMaxDepth = replicationAfter.PeerQueueMaxDepth
		result.PeerQueueSaturated = replicationAfter.PeerQueueSaturated - replicationBefore.PeerQueueSaturated
	}
	return result
}

func phase174RunWrites(
	p *phase174Pipeline,
	writers int,
	base uint32,
	payloads [][]byte,
	measureLatency bool,
) (time.Duration, []int64, error) {
	latencies := make([]int64, len(payloads))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	start := make(chan struct{})
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	wallStart := time.Now()
	for worker := 0; worker < writers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for operation := worker; operation < len(payloads); operation += writers {
				opStart := time.Now()
				lba := phase174OperationLBA(operation, base)
				var err error
				if p.backend == nil {
					_, err = p.primary.Write(lba, payloads[operation])
				} else {
					_, err = p.backend.Write(ctx, int64(lba*phase174BlockSize), payloads[operation])
				}
				if measureLatency {
					latencies[operation] = time.Since(opStart).Nanoseconds()
				}
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	return time.Since(wallStart), latencies, firstErr
}

func (p *phase174Pipeline) sync(ctx context.Context) error {
	if p.backend != nil {
		return p.backend.Sync(ctx)
	}
	_, err := p.primary.Sync()
	return err
}

func (p *phase174Pipeline) replicationStats() VolumeStats {
	if p.replication == nil {
		return VolumeStats{}
	}
	return p.replication.Stats()
}

func (p *phase174Pipeline) waitForReplicaFrontier(timeout time.Duration) int {
	if len(p.replicas) == 0 {
		return 0
	}
	_, _, want := p.primary.Boundaries()
	deadline := time.Now().Add(timeout)
	for {
		reached := 0
		for _, replica := range p.replicas {
			r, _, h := replica.Boundaries()
			if r >= want && h >= want {
				reached++
			}
		}
		if reached == len(p.replicas) || time.Now().After(deadline) {
			return reached
		}
		time.Sleep(time.Millisecond)
	}
}

func (p *phase174Pipeline) close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	var firstErr error
	if p.backend != nil {
		firstErr = p.backend.Close()
	}
	if p.replication != nil {
		if err := p.replication.Close(); firstErr == nil {
			firstErr = err
		}
	}
	for _, listener := range p.listeners {
		listener.Stop()
	}
	if err := p.primary.Close(); firstErr == nil {
		firstErr = err
	}
	for _, replica := range p.replicas {
		if err := replica.Close(); firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func phase174RecoverAndVerify(
	t *testing.T,
	primaryPath string,
	replicaPaths []string,
	payloads [][]byte,
) (primaryR, primaryH uint64, durableReplicas int, replicasEqual bool, samples int) {
	t.Helper()
	paths := append([]string{primaryPath}, replicaPaths...)
	frontiers := make([]uint64, 0, len(paths))
	replicasEqual = true
	for pathIndex, path := range paths {
		store, err := storage.OpenWALStore(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Recover(); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		r, _, h := store.Boundaries()
		if r != h {
			_ = store.Close()
			t.Fatalf("path=%s stable=%d head=%d", path, r, h)
		}
		frontiers = append(frontiers, h)
		if pathIndex > 0 && h != primaryH {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if pathIndex > 0 {
			durableReplicas++
		}
		for _, operation := range phase174SampleOperations(len(payloads)) {
			got, err := store.Read(phase174OperationLBA(operation, 0))
			if err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			if !bytes.Equal(got, payloads[operation]) {
				_ = store.Close()
				t.Fatalf("path=%s operation=%d data mismatch", path, operation)
			}
			samples++
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if pathIndex == 0 {
			primaryR, primaryH = r, h
		}
	}
	for _, frontier := range frontiers[1:] {
		if frontier != frontiers[0] {
			replicasEqual = false
		}
	}
	return primaryR, primaryH, durableReplicas, replicasEqual, samples
}

func phase174OperationLBA(operation int, base uint32) uint32 {
	return base + uint32((operation*7919)%phase174RegionBlocks)
}

func phase174Payloads(operations, set, run int, marker uint64) [][]byte {
	payloads := make([][]byte, operations)
	for operation := range payloads {
		payload := make([]byte, phase174BlockSize)
		for offset := 0; offset < len(payload); offset += 8 {
			value := marker<<48 | uint64(set)<<40 | uint64(run)<<32 |
				uint64(operation+1)<<12 | uint64(offset/8)
			binary.LittleEndian.PutUint64(payload[offset:offset+8], value)
		}
		payloads[operation] = payload
	}
	return payloads
}

func phase174SampleOperations(operations int) []int {
	samples := []int{0, operations / 7, operations / 3, operations / 2, operations - 1}
	sort.Ints(samples)
	return samples
}

func phase174Percentiles(samples []int64) (p50, p95, p99 int64) {
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
