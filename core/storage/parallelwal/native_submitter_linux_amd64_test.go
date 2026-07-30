//go:build linux && amd64

package parallelwal

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIOUringOwnerBatchesAcrossLanesAndRecoversPortably(t *testing.T) {
	path := filepath.Join(t.TempDir(), "native.bin")
	cfg := testConfig()
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}

	blocks := [][]byte{
		testBlock(0x11, cfg.BlockSize),
		testBlock(0x22, cfg.BlockSize),
		testBlock(0x33, cfg.BlockSize),
		testBlock(0x44, cfg.BlockSize),
	}
	lsns, err := store.WriteBatch(0, blocks)
	if err != nil {
		t.Fatal(err)
	}
	if len(lsns) != 4 || lsns[0] != 1 || lsns[3] != 4 {
		t.Fatalf("LSNs=%v want=[1 2 3 4]", lsns)
	}
	stats := store.NativeIOStats()
	if !stats.Enabled || stats.AdmittedRequests != 4 ||
		stats.SubmissionRounds != 1 || stats.SQEs != 4 ||
		stats.CompletionCount != 4 || stats.InflightHighWater != 4 ||
		stats.FallbackCount != 0 {
		t.Fatalf("native stats=%+v", stats)
	}
	t.Logf(
		"native_owner_stats enabled=%t admitted=%d rounds=%d sqes=%d completions=%d inflight_high_water=%d fallback=%d",
		stats.Enabled,
		stats.AdmittedRequests,
		stats.SubmissionRounds,
		stats.SQEs,
		stats.CompletionCount,
		stats.InflightHighWater,
		stats.FallbackCount,
	)
	if stable, err := store.Sync(); err != nil || stable != 4 {
		t.Fatalf("Sync=(%d,%v) want=(4,nil)", stable, err)
	}
	stats = store.NativeIOStats()
	if stats.DurabilityBarriers != 2 || stats.FsyncCompletions != 2 {
		t.Fatalf("native durability stats=%+v want two completed barriers", stats)
	}
	t.Logf(
		"native_durability_stats barriers=%d fsync_completions=%d submit_syscalls=%d",
		stats.DurabilityBarriers,
		stats.FsyncCompletions,
		stats.SubmitSyscalls,
	)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if recovered, err := reopened.Recover(); err != nil || recovered != 4 {
		t.Fatalf("Recover=(%d,%v) want=(4,nil)", recovered, err)
	}
	for lba, want := range blocks {
		got, err := reopened.Read(uint32(lba))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("LBA %d mismatch after portable reopen", lba)
		}
	}
}

func TestIOUringOwnerRotatesAcrossLanesAtDepthOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "depth-one.bin")
	cfg := testConfig()
	cfg.QueueDepth = 1
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	blocks := [][]byte{
		testBlock(0x51, cfg.BlockSize),
		testBlock(0x52, cfg.BlockSize),
		testBlock(0x53, cfg.BlockSize),
		testBlock(0x54, cfg.BlockSize),
	}
	if _, err := store.WriteBatch(0, blocks); err != nil {
		t.Fatal(err)
	}
	stats := store.NativeIOStats()
	if stats.QueueDepth != 1 || stats.AdmittedRequests != 4 || stats.SubmissionRounds != 4 ||
		stats.SQEs != 4 || stats.CompletionCount != 4 {
		t.Fatalf("depth-one native stats=%+v", stats)
	}
}

func TestIOUringOwnerUsesMultipleRoundsWhenDepthBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded-rounds.bin")
	cfg := testConfig()
	cfg.QueueDepth = 2
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	blocks := [][]byte{
		testBlock(0x61, cfg.BlockSize),
		testBlock(0x62, cfg.BlockSize),
		testBlock(0x63, cfg.BlockSize),
		testBlock(0x64, cfg.BlockSize),
	}
	if _, err := store.WriteBatch(0, blocks); err != nil {
		t.Fatal(err)
	}
	stats := store.NativeIOStats()
	if stats.QueueDepth != 2 || stats.SubmissionRounds != 2 ||
		stats.SQEs != 4 || stats.CompletionCount != 4 ||
		stats.InflightHighWater != 2 || stats.FallbackCount != 0 {
		t.Fatalf("bounded-round native stats=%+v", stats)
	}
}

func TestNativeReusesLaneEncodeBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reuse-buffer.bin")
	cfg := testConfig()
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.Write(0, testBlock(0x65, cfg.BlockSize)); err != nil {
		t.Fatal(err)
	}
	first := &store.native.buffers[0][0]
	if _, err := store.Write(4, testBlock(0x66, cfg.BlockSize)); err != nil {
		t.Fatal(err)
	}
	second := &store.native.buffers[0][0]
	if first != second {
		t.Fatal("same-lane native encode buffer was reallocated")
	}
	stats := store.NativeIOStats()
	if stats.BufferAllocations != 1 {
		t.Fatalf("buffer allocations=%d want=1", stats.BufferAllocations)
	}
}

func TestNativeRingWrapRecyclesAndRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "native-wrap.bin")
	cfg := Config{
		NumBlocks:     8,
		BlockSize:     512,
		LaneCount:     1,
		StripeBlocks:  1,
		SlotsPerLane:  4,
		RetainPerLane: 2,
		QueueDepth:    4,
		Execution:     ExecutionIOUring,
	}
	store, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := store.Write(0, testBlock(byte(i), cfg.BlockSize)); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		if stable, err := store.Sync(); err != nil || stable != uint64(i) {
			t.Fatalf("Sync %d=(%d,%v)", i, stable, err)
		}
	}
	stats := store.NativeIOStats()
	if stats.SubmissionRounds != 10 || stats.FallbackCount != 0 {
		t.Fatalf("native wrap stats=%+v", stats)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if recovered, err := reopened.Recover(); err != nil || recovered != 10 {
		t.Fatalf("Recover=(%d,%v) want=(10,nil)", recovered, err)
	}
	data, err := reopened.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 10 {
		t.Fatalf("latest byte=%d want=10", data[0])
	}
}

func TestNativeCloseWithInflightWriteRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "native-close-inflight.bin")
	cfg := testConfig()
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	store.lanes[0].beforeWrite = func(*writeRequest) {
		close(entered)
		<-release
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := store.Write(0, testBlock(0x51, cfg.BlockSize))
		writeDone <- writeErr
	}()
	<-entered

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- store.Close()
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned with a native write in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if recovered, err := reopened.Recover(); err != nil || recovered != 1 {
		t.Fatalf("Recover=(%d,%v) want=(1,nil)", recovered, err)
	}
	data, err := reopened.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0x51 {
		t.Fatalf("recovered byte=%02x want=51", data[0])
	}
}

func TestNativeSyncIsNotStarvedByLaterWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync-liveness.bin")
	cfg := testConfig()
	cfg.SlotsPerLane = 4096
	cfg.RetainPerLane = 2048
	cfg.QueueDepth = 64
	cfg.Execution = ExecutionIOUring
	store, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var writes atomic.Uint64
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for writer := 0; writer < 8; writer++ {
		writers.Add(1)
		go func(writer int) {
			defer writers.Done()
			data := testBlock(byte(writer+1), cfg.BlockSize)
			for {
				select {
				case <-stop:
					return
				default:
					if _, err := store.Write(uint32(writer), data); err != nil {
						return
					}
					writes.Add(1)
				}
			}
		}(writer)
	}

	deadline := time.Now().Add(2 * time.Second)
	for writes.Load() < 32 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if writes.Load() < 32 {
		close(stop)
		writers.Wait()
		_ = store.Close()
		t.Fatal("writers did not establish continuous load")
	}

	syncDone := make(chan error, 1)
	go func() {
		_, err := store.Sync()
		syncDone <- err
	}()
	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("Sync under later writes: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(stop)
		writers.Wait()
		t.Fatal("Sync was starved by later writes")
	}
	close(stop)
	writers.Wait()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
