package nvme_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type syncPairView struct{ id frontend.Identity }

func (v syncPairView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: v.id.VolumeID, ReplicaID: v.id.ReplicaID, Epoch: v.id.Epoch, EndpointVersion: v.id.EndpointVersion, Healthy: true}
}

type syncPairObserver struct {
	s     *storage.WALStore
	calls atomic.Int32
	fail  atomic.Bool
}

func (o *syncPairObserver) Observe(context.Context, uint32, uint64, []byte) error { return nil }
func (o *syncPairObserver) Sync(_ context.Context, target uint64) error {
	o.calls.Add(1)
	if o.fail.Load() {
		return errors.New("injected replica barrier failure")
	}
	n, err := o.s.Sync()
	if err == nil && n < target {
		return errors.New("local sync below required frontier")
	}
	return err
}

func newSyncPairFixture(t *testing.T, path string) (*storage.WALStore, *durable.StorageBackend, *nvmeClient) {
	t.Helper()
	s, err := storage.CreateWALStore(path, 32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	backend := durable.NewStorageBackend(s, syncPairView{id}, id)
	backend.SetOperational(true, "test recovered store")
	tg := nvme.NewTarget(nvme.TargetConfig{Listen: "127.0.0.1:0", SubsysNQN: "nqn.2026-04.example.v3:subsys", VolumeID: "v1", Provider: testback.NewStaticProvider(backend)})
	addr, err := tg.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tg.Close() })
	c := dialAndConnect(t, addr)
	t.Cleanup(c.close)
	if err := c.io.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return s, backend, c
}
func syncPairFlush(t *testing.T, c *nvmeClient, cid uint16) uint16 {
	t.Helper()
	cmd := nvme.CapsuleCommand{OpCode: 0, CID: cid, NSID: 1}
	if err := c.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatal(err)
	}
	resp := recvCapsuleResp(t, c.ioR)
	if resp.CID != cid {
		t.Fatalf("Flush CID=%d want %d", resp.CID, cid)
	}
	return resp.Status
}

// Model the observed guest fsync's two Flush capsules, not a mounted guest.
func TestNVMePairedFlushSharesPhysicalWALSync(t *testing.T) {
	s, backend, c := newSyncPairFixture(t, filepath.Join(t.TempDir(), "store.bin"))
	observer := &syncPairObserver{s: s}
	backend.SetWriteObserver(observer)
	if status := c.writeCmd(t, 0, 8, make([]byte, 4096)); status != 0 {
		t.Fatalf("Write status=%x", status)
	}
	for i := 0; i < 2; i++ {
		if status := syncPairFlush(t, c, uint16(90+i)); status != 0 {
			t.Fatalf("Flush %d status=%x", i, status)
		}
	}
	if n := s.WALSyncCount(); n != 1 {
		t.Fatalf("one write + two Flush capsules: physical WAL syncs=%d want 1", n)
	}
	if n := backend.WriteProfile().BackendSyncOps; n != 2 {
		t.Fatalf("backend Sync calls=%d want 2 (retain gates)", n)
	}
	if n := observer.calls.Load(); n != 2 {
		t.Fatalf("replication barriers=%d want 2", n)
	}
	observer.fail.Store(true)
	if status := syncPairFlush(t, c, 92); status == 0 {
		t.Fatal("cached local sync bypassed failed replica barrier")
	}
	if n := observer.calls.Load(); n != 3 {
		t.Fatalf("replication barriers=%d want 3", n)
	}
	backend.SetOperational(false, "test readiness lost")
	if err := backend.Sync(context.Background()); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("cached flush readiness error=%v", err)
	}
}

// The child ACKs real TCP Flush completions and never calls Close. The parent
// kills it, then opens/replays the file. This is process-crash, not power-loss, evidence.
func TestNVMePairedFlushCrashRecovery(t *testing.T) {
	const childEnv = "SW_BLOCK_SYNC_CRASH_CHILD"
	if path := os.Getenv(childEnv); path != "" {
		_, _, c := newSyncPairFixture(t, path)
		if status := c.writeCmd(t, 0, 8, bytes.Repeat([]byte{0x6d}, 4096)); status != 0 {
			t.Fatalf("Write status=%x", status)
		}
		for i := 0; i < 2; i++ {
			if status := syncPairFlush(t, c, uint16(100+i)); status != 0 {
				t.Fatalf("Flush status=%x", status)
			}
		}
		fmt.Println("FLUSH_ACKED_NO_CLOSE")
		time.Sleep(time.Hour)
		return
	}
	path := filepath.Join(t.TempDir(), "crashed.bin")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNVMePairedFlushCrashRecovery$")
	cmd.Env = append(os.Environ(), childEnv+"="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	ack := make(chan bool, 1)
	go func() {
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			if scan.Text() == "FLUSH_ACKED_NO_CLOSE" {
				ack <- true
				return
			}
		}
		ack <- false
	}()
	select {
	case ok := <-ack:
		if !ok {
			_ = cmd.Process.Kill()
			_ = cmd.Wait() // join stderr copying before reading the buffer
			t.Fatalf("child failed before ACK: %s", stderr.String())
		}
	case <-ctx.Done():
		t.Fatal("child did not ACK within bound")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child exited normally instead of crash")
	}
	s, err := storage.OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{0x6d}, 4096)) {
		t.Fatal("ACKed Flush data lost after process crash")
	}
}

// Opt-in serial tp04 proxy; neither a guest filesystem benchmark nor a gate.
func TestNVMePairedFlushLatency(t *testing.T) {
	if os.Getenv("SW_BLOCK_SYNC_PERF") != "1" {
		t.Skip("opt-in file-backed latency characterization")
	}
	s, backend, c := newSyncPairFixture(t, filepath.Join(t.TempDir(), "perf.bin"))
	const samples = 100
	ns := make([]int64, 0, samples)
	for i := 0; i < samples; i++ {
		if status := c.writeCmd(t, 0, 8, bytes.Repeat([]byte{byte(i)}, 4096)); status != 0 {
			t.Fatalf("Write status=%x", status)
		}
		start := time.Now()
		for j := 0; j < 2; j++ {
			if status := syncPairFlush(t, c, uint16(200+2*i+j)); status != 0 {
				t.Fatalf("Flush status=%x", status)
			}
		}
		ns = append(ns, time.Since(start).Nanoseconds())
	}
	sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
	var sum int64
	for _, n := range ns {
		sum += n
	}
	report := map[string]any{"scope": "TCP paired-Flush proxy, no mounted guest", "samples": samples, "mean_ns": sum / samples, "p50_ns": ns[49], "p95_ns": ns[94], "p99_ns": ns[98], "physical_wal_syncs": s.WALSyncCount(), "backend_sync_calls": backend.WriteProfile().BackendSyncOps}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("SYNC_PERF %s\n", data)
}
