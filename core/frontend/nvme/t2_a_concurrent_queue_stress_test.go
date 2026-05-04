// Ownership: sw — L1-A sign-prep stress test for T2B-NVMe-product-
// ready. Exercises the session concurrency model under kernel-
// realistic load (N IO queues × M sequential Writes each) well
// above what mkfs.ext4 requires.
//
// Scope:
//   - 8 concurrent IO queues (matches Linux kernel default)
//   - 50 sequential Writes per queue = 400 Writes total
//   - Each Write is 32 KiB split into 8 × 4 KiB H2CData chunks
//     (kernel mkfs pattern from BUG-001 dmesg analysis)
//   - Pairing shape: send cmd → recv R2T → send N H2CData →
//     recv CapsuleResp → next cmd (V2-strict: one R2T outstanding
//     per session at a time, per BUG-001 revert rationale)
//
// Invariants pinned:
//   - backend.calls == numQueues * itersPerQueue
//   - backend.bytes == numQueues * itersPerQueue * writeSize
//   - no timeouts (per-queue wall-clock bounded)
//   - no status errors
//
// Complements (does not replace):
//   - TestT2V2Port_NVMe_IO_8Queues_Concurrent32KiBWrites (1 iter/queue)
//   - TestT2V2Port_NVMe_IO_AdminWhileIOInFlight (admin+IO shape)

package nvme_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// stressWriteTarget provisions a target with a large-enough
// volume so the stress writes don't collide with the default
// 1 MiB size (2048 LBAs). 64 MiB accommodates 400 × 32 KiB
// Writes at distinct non-overlapping regions.
func stressWriteTarget(t *testing.T, volumeSize uint64) (*writeCountingBackend, string) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	backend := &writeCountingBackend{inner: rec}
	prov := testback.NewStaticProvider(backend)
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
		Handler:   nvme.HandlerConfig{VolumeSize: volumeSize},
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Target.Start: %v", err)
	}
	t.Cleanup(func() { _ = tg.Close() })
	return backend, addr
}

// TestT2A_ConcurrentQueueStress runs 8 IO queues in parallel, each
// driving 50 sequential 32-KiB chunked Writes. Pins the post-
// BUG-001-revert V2-strict session model under sustained
// concurrent load.
func TestT2A_ConcurrentQueueStress(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode: skipping L1-A stress test (10+ s)")
	}

	const (
		numQueues     = 8
		itersPerQueue = 50
		writeSize     = 32 * 1024
		chunks        = 8
		nlbPerWrite   = uint16(writeSize / 512)
	)

	// 64 MiB volume — room for 400 × 32 KiB non-overlapping writes.
	backend, addr := stressWriteTarget(t, 64*1024*1024)
	cli := newMultiQueueClient(t, addr, "nqn.2026-04.example.v3:subsys",
		"nqn.2026-04.example.host:1", numQueues)

	makePayload := func(qIdx, iter int) []byte {
		p := make([]byte, writeSize)
		seed := byte((qIdx*131 + iter*7) & 0xFF)
		for i := range p {
			p[i] = seed + byte(i)
		}
		return p
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	queueErrs := make([]atomic.Uint32, numQueues)
	statusErrs := make([]atomic.Uint32, numQueues)
	deadline := time.Now().Add(60 * time.Second)

	for qi := 0; qi < numQueues; qi++ {
		qi, q := qi, cli.queues[qi]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for iter := 0; iter < itersPerQueue; iter++ {
				if time.Now().After(deadline) {
					queueErrs[qi].Add(1)
					return
				}
				slba := uint64(qi*itersPerQueue+iter) * uint64(nlbPerWrite)
				payload := makePayload(qi, iter)
				status, err := cli.chunkedWriteOnQueue(t, q, slba, nlbPerWrite, payload, chunks)
				if err != nil {
					queueErrs[qi].Add(1)
					return
				}
				if status != 0 {
					statusErrs[qi].Add(1)
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	close(start)

	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatal("queues hung — concurrency model may have regressed (deadlock or lost R2T)")
	}

	totalExpectedCalls := int32(numQueues * itersPerQueue)
	totalExpectedBytes := int64(numQueues) * int64(itersPerQueue) * int64(writeSize)

	if got := backend.calls.Load(); got != totalExpectedCalls {
		t.Errorf("backend.calls=%d want %d — missing %d Writes",
			got, totalExpectedCalls, totalExpectedCalls-got)
	}
	if got := backend.bytes.Load(); got != totalExpectedBytes {
		t.Errorf("backend.bytes=%d want %d — delta %d",
			got, totalExpectedBytes, totalExpectedBytes-got)
	}

	for i := 0; i < numQueues; i++ {
		if n := queueErrs[i].Load(); n > 0 {
			t.Errorf("queue %d: %d transport errors", i, n)
		}
		if n := statusErrs[i].Load(); n > 0 {
			t.Errorf("queue %d: %d status-error CapsuleResps", i, n)
		}
	}
}
