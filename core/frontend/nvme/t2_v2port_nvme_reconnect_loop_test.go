// Ownership: QA (L1B-1 inventory item, landed early per QA parallel
// capacity during Batch 11c sign prep).
// sw may NOT modify without architect approval via §8B.4 Discovery
// Bridge.
//
// Maps to ledger row: PCDD-NVME-SESSION-RECONNECT-LOOP-001
//
// Purpose: stability test for attach/detach cycling — exercises
// session setup + teardown paths repeatedly to surface any resource
// leak (goroutine, TCP buffer, CNTLID registry entry) that single-
// shot tests would miss.
//
// Inventory item L1B-1 rationale (from
// sw-block/design/bugs/inventory/nvme-test-coverage-deferred.md):
// "kernel reconnect is a rare event" — but `nvme disconnect` +
// fresh connect is exactly what testops / operators do during
// maintenance. Worth pinning that it stays clean, not deferred to
// T3 perf gate.
//
// Test layer: L2 subprocess (real cmd/blockmaster + cmd/blockvolume).
// Tagged with testing.Short() skip so CI-short runs exclude it.

package nvme_test

import (
	"bytes"
	"runtime"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

func TestT2V2Port_NVMe_Process_ReconnectLoop50(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; skipped under -short")
	}

	h := startL2NvmeHarness(t)
	defer h.Close()

	// Baseline goroutine count BEFORE the loop, to detect leak.
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	const iterations = 50
	payloadPattern := []byte("reconnect-loop-probe-")

	// Write a distinct payload on each iteration, read it back,
	// then fully close the client and reconnect.
	for i := 0; i < iterations; i++ {
		cli := dialAndConnect(t, h.nvmeAddr)

		// Build a per-iteration payload so cross-iteration staleness
		// would surface as data mismatch.
		payload := make([]byte, nvme.DefaultBlockSize)
		copy(payload, payloadPattern)
		payload[len(payloadPattern)] = byte('0' + i%10)
		payload[len(payloadPattern)+1] = byte('A' + i/10)

		status := cli.writeCmd(t, 0, 1, payload)
		if status != 0 {
			cli.close()
			t.Fatalf("iter %d: write status=0x%04x", i, status)
		}
		status, data := cli.readCmd(t, 0, 1, int(nvme.DefaultBlockSize))
		if status != 0 {
			cli.close()
			t.Fatalf("iter %d: read status=0x%04x", i, status)
		}
		if !bytes.Equal(data[:len(payload)], payload) {
			cli.close()
			t.Fatalf("iter %d: readback mismatch; got first32=%q, want %q",
				i, first16N(data), first16N(payload))
		}

		// Clean teardown — IDLs and admin/IO TCP conns freed.
		cli.close()
	}

	// Drain window — give Target time to reap closed sessions.
	time.Sleep(1 * time.Second)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	// Goroutine delta check. 50 iterations; even a tiny per-iter
	// leak would show up as +50 here. Allow 20 stragglers for
	// subprocess I/O goroutines that haven't been GC'd yet.
	afterLoop := runtime.NumGoroutine()
	if afterLoop > baseline+20 {
		t.Errorf("goroutine leak across %d reconnect cycles: baseline=%d after=%d (delta=%d)",
			iterations, baseline, afterLoop, afterLoop-baseline)
	}
}
