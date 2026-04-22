// Ownership: QA (Batch 11 A-tier Phase 3 per
// sw-block/design/v3-phase-15-t2-batch-11-test-skeleton.md §A11.4).
// sw may NOT modify this file without architect approval via §8B.4
// Discovery Bridge.
//
// Maps to ledger row:
//   PCDD-NVME-IO-NO-TARGET-RETRY-001
//
// Pins port plan R6 (no target-side retry). V2 had write_retry.go
// performing target-side retry — explicitly rejected as retry-as-
// authority (see port plan §5 not-port list). V3 must return IO
// errors to the host verbatim; retry (if any) is the host kernel
// blk_mq's responsibility.
//
// Test shape follows skeleton §A11.4:
//   - Inline test-local fault backend (no production error type
//     pollution)
//   - Per-call WriteCount + ReadCount visible to the test
//   - Host issues ONE IO op; assertions pin backend.WriteCount == 1
//
// Test layer: Unit (library-level; Target + NVMeClient in-process).

package nvme_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// errFaultInjected is a test-local sentinel for A11.4. Deliberately
// not in the production frontend package (skeleton §A11.4 guidance).
var errFaultInjected = errors.New("t2_v2port_no_retry: injected IO fault")

// countingFaultBackend wraps a RecordingBackend, returning
// errFaultInjected on the next failWrites writes (then delegating
// to the underlying Read/Write). Exposes atomic WriteCalls /
// ReadCalls for the test to pin "called exactly once".
type countingFaultBackend struct {
	inner      *testback.RecordingBackend
	failWrites atomic.Int32 // remaining writes to fail
	writeCalls atomic.Int32 // total writes attempted (including failed)
	readCalls  atomic.Int32
}

func (c *countingFaultBackend) Identity() frontend.Identity {
	return c.inner.Identity()
}

func (c *countingFaultBackend) Close() error {
	return c.inner.Close()
}

func (c *countingFaultBackend) Read(ctx context.Context, offset int64, p []byte) (int, error) {
	c.readCalls.Add(1)
	return c.inner.Read(ctx, offset, p)
}

func (c *countingFaultBackend) Write(ctx context.Context, offset int64, p []byte) (int, error) {
	c.writeCalls.Add(1)
	if c.failWrites.Load() > 0 {
		c.failWrites.Add(-1)
		return 0, errFaultInjected
	}
	return c.inner.Write(ctx, offset, p)
}

// newFaultyTargetClient builds a Target fronted by a
// countingFaultBackend with `failN` initial write failures queued.
func newFaultyTargetClient(t *testing.T, failN int32) (*countingFaultBackend, *nvmeClient) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	backend := &countingFaultBackend{inner: rec}
	backend.failWrites.Store(failN)

	prov := testback.NewStaticProvider(backend)
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen: "127.0.0.1:0",
		// Match dialAndConnect's default SubNQN so happy-path
		// Connect succeeds; the test's focus is the IO path, not
		// Connect param validation.
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Target.Start: %v", err)
	}

	cli := dialAndConnect(t, addr)
	t.Cleanup(func() {
		cli.close()
		_ = tg.Close()
	})
	return backend, cli
}

// --- QA A11.4 — no target-side retry (port plan R6) ---

func TestT2V2Port_NVMe_IO_ErrorsReturnedVerbatim(t *testing.T) {
	t.Run("TransientWriteErrorNotRetriedByTarget", func(t *testing.T) {
		// Backend fails next 3 writes. Host issues ONE Write.
		// Pin: backend sees exactly 1 write call (no target-side
		// retry loop); host receives a spec-legal non-success
		// status.
		backend, cli := newFaultyTargetClient(t, 3)

		payload := make([]byte, nvme.DefaultBlockSize)
		status := cli.writeCmd(t, 0, 1, payload)

		if got := backend.writeCalls.Load(); got != 1 {
			t.Fatalf("backend.writeCalls=%d; want 1 (target-side retry detected — R6 violated)", got)
		}
		if status == 0 {
			t.Fatalf("Write returned success (0x0000) despite injected fault; error swallowed")
		}
		// Status must be a real error — we don't pin the exact
		// SCT/SC code (sw chooses; 0x80 Write Fault / 0x06
		// Internal Error both spec-legal). Just assert non-zero
		// with SCT!=0 OR SC!=0.
		if sctOf(status) == 0 && scOf(status) == 0 {
			t.Fatalf("Write status=0x%04x has SCT=0 and SC=0; not a real error", status)
		}
	})

	t.Run("SubsequentWriteReachesBackend", func(t *testing.T) {
		// After a failed write, a second write must still reach
		// the backend — no persistent gate / latched failure mode
		// on the target side. (V2's write_retry.go had a subtle
		// "pause all writes during pressure" path; R6 rejects it.)
		backend, cli := newFaultyTargetClient(t, 1)

		payload := make([]byte, nvme.DefaultBlockSize)

		_ = cli.writeCmd(t, 0, 1, payload) // first: fails
		status2 := cli.writeCmd(t, 1, 1, payload) // second: succeeds

		if status2 != 0 {
			t.Fatalf("second write status=0x%04x; want success after transient fault exhausted", status2)
		}
		if got := backend.writeCalls.Load(); got != 2 {
			t.Fatalf("backend.writeCalls=%d; want 2 (target did not reach backend for second write)", got)
		}
	})

	t.Run("ReadPathAlsoNoRetry", func(t *testing.T) {
		// Symmetric with write — but on the happy path (no read
		// fault injection in this helper). Pin: one Read host
		// command → one backend Read call. If target had a
		// speculative "read ahead + retry" layer, count would
		// exceed 1.
		backend, cli := newFaultyTargetClient(t, 0)

		// Seed with a write so the read can return real data.
		payload := make([]byte, nvme.DefaultBlockSize)
		for i := range payload {
			payload[i] = byte(i & 0xFF)
		}
		_ = cli.writeCmd(t, 0, 1, payload)

		readsBefore := backend.readCalls.Load()
		status, _ := cli.readCmd(t, 0, 1, int(nvme.DefaultBlockSize))
		readsAfter := backend.readCalls.Load()

		if status != 0 {
			t.Fatalf("Read status=0x%04x; want success", status)
		}
		if delta := readsAfter - readsBefore; delta != 1 {
			t.Fatalf("Read triggered %d backend reads; want 1 (no target-side read amplification)", delta)
		}
	})
}
