// Ownership: QA (regression for m01 pre-11c BLOCKER-1).
// sw may NOT modify without architect approval via §8B.4 Discovery
// Bridge.
//
// Purpose: reproduce in unit-scope the IO Write multi-chunk R2T
// accounting bug found on m01 2026-04-22 when `mkfs.ext4` issued a
// 32 KiB write:
//
//   kernel dmesg: "req 13 r2t len 32768 exceeded data len 32768 (28672 sent)"
//   v3 server:    "expected H2CData, got 0x4"
//
// Root-cause hypothesis: V3's handleWrite loop collects H2CData
// chunks summing to totalBytes; if N × 4 KiB chunks are sent by the
// kernel instead of one single 32 KiB PDU, some accounting drops
// one chunk. The Go harness's existing writeCmd helper sends the
// entire payload in ONE H2CData PDU, so this path never ran in
// Go-suite regression.
//
// This test emulates the kernel's chunked H2CData pattern so the
// bug surfaces at the Go unit level. Red→Green cycle drives the
// sw fix.
//
// Maps to ledger row (provisional): PCDD-NVME-IO-H2CDATA-CHUNKED-R2T-001

package nvme_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// writeCountingBackend wraps RecordingBackend and also exposes
// the total bytes actually written (for integrity pin).
//
// lastWrite uses atomic.Pointer to permit safe concurrent access
// from TestT2A_ConcurrentQueueStress (8 IO queues sharing one
// backend). Each Write Stores a fresh copied slice and never
// mutates it after store, so readers via Load() observe a stable
// snapshot. The chunked-R2T integrity tests still observe the
// most recent write — semantics preserved. Per §8B.4 Discovery
// Bridge (architect-approved 2026-04-26): test-fixture-only fix
// for the m01 -race finding under TestT2A_ConcurrentQueueStress.
type writeCountingBackend struct {
	inner     *testback.RecordingBackend
	calls     atomic.Int32
	bytes     atomic.Int64
	lastWrite atomic.Pointer[[]byte]
}

func (b *writeCountingBackend) Identity() frontend.Identity { return b.inner.Identity() }
func (b *writeCountingBackend) Close() error                { return b.inner.Close() }
func (b *writeCountingBackend) Sync(ctx context.Context) error {
	return b.inner.Sync(ctx)
}
func (b *writeCountingBackend) SetOperational(ok bool, evidence string) {
	b.inner.SetOperational(ok, evidence)
}
func (b *writeCountingBackend) Read(ctx context.Context, off int64, p []byte) (int, error) {
	return b.inner.Read(ctx, off, p)
}
func (b *writeCountingBackend) Write(ctx context.Context, off int64, p []byte) (int, error) {
	b.calls.Add(1)
	b.bytes.Add(int64(len(p)))
	// Capture for integrity check. atomic.Pointer makes this safe
	// for concurrent Writes (TestT2A_ConcurrentQueueStress shares
	// one backend across 8 IO queues). Each store is a fresh slice
	// that is never mutated after store, so Load() returns a stable
	// snapshot.
	cp := make([]byte, len(p))
	copy(cp, p)
	b.lastWrite.Store(&cp)
	return b.inner.Write(ctx, off, p)
}

// newChunkedWriteTarget spins up a target + client pair ready for
// a chunked Write; returns counting backend + client.
func newChunkedWriteTarget(t *testing.T) (*writeCountingBackend, *nvmeClient) {
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

// writeCmdChunked issues an IO Write with payload split across
// exactly `chunks` H2CData PDUs of equal size. Returns the
// controller's CapsuleResp status (0 = success) and any wire-level
// error surfaced before the resp arrived.
func writeCmdChunked(t *testing.T, c *nvmeClient, slba uint64, nlb uint16, payload []byte, chunks int) (uint16, error) {
	t.Helper()
	if len(payload)%chunks != 0 {
		t.Fatalf("payload %d not divisible by chunks %d", len(payload), chunks)
	}
	chunkSize := len(payload) / chunks
	cid := uint16(c.cid.Add(1))

	cmd := nvme.CapsuleCommand{
		OpCode: 0x01, // ioWrite
		CID:    cid,
		NSID:   1,
		D10:    uint32(slba & 0xFFFFFFFF),
		D11:    uint32(slba >> 32),
		D12:    uint32(nlb - 1),
	}
	if err := c.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		return 0, err
	}

	// Expect R2T (server requests the data).
	ch, err := c.ioR.Dequeue()
	if err != nil {
		return 0, err
	}
	if ch.Type != 0x9 /* pduR2T */ {
		if ch.Type == 0x5 /* pduCapsuleResp */ {
			var resp nvme.CapsuleResponse
			_ = c.ioR.Receive(&resp)
			return resp.Status, nil
		}
		t.Fatalf("expected R2T, got 0x%x", ch.Type)
	}
	var r2t nvme.R2THeader
	if err := c.ioR.Receive(&r2t); err != nil {
		return 0, err
	}

	// Send N H2CData chunks with proper DATAO advancing per chunk.
	for i := 0; i < chunks; i++ {
		offset := i * chunkSize
		h := nvme.H2CDataHeader{
			CCCID: cid,
			TAG:   r2t.TAG,
			DATAO: uint32(offset),
			DATAL: uint32(chunkSize),
		}
		if err := c.ioW.SendWithData(0x6 /* pduH2CData */, 0, &h, 16, payload[offset:offset+chunkSize]); err != nil {
			return 0, err
		}
	}

	// Receive CapsuleResp (success or server-side error status).
	resp := recvCapsuleResp(t, c.ioR)
	if resp.CID != cid {
		t.Fatalf("Write resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status, nil
}

// Repro: 8 × 4 KiB H2CData chunks for a 32 KiB Write — the exact
// pattern Linux kernel 6.17 emits during mkfs.ext4.
//
// RED expectation (pre-fix): status != 0 OR wire-level error OR
// timeout (server gets stuck in handleWrite loop waiting for
// one more chunk after 7 already arrived).
// GREEN expectation (post-fix): status==0, backend.calls==1,
// backend.bytes==32768, and the captured payload matches what we
// sent.
func TestT2V2Port_NVMe_IO_Write_8x4KiB_H2CDataChunks_mkfsPattern(t *testing.T) {
	backend, cli := newChunkedWriteTarget(t)

	// 32 KiB payload, 8 × 4 KiB chunks. V3 advertises 512-byte
	// LBA (nvme.DefaultBlockSize) — matches what m01 kernel saw
	// via Identify NS LBADS=9. For 32 KiB, NLB = 32768 / 512 = 64.
	const total = 32 * 1024
	const chunks = 8
	const nlb = uint16(total / 512)
	payload := make([]byte, total)
	for i := range payload {
		payload[i] = byte((i*7 + 13) & 0xFF) // distinguishable pattern
	}

	status, err := writeCmdChunked(t, cli, 0, nlb, payload, chunks)
	if err != nil {
		t.Fatalf("wire error during chunked Write: %v (handleWrite may have closed the connection mid-stream)", err)
	}
	if status != 0 {
		t.Fatalf("chunked Write status=0x%04x SCT=%d SC=0x%02x; want success — "+
			"server likely miscounted H2CData chunks (BLOCKER-1 from m01 sanity)",
			status, sctOf(status), scOf(status))
	}

	if calls := backend.calls.Load(); calls != 1 {
		t.Fatalf("backend.Write called %d times; want exactly 1 (no target-side chunk coalescing bug)", calls)
	}
	if bytes := backend.bytes.Load(); bytes != total {
		t.Fatalf("backend received %d bytes; want %d (lost %d bytes — BLOCKER-1 repro)",
			bytes, total, total-bytes)
	}

	// Integrity: every byte the backend got must match what we sent.
	lastWritePtr := backend.lastWrite.Load()
	if lastWritePtr == nil {
		t.Fatalf("lastWrite is nil; want %d-byte capture", total)
	}
	lastWrite := *lastWritePtr
	if len(lastWrite) != total {
		t.Fatalf("lastWrite len=%d want %d", len(lastWrite), total)
	}
	for i := 0; i < total; i++ {
		if lastWrite[i] != payload[i] {
			t.Fatalf("byte mismatch at offset %d: got 0x%02x want 0x%02x", i, lastWrite[i], payload[i])
		}
	}
}

// Adjacent shapes to localize the bug on the size spectrum.
// Each test is independent; running together after a fix gives
// confidence the fix is size-general, not a single-case patch.
func TestT2V2Port_NVMe_IO_Write_ChunkingShapes(t *testing.T) {
	// NLB is blocks at 512 B/block (V3 advertised LBA size).
	cases := []struct {
		name     string
		totalKiB int
		chunks   int
	}{
		{"2x4KiB_8KiB_total", 8, 2},
		{"4x4KiB_16KiB_total", 16, 4},
		{"8x4KiB_32KiB_total", 32, 8},
		{"16x2KiB_32KiB_total", 32, 16},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			backend, cli := newChunkedWriteTarget(t)
			total := tc.totalKiB * 1024
			payload := make([]byte, total)
			for i := range payload {
				payload[i] = byte(i & 0xFF)
			}
			nlb := uint16(total / 512)
			status, err := writeCmdChunked(t, cli, 0, nlb, payload, tc.chunks)
			if err != nil {
				t.Fatalf("wire error: %v", err)
			}
			if status != 0 {
				t.Fatalf("status=0x%04x (SCT=%d SC=0x%02x) — size %d chunks %d",
					status, sctOf(status), scOf(status), total, tc.chunks)
			}
			if got := backend.bytes.Load(); got != int64(total) {
				t.Fatalf("backend bytes=%d want %d (lost %d)", got, total, int64(total)-got)
			}
		})
	}
}
