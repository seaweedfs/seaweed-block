// Ownership: sw — L1-A sign-prep stress test for T2B-NVMe-product-
// ready. Pins single-queue Write behavior across a range of sizes
// up to MDTS boundary (32 KiB per Identify Controller MDTS=3 at
// 4 KiB page). Linux kernel caps its single-cmd IO size at MDTS;
// any larger host request would split into multiple cmds. This
// test exercises sustained sequential load at each size.
//
// Invariants pinned per subtest:
//   - Every iteration emits exactly one R2T with DATAL == expected
//     totalBytes (single-R2T-per-cmd invariant from V2-strict port)
//   - backend.calls increments by 1 per iteration
//   - backend.bytes increments by writeSize per iteration
//   - Each Write returns Success status
//
// Complements existing per-shape tests (Write_ChunkingShapes, the
// 32 KiB mkfsPattern) by adding iteration count — 100 × each size
// surfaces any leak / race that a single-shot test misses.

package nvme_test

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// TestT2A_LargeWriteStress iterates 100 chunked Writes at each of
// several realistic sizes, asserting backend bookkeeping matches
// expectations after each size group.
func TestT2A_LargeWriteStress(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode: skipping L1-A stress test")
	}

	shapes := []struct {
		name       string
		writeSize  uint32
		chunks     int
		iterations int
	}{
		{"1LBA_1chunk", 512, 1, 100},
		{"8LBA_1chunk", 4096, 1, 100},
		{"16LBA_2chunks", 8192, 2, 100},
		{"32LBA_4chunks", 16384, 4, 100},
		{"64LBA_8chunks_MDTS", 32768, 8, 100},
	}

	for _, shape := range shapes {
		shape := shape
		t.Run(shape.name, func(t *testing.T) {
			if shape.writeSize%uint32(shape.chunks) != 0 {
				t.Fatalf("writeSize %d not divisible by chunks %d",
					shape.writeSize, shape.chunks)
			}

			volumeSize := uint64(shape.iterations) * uint64(shape.writeSize)
			if volumeSize < 1024*1024 {
				volumeSize = 1024 * 1024
			}
			backend, addr := stressWriteTarget(t, volumeSize)
			cli := dialAndConnect(t, addr)
			defer cli.close()

			nlb := uint16(shape.writeSize / 512)
			for iter := 0; iter < shape.iterations; iter++ {
				payload := make([]byte, shape.writeSize)
				seed := byte((iter * 17) & 0xFF)
				for i := range payload {
					payload[i] = seed + byte(i)
				}
				slba := uint64(iter) * uint64(nlb)
				status := writeChunkedOnClient(t, cli, slba, nlb, payload, shape.chunks)
				if status != 0 {
					t.Fatalf("iter=%d status=0x%04x", iter, status)
				}
			}

			expectedCalls := int32(shape.iterations)
			expectedBytes := int64(shape.iterations) * int64(shape.writeSize)
			if got := backend.calls.Load(); got != expectedCalls {
				t.Errorf("backend.calls=%d want %d", got, expectedCalls)
			}
			if got := backend.bytes.Load(); got != expectedBytes {
				t.Errorf("backend.bytes=%d want %d", got, expectedBytes)
			}
		})
	}
}

// writeChunkedOnClient issues a Write with N H2CData chunks on
// an nvmeClient's IO queue. Asserts single-R2T-per-cmd invariant
// (DATAL == len(payload)) then sends chunks and returns the final
// CapsuleResp status. Distinct from nvmeClient.writeCmd which
// sends a single H2CData covering the whole payload.
func writeChunkedOnClient(t *testing.T, c *nvmeClient, slba uint64, nlb uint16, payload []byte, chunks int) uint16 {
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
		t.Fatalf("send Write: %v", err)
	}

	ch, err := c.ioR.Dequeue()
	if err != nil {
		t.Fatalf("recv R2T hdr: %v", err)
	}
	if ch.Type == 0x5 {
		var resp nvme.CapsuleResponse
		_ = c.ioR.Receive(&resp)
		return resp.Status
	}
	if ch.Type != 0x9 {
		t.Fatalf("expected R2T (0x9), got 0x%x", ch.Type)
	}
	var r2t nvme.R2THeader
	if err := c.ioR.Receive(&r2t); err != nil {
		t.Fatalf("recv R2T: %v", err)
	}
	if r2t.DATAL != uint32(len(payload)) {
		t.Fatalf("R2T DATAL=%d want %d (single-R2T-per-cmd invariant broken)",
			r2t.DATAL, len(payload))
	}

	for i := 0; i < chunks; i++ {
		off := i * chunkSize
		h := nvme.H2CDataHeader{
			CCCID: cid,
			TAG:   r2t.TAG,
			DATAO: uint32(off),
			DATAL: uint32(chunkSize),
		}
		if err := c.ioW.SendWithData(0x6, 0, &h, 16, payload[off:off+chunkSize]); err != nil {
			t.Fatalf("send H2CData[%d]: %v", i, err)
		}
	}

	resp := recvCapsuleResp(t, c.ioR)
	if resp.CID != cid {
		t.Fatalf("Write resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status
}
