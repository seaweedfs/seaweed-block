// Ownership: QA (BUG-001 repro — queue-internal pipeline).
// sw may NOT modify without architect approval via §8B.4 Discovery
// Bridge.
//
// REVISION 2026-04-22 (post-c0c0a1d revert to V2-strict):
// The original 2026-04-22 12:32 PT draft expected ASYNC R2T
// emission (server sends N R2Ts immediately after receiving N
// pipelined cmds). That pattern is incompatible with real Linux
// kernel behavior — kernel expects ONE R2T outstanding at a time
// and pumps H2CData serially per cmd (V2 model, confirmed working
// on m01 historically). See feedback_porting_discipline.md second
// citation.
//
// Current shape (kernel-realistic, V2-strict compatible):
//   1. Send Cmd-0 + Cmd-1 back-to-back on one IO queue (pipelined
//      capsule delivery — exercises server's pendingCapsules /
//      bufferInterleaved path in V2's rxLoop)
//   2. Recv R2T-0 → send H2CData-0 (all chunks) → recv CapResp-0
//   3. Recv R2T-1 → send H2CData-1 (all chunks) → recv CapResp-1
//
// The PRE-FIX BUG-001 failure mode (serial rxLoop reads Cmd-1
// as type 0x04 inside handleWrite's H2CData loop) is still
// detected by this shape: server's "expected H2CData, got 0x4"
// error fires before we reach the second R2T read.

package nvme_test

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// TestT2V2Port_NVMe_IO_Pipelined2WritesSameQueue pins the V2-strict
// two-cmd pipeline shape: server may issue R2Ts in arrival order
// but MUST process one cmd's H2CData to completion before emitting
// the next cmd's R2T (single-R2T-outstanding invariant).
func TestT2V2Port_NVMe_IO_Pipelined2WritesSameQueue(t *testing.T) {
	backend, cli := newChunkedWriteTarget(t)

	const total = 32 * 1024
	const chunks = 8
	const nlb = uint16(total / 512)

	payload0 := make([]byte, total)
	payload1 := make([]byte, total)
	for i := range payload0 {
		payload0[i] = byte((i + 0x11) & 0xFF)
		payload1[i] = byte((i + 0x22) & 0xFF)
	}

	// --- Phase 1: pipeline both CapsuleCmds before any R2T read ---
	//
	// This is the kernel behavior that broke pre-c0c0a1d serial
	// dispatch. V2-strict handles it via pendingCapsules drain.

	cid0 := uint16(cli.cid.Add(1))
	cmd0 := nvme.CapsuleCommand{
		OpCode: 0x01, CID: cid0, NSID: 1,
		D10: 0, D11: 0, D12: uint32(nlb - 1),
	}
	cid1 := uint16(cli.cid.Add(1))
	cmd1 := nvme.CapsuleCommand{
		OpCode: 0x01, CID: cid1, NSID: 1,
		D10: uint32(nlb), D11: 0, D12: uint32(nlb - 1),
	}
	if err := cli.ioW.SendHeaderOnly(0x4, &cmd0, 64); err != nil {
		t.Fatalf("send cmd0: %v", err)
	}
	if err := cli.ioW.SendHeaderOnly(0x4, &cmd1, 64); err != nil {
		t.Fatalf("send cmd1: %v", err)
	}

	_ = cli.io.SetReadDeadline(time.Now().Add(5 * time.Second))

	// --- Phase 2: opportunistic drain ---
	//
	// V2-strict spawns backend.Write in a goroutine after
	// collectR2TData returns; txLoop may flush R2T-1 before
	// CapResp-0 depending on backend timing. Kernel does the
	// same opportunistic drain — treats each PDU by its type
	// regardless of which cmd it belongs to. We mirror that.

	payloadByCID := map[uint16][]byte{cid0: payload0, cid1: payload1}
	respSeen := map[uint16]bool{}
	r2tSeen := map[uint16]bool{}

	for len(respSeen) < 2 {
		ch, err := cli.ioR.Dequeue()
		if err != nil {
			t.Fatalf("drain Dequeue: %v — server closed mid-stream (BUG-001 symptom if R2Ts were none-received yet)", err)
		}
		switch ch.Type {
		case 0x9: // R2T
			var r2t nvme.R2THeader
			if err := cli.ioR.Receive(&r2t); err != nil {
				t.Fatalf("recv R2T: %v", err)
			}
			if r2tSeen[r2t.CCCID] {
				t.Fatalf("duplicate R2T for CID=%d", r2t.CCCID)
			}
			r2tSeen[r2t.CCCID] = true

			p, ok := payloadByCID[r2t.CCCID]
			if !ok {
				t.Fatalf("R2T for unknown CID=%d", r2t.CCCID)
			}
			for i := 0; i < chunks; i++ {
				off := i * (total / chunks)
				h := nvme.H2CDataHeader{
					CCCID: r2t.CCCID, TAG: r2t.TAG,
					DATAO: uint32(off), DATAL: uint32(total / chunks),
				}
				if err := cli.ioW.SendWithData(0x6, 0, &h, 16, p[off:off+total/chunks]); err != nil {
					t.Fatalf("send H2CData CID=%d chunk %d: %v", r2t.CCCID, i, err)
				}
			}
		case 0x5: // CapsuleResp
			var resp nvme.CapsuleResponse
			if err := cli.ioR.Receive(&resp); err != nil {
				t.Fatalf("recv CapResp: %v", err)
			}
			if resp.Status != 0 {
				t.Fatalf("CapResp CID=%d status=0x%04x", resp.CID, resp.Status)
			}
			if respSeen[resp.CID] {
				t.Fatalf("duplicate CapResp for CID=%d", resp.CID)
			}
			respSeen[resp.CID] = true
		default:
			t.Fatalf("unexpected PDU type 0x%02x during drain", ch.Type)
		}
	}

	if !r2tSeen[cid0] || !r2tSeen[cid1] {
		t.Fatalf("missing R2T: cid0=%v cid1=%v", r2tSeen[cid0], r2tSeen[cid1])
	}
	if !respSeen[cid0] || !respSeen[cid1] {
		t.Fatalf("missing CapResp: cid0=%v cid1=%v", respSeen[cid0], respSeen[cid1])
	}

	// --- Integrity ---
	if got := backend.calls.Load(); got != 2 {
		t.Errorf("backend.calls=%d, want 2", got)
	}
	if got := backend.bytes.Load(); got != int64(2*total) {
		t.Errorf("backend.bytes=%d, want %d", got, 2*total)
	}
}
