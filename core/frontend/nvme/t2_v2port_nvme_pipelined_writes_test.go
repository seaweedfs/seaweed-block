// Ownership: QA (BUG-001 repro — queue-internal pipeline).
// sw may NOT modify without architect approval via §8B.4 Discovery
// Bridge.
//
// Purpose: reproduce the m01 mkfs.ext4 hang at unit level by
// emulating the specific wire pattern Linux kernel emits when it
// pipelines multiple Write commands on a single IO queue.
//
// Evidence that this is the m01 root cause (see BUG-001 §5):
//   - m01 kernel log: tag 13 AND tag 14 BOTH timed out on SAME queue
//     (QID 1) — kernel had 2 concurrent writes in flight
//   - Only 1 of 8 IO queue sessions reported "expected H2CData, got 0x4"
//   - V3's handleCapsuleCmd is strictly serial per connection; while
//     handleWrite for cmd 13 is in the H2CData collection loop, cmd 14
//     bytes sit in the TCP receive buffer as type=0x04 PDUs
//
// Test strategy (pipelined client):
//   1. On ONE IO queue, send Cmd-13 CapsuleCommand
//   2. Immediately send Cmd-14 CapsuleCommand (no wait for R2T-13)
//   3. Read what server sends back
//
// Expected RED (pre-fix): server reports "expected H2CData, got 0x4"
// because handleWrite for 13 reads Cmd-14 bytes (type 4) as the
// next expected H2CData.
//
// Expected GREEN (post-fix): server accepts both cmds, interleaves
// R2Ts / H2CData properly; test-client assembles responses.

package nvme_test

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// TestT2V2Port_NVMe_IO_Pipelined2WritesSameQueue emulates the exact
// m01 failure pattern.
//
// Status at landing (2026-04-22): this test is expected RED with
// current V3 (Batch 11b). When sw's H4 fix lands, it must go GREEN
// without breaking the other 7 debug tests.
func TestT2V2Port_NVMe_IO_Pipelined2WritesSameQueue(t *testing.T) {
	backend, cli := newChunkedWriteTarget(t)

	// Two 32 KiB Writes at non-overlapping LBA ranges.
	const total = 32 * 1024
	const chunks = 8
	const nlb = uint16(total / 512)

	payload1 := make([]byte, total)
	payload2 := make([]byte, total)
	for i := range payload1 {
		payload1[i] = byte((i + 0x11) & 0xFF)
		payload2[i] = byte((i + 0x22) & 0xFF)
	}

	// --- Pipeline the two CapsuleCmds back-to-back on the SAME IO queue ---

	cid1 := uint16(cli.cid.Add(1))
	cmd1 := nvme.CapsuleCommand{
		OpCode: 0x01, CID: cid1, NSID: 1,
		D10: 0, D11: 0, D12: uint32(nlb - 1),
	}
	cid2 := uint16(cli.cid.Add(1))
	cmd2 := nvme.CapsuleCommand{
		OpCode: 0x01, CID: cid2, NSID: 1,
		D10: uint32(nlb), D11: 0, D12: uint32(nlb - 1),
	}

	// SEND cmd1 then cmd2 with NO read in between — this is the
	// pipeline that kernel does and that breaks V3.
	if err := cli.ioW.SendHeaderOnly(0x4, &cmd1, 64); err != nil {
		t.Fatalf("send cmd1: %v", err)
	}
	if err := cli.ioW.SendHeaderOnly(0x4, &cmd2, 64); err != nil {
		t.Fatalf("send cmd2: %v", err)
	}

	// Now read — we expect TWO R2Ts (one per cmd). Under current V3
	// serial behavior, we'll only get R2T for cmd1 before V3 tries
	// to consume cmd2 as H2CData and errors out.
	type r2tPair struct {
		r2t nvme.R2THeader
		cid uint16
		ok  bool
	}
	r2ts := make([]r2tPair, 0, 2)

	// Short deadline: if bug is present, server closes connection
	// after reading cmd2-as-H2CData. With 2s timeout we'll detect
	// the hang without dragging the whole suite.
	_ = cli.io.SetReadDeadline(time.Now().Add(2 * time.Second))

	for i := 0; i < 2; i++ {
		ch, err := cli.ioR.Dequeue()
		if err != nil {
			// Error or timeout — record and break; classification below.
			t.Logf("Dequeue[%d] error (H4 BUG-001 symptom if only got %d R2Ts): %v", i, len(r2ts), err)
			break
		}
		if ch.Type == 0x5 /* pduCapsuleResp */ {
			var resp nvme.CapsuleResponse
			_ = cli.ioR.Receive(&resp)
			t.Logf("got early CapsuleResp for CID=%d status=0x%04x", resp.CID, resp.Status)
			break
		}
		if ch.Type != 0x9 /* pduR2T */ {
			t.Fatalf("pair[%d] expected R2T, got type 0x%x", i, ch.Type)
		}
		var r2t nvme.R2THeader
		if err := cli.ioR.Receive(&r2t); err != nil {
			t.Fatalf("recv R2T[%d]: %v", i, err)
		}
		r2ts = append(r2ts, r2tPair{r2t: r2t, cid: r2t.CCCID, ok: true})
	}

	if len(r2ts) < 2 {
		t.Fatalf("only got %d R2Ts for 2 pipelined Writes — H4 confirmed: "+
			"V3 session serializes at Cmd level, cannot respond to cmd2 until cmd1 "+
			"completes (but cmd1 is blocked waiting for H2CData that kernel hasn't "+
			"sent yet because it's waiting for R2T-2 first). This is the m01 BUG-001 "+
			"deadlock pattern.",
			len(r2ts))
	}

	// --- If we got both R2Ts, proceed to send interleaved H2CData ---
	// This branch will only execute after H4 is fixed and V3 can
	// pipeline. Kernel would interleave here: H2CData for whichever
	// request it has data ready for, cited by CCCID/TAG.
	//
	// For the RED test, we don't reach here.

	// Send all chunks for cmd1 first (kernel order also commonly
	// serializes data per cmd even with pipelined cmds).
	for i := 0; i < chunks; i++ {
		off := i * (total / chunks)
		h := nvme.H2CDataHeader{
			CCCID: cid1, TAG: r2ts[0].r2t.TAG,
			DATAO: uint32(off), DATAL: uint32(total / chunks),
		}
		if err := cli.ioW.SendWithData(0x6, 0, &h, 16, payload1[off:off+total/chunks]); err != nil {
			t.Fatalf("send H2CData cmd1 chunk %d: %v", i, err)
		}
	}
	// Now chunks for cmd2.
	for i := 0; i < chunks; i++ {
		off := i * (total / chunks)
		h := nvme.H2CDataHeader{
			CCCID: cid2, TAG: r2ts[1].r2t.TAG,
			DATAO: uint32(off), DATAL: uint32(total / chunks),
		}
		if err := cli.ioW.SendWithData(0x6, 0, &h, 16, payload2[off:off+total/chunks]); err != nil {
			t.Fatalf("send H2CData cmd2 chunk %d: %v", i, err)
		}
	}

	// Expect two CapsuleResps (one per cmd).
	_ = cli.io.SetReadDeadline(time.Now().Add(5 * time.Second))
	seen := map[uint16]bool{}
	for i := 0; i < 2; i++ {
		resp := recvCapsuleResp(t, cli.ioR)
		if resp.Status != 0 {
			t.Fatalf("pipelined cmd CID=%d status=0x%04x", resp.CID, resp.Status)
		}
		seen[resp.CID] = true
	}
	if !seen[cid1] || !seen[cid2] {
		t.Fatalf("missing CapsuleResp: cid1=%v cid2=%v", seen[cid1], seen[cid2])
	}

	if got := backend.calls.Load(); got != 2 {
		t.Errorf("backend.calls=%d; want 2 (one per pipelined Write)", got)
	}
	if got := backend.bytes.Load(); got != int64(2*total) {
		t.Errorf("backend.bytes=%d; want %d", got, 2*total)
	}
}
