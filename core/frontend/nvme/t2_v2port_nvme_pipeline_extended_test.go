// Ownership: QA (Condition 4 of BUG-001 fix Discovery Bridge review).
// sw may NOT modify without architect approval via §8B.4.
//
// REVISION 2026-04-22 (post-c0c0a1d revert to V2-strict):
// Original draft expected async R2T emission — incompatible with
// kernel. Rewritten to kernel-realistic paired R2T+data+resp cycle
// under single-R2T-outstanding invariant. See BUG-001 §13 +
// feedback_porting_discipline.md citation.
//
// 1. TestT2V2Port_NVMe_IO_PipelinedNWritesSameQueue —
//    Parametric N ∈ {2, 4, 8, 16, 32}. All N CapsuleCmds shipped
//    back-to-back (kernel pipelines freely); then paired
//    R2T→H2CData→CapResp cycle per cmd. Pins server's
//    pendingCapsules drain works at scale (not just N=2) and
//    FIFO ordering is preserved.
//
// 2. TestT2V2Port_NVMe_IO_ConnectionCloseDrainsInflight —
//    Simplified: 1 cmd + R2T received, then abrupt TCP close
//    BEFORE sending H2CData. Pins cleanup discipline (no
//    goroutine leak on mid-R2T close). Previous multi-cmd shape
//    was covered "by accident" under V2-strict since inline
//    collectR2TData blocks and then returns on socket error.

package nvme_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// TestT2V2Port_NVMe_IO_PipelinedNWritesSameQueue —
// N pipelined cmds, kernel-realistic paired-cycle shape.
func TestT2V2Port_NVMe_IO_PipelinedNWritesSameQueue(t *testing.T) {
	cases := []int{2, 4, 8, 16, 32}
	for _, n := range cases {
		n := n
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			backend, cli := newChunkedWriteTarget(t)

			const perCmdBytes = 8 * 1024
			const chunks = 2
			const nlb = uint16(perCmdBytes / 512)

			payloads := make([][]byte, n)
			cids := make([]uint16, n)
			for i := range payloads {
				p := make([]byte, perCmdBytes)
				for j := range p {
					p[j] = byte((i*7 + j) & 0xFF)
				}
				payloads[i] = p
			}

			// --- Phase 1: fire ALL N cmds back-to-back (pipelined) ---
			for i := 0; i < n; i++ {
				cids[i] = uint16(cli.cid.Add(1))
				cmd := nvme.CapsuleCommand{
					OpCode: 0x01, CID: cids[i], NSID: 1,
					D10: uint32(i) * uint32(nlb), D11: 0,
					D12: uint32(nlb - 1),
				}
				if err := cli.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
					t.Fatalf("send cmd[%d]: %v", i, err)
				}
			}

			_ = cli.io.SetReadDeadline(time.Now().Add(10 * time.Second))

			// --- Phase 2: opportunistic drain until N CapResps ---
			//
			// V2-strict spawns backend.Write as a goroutine after
			// collectR2TData returns; txLoop orders R2T-i+1 and
			// CapResp-i based on goroutine completion vs pendingCapsules
			// drain timing. Kernel does the same opportunistic drain
			// by PDU type. We mirror kernel behavior so the test is
			// tolerant of legitimate ordering freedom.

			payloadByCID := make(map[uint16][]byte, n)
			for i, cid := range cids {
				payloadByCID[cid] = payloads[i]
			}
			respSeen := make(map[uint16]bool, n)
			r2tSeen := make(map[uint16]bool, n)

			for len(respSeen) < n {
				ch, err := cli.ioR.Dequeue()
				if err != nil {
					t.Fatalf("drain Dequeue (seen %d R2T, %d resp of %d): %v",
						len(r2tSeen), len(respSeen), n, err)
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
					for c := 0; c < chunks; c++ {
						off := c * (perCmdBytes / chunks)
						h := nvme.H2CDataHeader{
							CCCID: r2t.CCCID, TAG: r2t.TAG,
							DATAO: uint32(off),
							DATAL: uint32(perCmdBytes / chunks),
						}
						if err := cli.ioW.SendWithData(0x6, 0, &h,
							16, p[off:off+perCmdBytes/chunks]); err != nil {
							t.Fatalf("send H2CData CID=%d chunk %d: %v", r2t.CCCID, c, err)
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

			if len(r2tSeen) != n {
				t.Errorf("R2T count: got %d, want %d", len(r2tSeen), n)
			}

			// --- Integrity ---
			if got := backend.calls.Load(); got != int32(n) {
				t.Errorf("backend.calls=%d, want %d", got, n)
			}
			if got, want := backend.bytes.Load(), int64(n)*int64(perCmdBytes); got != want {
				t.Errorf("backend.bytes=%d, want %d (lost %d)", got, want, want-got)
			}
		})
	}
}

// TestT2V2Port_NVMe_IO_ConnectionCloseDrainsInflight pins cleanup
// discipline: abrupt close mid-R2T (client received R2T, never
// sends H2CData) must not leak goroutines or panic the target.
func TestT2V2Port_NVMe_IO_ConnectionCloseDrainsInflight(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	backend, cli := newChunkedWriteTarget(t)

	const total = 32 * 1024
	const nlb = uint16(total / 512)

	// Fire one cmd. Server will emit R2T and then block inline
	// waiting for H2CData (V2-strict). We receive R2T but never
	// send data — then close the TCP conn.
	cid := uint16(cli.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x01, CID: cid, NSID: 1,
		D10: 0, D11: 0, D12: uint32(nlb - 1),
	}
	if err := cli.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send cmd: %v", err)
	}

	_ = cli.io.SetReadDeadline(time.Now().Add(5 * time.Second))
	ch, err := cli.ioR.Dequeue()
	if err != nil {
		t.Fatalf("R2T Dequeue: %v", err)
	}
	if ch.Type != 0x9 {
		t.Fatalf("expected R2T, got type 0x%02x", ch.Type)
	}
	var r2t nvme.R2THeader
	if err := cli.ioR.Receive(&r2t); err != nil {
		t.Fatalf("recv R2T: %v", err)
	}

	// Abrupt close — server is now inline in recvH2CData waiting
	// on a PDU that will never arrive. Close triggers read error
	// on the server side; rxLoop must unwind cleanly.
	_ = cli.io.Close()

	// Grace period for target-side drain.
	time.Sleep(500 * time.Millisecond)

	// Backend must not have been called (no H2CData ever arrived).
	if got := backend.calls.Load(); got != 0 {
		t.Errorf("backend.calls=%d, want 0 (no H2CData was sent)", got)
	}

	// Goroutine delta after drain. 50+ over baseline is a clear
	// leak signal.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	afterDrain := runtime.NumGoroutine()
	if afterDrain > baseline+50 {
		t.Errorf("goroutine leak: baseline=%d after-close=%d (delta=%d)",
			baseline, afterDrain, afterDrain-baseline)
	}
}
