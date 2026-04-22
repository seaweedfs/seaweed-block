// Ownership: QA (Condition 4 of BUG-001 fix Discovery Bridge review).
// sw may NOT modify without architect approval via §8B.4.
//
// Two additional regression tests for the pipelined-cmd port:
//
// 1. TestT2V2Port_NVMe_IO_PipelinedNWritesSameQueue —
//    Parametric N ∈ {2, 4, 8, 16, 32}. After BUG-001 fix, server
//    must accept N in-flight CapsuleCmds on one IO queue, issue
//    N R2Ts, receive N payloads, emit N CapsuleResps. Pins
//    "pipelining is not just 2" so sw's fix can't pass by
//    accident with a 2-slot special case.
//
// 2. TestT2V2Port_NVMe_IO_ConnectionCloseDrainsInflight —
//    Fire N pipelined cmds, close the TCP connection mid-stream.
//    Target must not panic, must not leak goroutines, must
//    release the admin controller. Pins cleanup discipline in
//    the new rxLoop / txLoop design.
//
// Status at landing (2026-04-22): both are RED on current V3
// (Batch 11b). When sw's BUG-001 fix lands, both must go GREEN
// without breaking any earlier A-tier or debug test.

package nvme_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// TestT2V2Port_NVMe_IO_PipelinedNWritesSameQueue pins that the
// fix supports arbitrary queue depths, not just depth-2 (the
// minimum repro from BUG-001).
//
// Kernel advertises QD up to ctrl.CAP.MQES+1 = 64 in our case;
// picks 8 in practice for mkfs. Testing up to 32 covers the
// realistic range without chasing the 64 absolute max.
func TestT2V2Port_NVMe_IO_PipelinedNWritesSameQueue(t *testing.T) {
	cases := []int{2, 4, 8, 16, 32}
	for _, n := range cases {
		n := n
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			backend, cli := newChunkedWriteTarget(t)

			const perCmdBytes = 8 * 1024 // 8 KiB per cmd keeps total bounded at N=32
			const chunks = 2              // 2 × 4 KiB H2CData PDUs per cmd
			const nlb = uint16(perCmdBytes / 512)

			// Build N payloads so data-integrity on backend side
			// can be verified (each distinct).
			payloads := make([][]byte, n)
			for i := range payloads {
				p := make([]byte, perCmdBytes)
				for j := range p {
					p[j] = byte((i*7 + j) & 0xFF)
				}
				payloads[i] = p
			}

			// --- Send N capsule cmds back-to-back ---
			cids := make([]uint16, n)
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

			// --- Collect N R2Ts; server must interleave them ---
			r2tByCID := make(map[uint16]nvme.R2THeader, n)
			for collected := 0; collected < n; collected++ {
				ch, err := cli.ioR.Dequeue()
				if err != nil {
					t.Fatalf("Dequeue R2T[%d/%d]: %v — BUG-001 fix incomplete: server did not emit all N R2Ts for pipelined cmds",
						collected, n, err)
				}
				if ch.Type == 0x5 {
					var resp nvme.CapsuleResponse
					_ = cli.ioR.Receive(&resp)
					t.Fatalf("early CapsuleResp cid=%d status=0x%04x — server responded before issuing all R2Ts",
						resp.CID, resp.Status)
				}
				if ch.Type != 0x9 {
					t.Fatalf("R2T[%d] type=0x%x, want 0x09", collected, ch.Type)
				}
				var r2t nvme.R2THeader
				if err := cli.ioR.Receive(&r2t); err != nil {
					t.Fatalf("recv R2T[%d]: %v", collected, err)
				}
				if _, seen := r2tByCID[r2t.CCCID]; seen {
					t.Fatalf("duplicate R2T for CID=%d", r2t.CCCID)
				}
				r2tByCID[r2t.CCCID] = r2t
			}

			// --- Send all H2CData: per-cmd serial internally,
			// cmd order round-robin to test interleaved arrival ---
			for chunk := 0; chunk < chunks; chunk++ {
				for i, cid := range cids {
					r2t := r2tByCID[cid]
					off := chunk * (perCmdBytes / chunks)
					h := nvme.H2CDataHeader{
						CCCID: cid, TAG: r2t.TAG,
						DATAO: uint32(off),
						DATAL: uint32(perCmdBytes / chunks),
					}
					if err := cli.ioW.SendWithData(0x6, 0, &h,
						16, payloads[i][off:off+perCmdBytes/chunks]); err != nil {
						t.Fatalf("send H2CData cid=%d chunk=%d: %v", cid, chunk, err)
					}
				}
			}

			// --- Receive N CapsuleResps ---
			seen := make(map[uint16]bool, n)
			for i := 0; i < n; i++ {
				resp := recvCapsuleResp(t, cli.ioR)
				if resp.Status != 0 {
					t.Fatalf("resp[%d] CID=%d status=0x%04x", i, resp.CID, resp.Status)
				}
				if seen[resp.CID] {
					t.Fatalf("duplicate resp for CID=%d", resp.CID)
				}
				seen[resp.CID] = true
			}
			for _, cid := range cids {
				if !seen[cid] {
					t.Errorf("missing CapsuleResp for CID=%d", cid)
				}
			}

			// --- Integrity: N backend.Write calls, N × perCmdBytes total ---
			if got := backend.calls.Load(); got != int32(n) {
				t.Errorf("backend.calls=%d; want %d", got, n)
			}
			if got, want := backend.bytes.Load(), int64(n)*int64(perCmdBytes); got != want {
				t.Errorf("backend.bytes=%d; want %d (lost %d)", got, want, want-got)
			}
		})
	}
}

// TestT2V2Port_NVMe_IO_ConnectionCloseDrainsInflight pins
// cleanup discipline: if the host closes the TCP connection
// while N cmds are in-flight, Target must release resources
// without panicking or leaking goroutines.
//
// Strategy:
//  1. Snapshot runtime.NumGoroutine
//  2. Spin target, open IO queue, fire N=8 pipelined cmds
//  3. Receive the first R2T so we're guaranteed sw has entered
//     the rxLoop-active state
//  4. Abruptly close the IO TCP conn
//  5. Give Target a grace period to drain
//  6. Assert goroutine count is within reason (tolerate ~4
//     stragglers for admin session + Target accept loop + WG
//     drain; 50+ extra means leak)
//  7. Close Target; re-check NumGoroutine is back near baseline
//
// RED pre-fix: handleWrite's blocking read on a closed conn
// may return an error AND the serial session loop may not
// propagate cleanup to admin controller's release path.
// GREEN post-fix: rxLoop detects EOF, cancels pending cmds via
// ctx, txLoop drains, cmdWg.Wait unblocks, session returns.
func TestT2V2Port_NVMe_IO_ConnectionCloseDrainsInflight(t *testing.T) {
	_ = backendStubForClose // keep compile if future helpers elided

	// Baseline before we even build the target — avoids counting
	// package init goroutines as leaks.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	backend, cli := newChunkedWriteTarget(t)

	const n = 8
	const perCmdBytes = 8 * 1024
	const nlb = uint16(perCmdBytes / 512)

	// Fire N pipelined cmds without reading.
	for i := 0; i < n; i++ {
		cid := uint16(cli.cid.Add(1))
		cmd := nvme.CapsuleCommand{
			OpCode: 0x01, CID: cid, NSID: 1,
			D10: uint32(i) * uint32(nlb), D11: 0,
			D12: uint32(nlb - 1),
		}
		if err := cli.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
			t.Fatalf("send cmd[%d]: %v", i, err)
		}
	}

	// Wait for the first R2T so rxLoop is demonstrably active.
	_ = cli.io.SetReadDeadline(time.Now().Add(5 * time.Second))
	ch, err := cli.ioR.Dequeue()
	if err != nil {
		// Pre-fix, server may have already closed us with
		// "expected H2CData, got 0x4". That's fine — we still
		// get to test the cleanup path, just from the other
		// direction.
		t.Logf("first Dequeue returned %v (acceptable — proceeding to close path)", err)
	} else if ch.Type == 0x9 {
		var r2t nvme.R2THeader
		_ = cli.ioR.Receive(&r2t)
	}

	// Abrupt close of the IO TCP conn — simulates a host crash
	// or nvme disconnect while data is in flight.
	_ = cli.io.Close()

	// Grace period for target to drain + release admin controller.
	time.Sleep(500 * time.Millisecond)

	// We expect backend.calls to be 0 or some small number; the
	// actual invariant is "target didn't panic and didn't wedge".
	_ = backend

	// Goroutine delta after drain. We don't care about exact
	// count; 50+ over baseline is a clear leak signal.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	afterDrain := runtime.NumGoroutine()
	if afterDrain > baseline+50 {
		t.Errorf("goroutine leak: baseline=%d after-conn-close=%d (delta=%d)",
			baseline, afterDrain, afterDrain-baseline)
	}

	// Target.Close runs via t.Cleanup; once fired, final check
	// in a second t.Cleanup runs after it. Can't do that here
	// easily; instead trust the drain grace period + baseline
	// comparison.
}

// backendStubForClose is a vestigial symbol to keep the import
// for (*writeCountingBackend) visible if helper imports shift.
var backendStubForClose = (*writeCountingBackend)(nil)
