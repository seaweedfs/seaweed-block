// Ownership: QA (L1B-4 inventory item, landed early per QA parallel
// capacity during Batch 11c sign prep).
// sw may NOT modify without architect approval via §8B.4 Discovery
// Bridge.
//
// Maps to ledger row: PCDD-NVME-SESSION-DISCONNECT-MID-R2T-001
//
// Purpose: pin server cleanup correctness when the host crashes
// or nvme-disconnects MID-H2CData-stream (some chunks in, some
// still pending). Distinct from ConnectionCloseDrainsInflight,
// which closes BEFORE any H2CData is sent — the two tests
// exercise different server error branches:
//
//   ConnectionCloseDrainsInflight → recvH2CData blocked at first
//     PDU read; socket closes → Dequeue returns EOF on the first
//     iteration.
//
//   this test → recvH2CData has already consumed N chunks and is
//     reading the (N+1)th; socket closes mid-loop → Dequeue
//     returns EOF during the loop. Exercises the "partial data
//     buffered" cleanup path.
//
// Test layer: Unit (in-process Target + nvmeClient; no subprocess).

package nvme_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

func TestT2V2Port_NVMe_IO_DisconnectMidH2CDataStream(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	backend, cli := newChunkedWriteTarget(t)

	// 32 KiB write, 8 chunks. We'll send 3 chunks then abruptly
	// close the conn — server's recvH2CData is inside its loop
	// at that moment, expecting 5 more chunks.
	const total = 32 * 1024
	const chunks = 8
	const nlb = uint16(total / 512)
	const sendChunks = 3

	payload := make([]byte, total)
	for i := range payload {
		payload[i] = byte((i * 17) & 0xFF)
	}

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

	// Send only `sendChunks` of `chunks` total. After this, server
	// has received partial data and is blocked in recvH2CData
	// awaiting the next H2CData PDU.
	for i := 0; i < sendChunks; i++ {
		off := i * (total / chunks)
		h := nvme.H2CDataHeader{
			CCCID: cid, TAG: r2t.TAG,
			DATAO: uint32(off),
			DATAL: uint32(total / chunks),
		}
		if err := cli.ioW.SendWithData(0x6, 0, &h, 16, payload[off:off+total/chunks]); err != nil {
			t.Fatalf("send H2CData chunk %d: %v", i, err)
		}
	}

	// Abrupt close — server recvH2CData's Dequeue will surface EOF
	// mid-loop. rxLoop must unwind cleanly, releasing any partial
	// buffer without panic or leak.
	_ = cli.io.Close()

	// Grace period for target drain.
	time.Sleep(500 * time.Millisecond)

	// Backend must NOT have been called — partial data never
	// completed to a full Write.
	if got := backend.calls.Load(); got != 0 {
		t.Errorf("backend.calls=%d; want 0 (partial data must not reach backend)", got)
	}
	if got := backend.bytes.Load(); got != 0 {
		t.Errorf("backend.bytes=%d; want 0 (partial data must not be written)", got)
	}

	// Goroutine leak check.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	afterClose := runtime.NumGoroutine()
	if afterClose > baseline+50 {
		t.Errorf("goroutine leak: baseline=%d after-mid-stream-close=%d (delta=%d)",
			baseline, afterClose, afterClose-baseline)
	}
}
