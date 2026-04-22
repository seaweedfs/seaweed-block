// Ownership: sw test-support — minimal Go NVMe/TCP initiator
// for L1 in-process route tests. Mirrors the iSCSI test client
// in shape: dial, ICReq, fabric Connect, IO Read/Write, close.
// Not a general-purpose initiator — only what L1 needs.
package nvme_test

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// nvmeClient drives one NVMe/TCP connection against a target.
type nvmeClient struct {
	conn net.Conn
	r    *nvme.Reader
	w    *nvme.Writer
	cid  atomic.Uint32 // command identifier counter
}

func dialAndConnect(t *testing.T, addr string) *nvmeClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	c := &nvmeClient{
		conn: conn,
		r:    nvme.NewReader(conn),
		w:    nvme.NewWriter(conn),
	}

	// Phase 1: ICReq → ICResp.
	icr := nvme.ICRequest{
		PDUFormatVersion: 0x0000, // NVMe-TCP 1.0a
		PDUDataAlignment: 0,
		PDUDataDigest:    0,
		PDUMaxR2T:        1,
	}
	if err := c.w.SendHeaderOnly(0x0 /* pduICReq */, &icr, 120 /* icBodySize */); err != nil {
		t.Fatalf("send ICReq: %v", err)
	}
	ch, err := c.r.Dequeue()
	if err != nil {
		t.Fatalf("read ICResp: %v", err)
	}
	if ch.Type != 0x1 /* pduICResp */ {
		t.Fatalf("expected ICResp, got 0x%x", ch.Type)
	}
	var icrep nvme.ICResponse
	if err := c.r.Receive(&icrep); err != nil {
		t.Fatalf("read ICResp body: %v", err)
	}

	// Phase 2: Fabric Connect.
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x7F, // adminFabric
		FCType: 0x01, // fcConnect
		CID:    cid,
		// D10/D11 carry RECFMT and SQSIZE in real drivers; for
		// our minimal Connect they're ignored.
	}
	cd := nvme.ConnectData{
		HostID:  [16]byte{0x01, 0x02, 0x03, 0x04},
		CNTLID:  0xFFFF, // request new controller
		SubNQN:  "nqn.2026-04.example.v3:subsys",
		HostNQN: "nqn.2026-04.example.host:1",
	}
	cdBuf := make([]byte, 1024 /* connectDataSize */)
	cd.Marshal(cdBuf)
	if err := c.w.SendWithData(0x4 /* pduCapsuleCmd */, 0, &cmd, 64 /* capsuleCmdSize */, cdBuf); err != nil {
		t.Fatalf("send Connect: %v", err)
	}
	resp := c.recvCapsuleResp(t)
	if resp.CID != cid {
		t.Fatalf("Connect resp CID=%d want %d", resp.CID, cid)
	}
	if resp.Status != 0 {
		t.Fatalf("Connect status=0x%04x (non-success)", resp.Status)
	}
	return c
}

// readCmd issues an IO Read CapsuleCmd and returns (status, data).
// expectedBytes drives the C2HData receive.
func (c *nvmeClient) readCmd(t *testing.T, slba uint64, nlb uint16, expectedBytes int) (uint16, []byte) {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x02, // ioRead
		CID:    cid,
		NSID:   1,
		D10:    uint32(slba & 0xFFFFFFFF),
		D11:    uint32(slba >> 32),
		D12:    uint32(nlb - 1), // NLB is zero-based on wire
	}
	if err := c.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send Read: %v", err)
	}

	// Expect a C2HData (with data) followed by a CapsuleResp.
	// Server sends C2HData first.
	ch, err := c.r.Dequeue()
	if err != nil {
		t.Fatalf("read C2HData hdr: %v", err)
	}
	if ch.Type != 0x7 /* pduC2HData */ {
		// Could be an error CapsuleResp without data — handle that.
		if ch.Type == 0x5 /* pduCapsuleResp */ {
			var resp nvme.CapsuleResponse
			_ = c.r.Receive(&resp)
			return resp.Status, nil
		}
		t.Fatalf("expected C2HData or CapsuleResp, got 0x%x", ch.Type)
	}
	var dh nvme.C2HDataHeader
	if err := c.r.Receive(&dh); err != nil {
		t.Fatalf("recv C2HData: %v", err)
	}
	data := make([]byte, c.r.Length())
	if err := c.r.ReceiveData(data); err != nil {
		t.Fatalf("read data: %v", err)
	}
	resp := c.recvCapsuleResp(t)
	if resp.CID != cid {
		t.Fatalf("Read resp CID=%d want %d", resp.CID, cid)
	}
	_ = expectedBytes
	return resp.Status, data
}

// writeCmd issues an IO Write CapsuleCmd, waits for R2T, sends
// the H2CData payload, then reads the CapsuleResp status.
func (c *nvmeClient) writeCmd(t *testing.T, slba uint64, nlb uint16, payload []byte) uint16 {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x01, // ioWrite
		CID:    cid,
		NSID:   1,
		D10:    uint32(slba & 0xFFFFFFFF),
		D11:    uint32(slba >> 32),
		D12:    uint32(nlb - 1),
	}
	if err := c.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send Write: %v", err)
	}

	// Expect R2T from server.
	ch, err := c.r.Dequeue()
	if err != nil {
		t.Fatalf("read R2T hdr: %v", err)
	}
	if ch.Type == 0x5 /* CapsuleResp */ {
		// Server short-circuited (e.g. validation error) — return
		// the error status without sending data.
		var resp nvme.CapsuleResponse
		_ = c.r.Receive(&resp)
		return resp.Status
	}
	if ch.Type != 0x9 /* pduR2T */ {
		t.Fatalf("expected R2T, got 0x%x", ch.Type)
	}
	var r2t nvme.R2THeader
	if err := c.r.Receive(&r2t); err != nil {
		t.Fatalf("recv R2T: %v", err)
	}

	// Send H2CData carrying the full payload in one PDU.
	h2c := nvme.H2CDataHeader{
		CCCID: cid,
		TAG:   r2t.TAG,
		DATAO: 0,
		DATAL: uint32(len(payload)),
	}
	if err := c.w.SendWithData(0x6 /* pduH2CData */, 0, &h2c, 16 /* h2cDataHdrSize */, payload); err != nil {
		t.Fatalf("send H2CData: %v", err)
	}

	resp := c.recvCapsuleResp(t)
	if resp.CID != cid {
		t.Fatalf("Write resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status
}

func (c *nvmeClient) recvCapsuleResp(t *testing.T) nvme.CapsuleResponse {
	t.Helper()
	ch, err := c.r.Dequeue()
	if err != nil {
		t.Fatalf("read CapsuleResp hdr: %v", err)
	}
	if ch.Type != 0x5 /* pduCapsuleResp */ {
		t.Fatalf("expected CapsuleResp, got 0x%x", ch.Type)
	}
	var resp nvme.CapsuleResponse
	if err := c.r.Receive(&resp); err != nil {
		t.Fatalf("recv CapsuleResp: %v", err)
	}
	return resp
}

func (c *nvmeClient) close() { _ = c.conn.Close() }

// expectStatusSuccess fails the test if the wire status word is non-zero.
func expectStatusSuccess(t *testing.T, status uint16, op string) {
	t.Helper()
	if status != 0 {
		t.Fatalf("%s: status=0x%04x want 0 (Success)", op, status)
	}
}

// expectStatusANATransition fails the test if status is not the
// stale-lineage tuple (SCT=3, SC=3 → wire = 0x0606 with DNR=1
// adds 0x8000).
func expectStatusANATransition(t *testing.T, status uint16, op string) {
	t.Helper()
	// SCT=3 SC=3 with DNR=1 = 0x8606; tolerate DNR=0 too (0x0606).
	const sctMask = 0x0E00
	const scMask = 0x01FE
	gotSCT := (status & sctMask) >> 9
	gotSC := (status & scMask) >> 1
	if gotSCT != 3 || gotSC != 3 {
		t.Fatalf("%s: status=0x%04x SCT=%d SC=0x%02x want SCT=3 SC=0x03 (ANA Transition)",
			op, status, gotSCT, gotSC)
	}
}
