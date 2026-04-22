// Ownership: sw test-support — minimal Go NVMe/TCP initiator
// for L1 in-process route tests. Mirrors the iSCSI test client
// in shape: dial, ICReq, fabric Connect, IO Read/Write, close.
// Not a general-purpose initiator — only what L1 needs.
//
// Batch 11a rewire: NVMe-oF queue model is now enforced. An
// nvmeClient opens TWO TCP connections:
//   - adminConn (QID=0): admin Connect allocates CNTLID,
//     admin opcodes (Identify in 11a; Property/Features/Keep
//     Alive/AER in 11b) go here.
//   - ioConn (QID>0): IO Connect cites the CNTLID; Read/
//     Write/Flush go here.
// Tests that only need IO round-trip see the existing API
// (writeCmd/readCmd dispatch through ioConn); tests that
// exercise admin opcodes use adminCmd.
package nvme_test

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// nvmeClient drives one NVMe/TCP session pair (admin + IO)
// against a target.
type nvmeClient struct {
	t *testing.T

	admin     net.Conn
	adminR    *nvme.Reader
	adminW    *nvme.Writer

	io     net.Conn
	ioR    *nvme.Reader
	ioW    *nvme.Writer

	cid    atomic.Uint32 // command identifier counter (shared across queues)
	cntlID uint16        // assigned by admin Connect; echoed by IO Connect

	// Back-compat shims: old tests use cli.conn / cli.r / cli.w
	// for IO commands. Point these at the IO queue so existing
	// scsiCmdFull-style helpers keep working.
	conn net.Conn
	r    *nvme.Reader
	w    *nvme.Writer
}

// close tears down both TCP connections. Idempotent.
func (c *nvmeClient) close() {
	if c.io != nil {
		_ = c.io.Close()
	}
	if c.admin != nil {
		_ = c.admin.Close()
	}
}

// dialAndConnect opens the admin + IO queue pair using the
// canonical test identity (SubNQN = "nqn.2026-04.example.v3:subsys",
// HostNQN = "nqn.2026-04.example.host:1"). Returns a client
// with both connections established and IO opcodes routed
// through ioConn.
func dialAndConnect(t *testing.T, addr string) *nvmeClient {
	t.Helper()
	return dialAndConnectOpts(t, addr, connectOptions{})
}

// connectOptions tunes Connect parameters. Zero-values produce
// the canonical test identity.
type connectOptions struct {
	SubNQN  string
	HostNQN string
	// SkipIOQueue bypasses the IO queue Connect. Used by tests
	// that want admin-only (e.g., Identify-only).
	SkipIOQueue bool
}

func dialAndConnectOpts(t *testing.T, addr string, opts connectOptions) *nvmeClient {
	t.Helper()
	if opts.SubNQN == "" {
		opts.SubNQN = "nqn.2026-04.example.v3:subsys"
	}
	if opts.HostNQN == "" {
		opts.HostNQN = "nqn.2026-04.example.host:1"
	}

	c := &nvmeClient{t: t}

	// Admin queue: Connect with QID=0, CNTLID=0xFFFF.
	c.admin = dialAndHandshake(t, addr)
	c.adminR = nvme.NewReader(c.admin)
	c.adminW = nvme.NewWriter(c.admin)

	cid := uint16(c.cid.Add(1))
	adminCmd := nvme.CapsuleCommand{
		OpCode: 0x7F, // adminFabric
		FCType: 0x01, // fcConnect
		CID:    cid,
		D10:    uint32(0) << 16, // QID=0 (admin)
	}
	adminCD := nvme.ConnectData{
		HostID:  [16]byte{0x01, 0x02, 0x03, 0x04},
		CNTLID:  0xFFFF,
		SubNQN:  opts.SubNQN,
		HostNQN: opts.HostNQN,
	}
	cdBuf := make([]byte, 1024)
	adminCD.Marshal(cdBuf)
	if err := c.adminW.SendWithData(0x4, 0, &adminCmd, 64, cdBuf); err != nil {
		t.Fatalf("admin Connect send: %v", err)
	}
	adminResp := recvCapsuleResp(t, c.adminR)
	if adminResp.CID != cid {
		t.Fatalf("admin Connect resp CID=%d want %d", adminResp.CID, cid)
	}
	if adminResp.Status != 0 {
		t.Fatalf("admin Connect status=0x%04x (non-success)", adminResp.Status)
	}
	// CapsuleResp DW0[15:0] = assigned CNTLID.
	c.cntlID = uint16(adminResp.DW0 & 0xFFFF)
	if c.cntlID == 0 {
		t.Fatalf("admin Connect did not assign CNTLID (DW0=0x%08x)", adminResp.DW0)
	}

	if opts.SkipIOQueue {
		return c
	}

	// IO queue: Connect with QID=1, CNTLID = what admin gave us.
	c.io = dialAndHandshake(t, addr)
	c.ioR = nvme.NewReader(c.io)
	c.ioW = nvme.NewWriter(c.io)

	ioCID := uint16(c.cid.Add(1))
	ioCmd := nvme.CapsuleCommand{
		OpCode: 0x7F,
		FCType: 0x01,
		CID:    ioCID,
		D10:    uint32(1) << 16, // QID=1 (first IO queue)
	}
	ioCD := nvme.ConnectData{
		HostID:  adminCD.HostID,
		CNTLID:  c.cntlID, // echo admin's assignment
		SubNQN:  opts.SubNQN,
		HostNQN: opts.HostNQN,
	}
	ioCDBuf := make([]byte, 1024)
	ioCD.Marshal(ioCDBuf)
	if err := c.ioW.SendWithData(0x4, 0, &ioCmd, 64, ioCDBuf); err != nil {
		t.Fatalf("IO Connect send: %v", err)
	}
	ioResp := recvCapsuleResp(t, c.ioR)
	if ioResp.CID != ioCID {
		t.Fatalf("IO Connect resp CID=%d want %d", ioResp.CID, ioCID)
	}
	if ioResp.Status != 0 {
		t.Fatalf("IO Connect status=0x%04x (non-success)", ioResp.Status)
	}

	// Back-compat: existing tests use c.conn / c.r / c.w for IO.
	c.conn = c.io
	c.r = c.ioR
	c.w = c.ioW

	return c
}

// dialAndHandshake does TCP connect + ICReq/ICResp. Returns the
// conn ready for Connect.
func dialAndHandshake(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	w := nvme.NewWriter(conn)
	r := nvme.NewReader(conn)

	icr := nvme.ICRequest{
		PDUFormatVersion: 0x0000,
		PDUMaxR2T:        1,
	}
	if err := w.SendHeaderOnly(0x0, &icr, 120); err != nil {
		t.Fatalf("send ICReq: %v", err)
	}
	ch, err := r.Dequeue()
	if err != nil {
		t.Fatalf("read ICResp: %v", err)
	}
	if ch.Type != 0x1 {
		t.Fatalf("expected ICResp, got 0x%x", ch.Type)
	}
	var icrep nvme.ICResponse
	if err := r.Receive(&icrep); err != nil {
		t.Fatalf("read ICResp body: %v", err)
	}
	return conn
}

// recvCapsuleResp is a standalone reader (not a method on
// nvmeClient) so it works for either admin or IO connection.
func recvCapsuleResp(t *testing.T, r *nvme.Reader) nvme.CapsuleResponse {
	t.Helper()
	ch, err := r.Dequeue()
	if err != nil {
		t.Fatalf("read CapsuleResp hdr: %v", err)
	}
	if ch.Type != 0x5 {
		t.Fatalf("expected CapsuleResp, got 0x%x", ch.Type)
	}
	var resp nvme.CapsuleResponse
	if err := r.Receive(&resp); err != nil {
		t.Fatalf("recv CapsuleResp: %v", err)
	}
	return resp
}

// ---------- IO command helpers (routed through ioConn) ----------

// readCmd issues an IO Read CapsuleCmd on the IO queue.
func (c *nvmeClient) readCmd(t *testing.T, slba uint64, nlb uint16, expectedBytes int) (uint16, []byte) {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x02, // ioRead
		CID:    cid,
		NSID:   1,
		D10:    uint32(slba & 0xFFFFFFFF),
		D11:    uint32(slba >> 32),
		D12:    uint32(nlb - 1),
	}
	if err := c.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send Read: %v", err)
	}

	ch, err := c.ioR.Dequeue()
	if err != nil {
		t.Fatalf("read C2HData hdr: %v", err)
	}
	if ch.Type != 0x7 /* pduC2HData */ {
		if ch.Type == 0x5 {
			var resp nvme.CapsuleResponse
			_ = c.ioR.Receive(&resp)
			return resp.Status, nil
		}
		t.Fatalf("expected C2HData or CapsuleResp, got 0x%x", ch.Type)
	}
	var dh nvme.C2HDataHeader
	if err := c.ioR.Receive(&dh); err != nil {
		t.Fatalf("recv C2HData: %v", err)
	}
	data := make([]byte, c.ioR.Length())
	if err := c.ioR.ReceiveData(data); err != nil {
		t.Fatalf("read data: %v", err)
	}
	resp := recvCapsuleResp(t, c.ioR)
	if resp.CID != cid {
		t.Fatalf("Read resp CID=%d want %d", resp.CID, cid)
	}
	_ = expectedBytes
	return resp.Status, data
}

// writeCmd issues an IO Write on the IO queue; handles R2T +
// H2CData + CapsuleResp round trip.
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
	if err := c.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send Write: %v", err)
	}

	ch, err := c.ioR.Dequeue()
	if err != nil {
		t.Fatalf("read R2T hdr: %v", err)
	}
	if ch.Type == 0x5 {
		var resp nvme.CapsuleResponse
		_ = c.ioR.Receive(&resp)
		return resp.Status
	}
	if ch.Type != 0x9 {
		t.Fatalf("expected R2T, got 0x%x", ch.Type)
	}
	var r2t nvme.R2THeader
	if err := c.ioR.Receive(&r2t); err != nil {
		t.Fatalf("recv R2T: %v", err)
	}

	h2c := nvme.H2CDataHeader{
		CCCID: cid,
		TAG:   r2t.TAG,
		DATAO: 0,
		DATAL: uint32(len(payload)),
	}
	if err := c.ioW.SendWithData(0x6, 0, &h2c, 16, payload); err != nil {
		t.Fatalf("send H2CData: %v", err)
	}

	resp := recvCapsuleResp(t, c.ioR)
	if resp.CID != cid {
		t.Fatalf("Write resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status
}

// ---------- Admin command helpers (routed through adminConn) ----------

// adminIdentify issues an admin Identify with the given CNS and
// optional NSID. Returns the 4 KiB response data + status.
func (c *nvmeClient) adminIdentify(t *testing.T, cns uint8, nsid uint32) (uint16, []byte) {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x06, // adminIdentify
		CID:    cid,
		NSID:   nsid,
		D10:    uint32(cns), // CNS in CDW10[7:0]
	}
	if err := c.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send Identify: %v", err)
	}
	ch, err := c.adminR.Dequeue()
	if err != nil {
		t.Fatalf("read Identify resp hdr: %v", err)
	}
	if ch.Type == 0x5 {
		// Error path — no C2HData, just CapsuleResp.
		var resp nvme.CapsuleResponse
		_ = c.adminR.Receive(&resp)
		return resp.Status, nil
	}
	if ch.Type != 0x7 {
		t.Fatalf("expected C2HData, got 0x%x", ch.Type)
	}
	var dh nvme.C2HDataHeader
	if err := c.adminR.Receive(&dh); err != nil {
		t.Fatalf("recv C2HData: %v", err)
	}
	data := make([]byte, c.adminR.Length())
	if err := c.adminR.ReceiveData(data); err != nil {
		t.Fatalf("read identify data: %v", err)
	}
	resp := recvCapsuleResp(t, c.adminR)
	if resp.CID != cid {
		t.Fatalf("Identify resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status, data
}

// adminPropertyGet issues a Fabric PropertyGet for the given
// register offset. size8=true for CAP (8-byte), false for
// VS/CC/CSTS (4-byte). Returns (status, value).
func (c *nvmeClient) adminPropertyGet(t *testing.T, offset uint32, size8 bool) (uint16, uint64) {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	var sizeBit uint32
	if size8 {
		sizeBit = 1
	}
	cmd := nvme.CapsuleCommand{
		OpCode: 0x7F, // adminFabric
		FCType: 0x04, // fcPropertyGet
		CID:    cid,
		D10:    sizeBit, // bit 0 = size (0=4B, 1=8B)
		D11:    offset,
	}
	if err := c.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send PropertyGet: %v", err)
	}
	resp := recvCapsuleResp(t, c.adminR)
	if resp.CID != cid {
		t.Fatalf("PropertyGet resp CID=%d want %d", resp.CID, cid)
	}
	val := uint64(resp.DW0)
	if size8 {
		val |= uint64(resp.DW1) << 32
	}
	return resp.Status, val
}

// adminPropertySet issues a Fabric PropertySet.
func (c *nvmeClient) adminPropertySet(t *testing.T, offset uint32, size8 bool, value uint64) uint16 {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	var sizeBit uint32
	if size8 {
		sizeBit = 1
	}
	cmd := nvme.CapsuleCommand{
		OpCode: 0x7F,
		FCType: 0x00, // fcPropertySet
		CID:    cid,
		D10:    sizeBit,
		D11:    offset,
		D12:    uint32(value),
		D13:    uint32(value >> 32),
	}
	if err := c.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send PropertySet: %v", err)
	}
	resp := recvCapsuleResp(t, c.adminR)
	if resp.CID != cid {
		t.Fatalf("PropertySet resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status
}

// adminSetFeatures issues Set Features with the given FID in
// CDW10 and value in CDW11. Returns (status, DW0 of response).
func (c *nvmeClient) adminSetFeatures(t *testing.T, fid uint8, cdw11 uint32) (uint16, uint32) {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x09, // adminSetFeatures
		CID:    cid,
		D10:    uint32(fid),
		D11:    cdw11,
	}
	if err := c.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send SetFeatures: %v", err)
	}
	resp := recvCapsuleResp(t, c.adminR)
	if resp.CID != cid {
		t.Fatalf("SetFeatures resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status, resp.DW0
}

// adminGetFeatures issues Get Features with the given FID + SEL.
func (c *nvmeClient) adminGetFeatures(t *testing.T, fid uint8, sel uint8) (uint16, uint32) {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x0A, // adminGetFeatures
		CID:    cid,
		D10:    uint32(fid) | (uint32(sel&0x7) << 8),
	}
	if err := c.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send GetFeatures: %v", err)
	}
	resp := recvCapsuleResp(t, c.adminR)
	if resp.CID != cid {
		t.Fatalf("GetFeatures resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status, resp.DW0
}

// adminKeepAlive issues KeepAlive on the admin queue.
func (c *nvmeClient) adminKeepAlive(t *testing.T) uint16 {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x18, // adminKeepAlive
		CID:    cid,
	}
	if err := c.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send KeepAlive: %v", err)
	}
	resp := recvCapsuleResp(t, c.adminR)
	if resp.CID != cid {
		t.Fatalf("KeepAlive resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status
}

// adminAER issues Async Event Request. Returns the assigned CID
// so the test can assert the server parked it (vs emitted a
// response). Does NOT wait for a CapsuleResp — AER parking is
// non-response, per QA constraint #1.
func (c *nvmeClient) adminAER(t *testing.T) uint16 {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x0C, // adminAsyncEvent
		CID:    cid,
	}
	if err := c.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send AER: %v", err)
	}
	return cid
}

// adminAERExpectLimitExceeded issues an AER and expects a
// CapsuleResp (the parking slot is already full). Returns the
// status.
func (c *nvmeClient) adminAERExpectResponse(t *testing.T) uint16 {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x0C,
		CID:    cid,
	}
	if err := c.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send AER: %v", err)
	}
	resp := recvCapsuleResp(t, c.adminR)
	if resp.CID != cid {
		t.Fatalf("AER resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status
}

// ---------- Status assertion helpers ----------

func expectStatusSuccess(t *testing.T, status uint16, op string) {
	t.Helper()
	if status != 0 {
		t.Fatalf("%s: status=0x%04x want 0 (Success)", op, status)
	}
}

func expectStatusANATransition(t *testing.T, status uint16, op string) {
	t.Helper()
	const sctMask = 0x0E00
	const scMask = 0x01FE
	gotSCT := (status & sctMask) >> 9
	gotSC := (status & scMask) >> 1
	if gotSCT != 3 || gotSC != 3 {
		t.Fatalf("%s: status=0x%04x SCT=%d SC=0x%02x want SCT=3 SC=0x03 (ANA Transition)",
			op, status, gotSCT, gotSC)
	}
}
