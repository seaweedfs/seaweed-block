package nvme

// NVMe/TCP session — minimal T2 scope.
//
// One controller per TCP connection. Sequence:
//   1. ICReq -> ICResp (TCP-level transport handshake)
//   2. Fabric Connect (CapsuleCmd OpCode=0x7F FCType=0x01) ->
//      CapsuleResp success (controller established)
//   3. IO Read/Write/Flush via CapsuleCmd; Read replies with
//      C2HData (carrying the bytes) + CapsuleResp; Write
//      replies with R2T (request data) -> H2CData (host sends) ->
//      CapsuleResp.
//
// Production NVMe-TCP supports many features T2 skips:
// digests (HDGST/DDGST), Identify Controller / Identify
// Namespace / Active NS List, Get Log Page, Set Features
// negotiation, multi-PDU H2C/C2H Data, Async Event Reports,
// Keep Alive timer. Those land with capsule extensions in a
// later checkpoint as L2-OS enablement requires.

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
)

// Session carries per-connection NVMe/TCP state.
type Session struct {
	conn    net.Conn
	r       *Reader
	w       *Writer
	handler *IOHandler
	logger  Logger

	// target holds a reference back to the Target so we can
	// allocate/look-up CNTLIDs on Connect. Set by the Target
	// when it constructs the session (11a).
	target *Target

	// expectedSubNQN is the subsystem NQN this session must
	// match against the host's Fabric Connect ConnectData.
	// Empty means "no enforcement" (in-process L1 tests that
	// don't pin a subsystem identity); production always sets
	// it via Target.SubsysNQN. When non-empty and the host's
	// SubNQN doesn't match, Connect is rejected with
	// SCT=CommandSpecific SC=ConnectInvalidParameters
	// (NVMe-oF spec §3.3.1).
	expectedSubNQN string

	connected atomic.Bool // true after Fabric Connect

	// Queue-model state set during Fabric Connect (§3.1 A10.5 + R3,
	// QA finding #1 moving queue model into 11a):
	//
	//   qid   — 0 = admin queue; >0 = IO queue. Determines which
	//           opcodes the dispatcher accepts on this session.
	//   ctrl  — set on BOTH admin and IO sessions. Admin Connect
	//           allocates the controller and pins it here; IO
	//           Connect must cite an existing CNTLID and pins
	//           the same pointer.
	qid    uint16
	qidSet bool
	ctrl   *adminController

	closed atomic.Bool

	// sqhd advances per CapsuleResponse (NVMe submission queue
	// head pointer for queue depth tracking — 0 in T2 scope
	// but bumped so the wire field looks live).
	sqhd uint16
}

// Logger is a tiny indirection for log injection.
type Logger interface {
	Printf(format string, args ...interface{})
}

func newSession(conn net.Conn, h *IOHandler, target *Target, expectedSubNQN string, lg Logger) *Session {
	return &Session{
		conn:           conn,
		r:              NewReader(conn),
		w:              NewWriter(conn),
		handler:        h,
		target:         target,
		expectedSubNQN: expectedSubNQN,
		logger:         lg,
	}
}

func (s *Session) serve(ctx context.Context) error {
	defer s.close()
	if err := s.handleICReq(); err != nil {
		return fmt.Errorf("ICReq: %w", err)
	}
	for !s.closed.Load() {
		ch, err := s.r.Dequeue()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		switch ch.Type {
		case pduCapsuleCmd:
			if err := s.handleCapsuleCmd(ctx); err != nil {
				return err
			}
		case pduH2CData:
			// Stray Data PDU outside an active R2T: protocol error.
			// (T2 minimal session requests R2T inline within
			// handleCapsuleCmd, so the dispatcher never returns
			// here mid-write.)
			return fmt.Errorf("unexpected H2CData outside R2T window")
		default:
			return fmt.Errorf("unsupported PDU type 0x%x", ch.Type)
		}
	}
	return nil
}

// ---------- Phase 1: TCP-level init ----------

func (s *Session) handleICReq() error {
	// Read ICReq.
	ch, err := s.r.Dequeue()
	if err != nil {
		return err
	}
	if ch.Type != pduICReq {
		return fmt.Errorf("expected ICReq (0x%x) got 0x%x", pduICReq, ch.Type)
	}
	var req ICRequest
	if err := s.r.Receive(&req); err != nil {
		return err
	}

	// Reply with ICResp. We accept whatever PDUFormatVersion the
	// host sent — T2 minimal compatibility.
	resp := ICResponse{
		PDUFormatVersion: req.PDUFormatVersion,
		PDUDataAlignment: 0,
		PDUDataDigest:    0,
		MaxH2CDataLength: 0x8000, // 32 KiB; bigger writes get split into multi-PDU
	}
	return s.w.SendHeaderOnly(pduICResp, &resp, icBodySize)
}

// ---------- Phase 2/3: capsule command dispatch ----------

func (s *Session) handleCapsuleCmd(ctx context.Context) error {
	var cmd CapsuleCommand
	if err := s.r.Receive(&cmd); err != nil {
		return err
	}
	// Read inline ConnectData if this is Fabric Connect (capsule
	// can carry a 1024-byte payload).
	var inline []byte
	if n := s.r.Length(); n > 0 {
		inline = make([]byte, n)
		if err := s.r.ReceiveData(inline); err != nil {
			return err
		}
	}

	// Fabric commands (including Connect) bypass the qid/connected
	// gate — they ESTABLISH the gate.
	if cmd.OpCode == adminFabric {
		return s.handleFabric(&cmd, inline)
	}
	if !s.connected.Load() {
		return s.replyStatus(&cmd, MakeStatusField(SCTGeneric, SCInvalidOpcode, true))
	}

	// Post-Connect dispatch: route by queue type (QA finding #1
	// moved queue routing into 11a).
	//   QID=0 (admin queue) → admin opcodes only; IO opcodes
	//                         return Invalid Opcode.
	//   QID>0 (IO queue)    → IO opcodes only; admin opcodes
	//                         return Invalid Opcode.
	// This enforces the NVMe queue model Linux kernel expects:
	// admin commands must arrive on the admin connection, IO
	// must arrive on the IO connection.
	if s.qid == 0 {
		return s.adminDispatch(ctx, &cmd)
	}
	return s.ioDispatch(ctx, &cmd)
}

// adminDispatch routes admin-queue opcodes. 11a implements
// Identify (CNS 0x00/0x01/0x02/0x03). SetFeatures / GetFeatures /
// KeepAlive / AsyncEventRequest are 11b. Unknown opcodes return
// Invalid Opcode — §6 stop rule #4 (advertised ≡ implemented)
// means Identify Controller must NOT advertise any capability
// that falls through to this default.
func (s *Session) adminDispatch(ctx context.Context, cmd *CapsuleCommand) error {
	switch cmd.OpCode {
	case adminIdentify:
		return s.handleAdminIdentify(cmd)
	default:
		return s.replyStatus(cmd, MakeStatusField(SCTGeneric, SCInvalidOpcode, true))
	}
}

// ioDispatch routes IO-queue opcodes.
func (s *Session) ioDispatch(ctx context.Context, cmd *CapsuleCommand) error {
	switch cmd.OpCode {
	case ioRead:
		return s.handleRead(ctx, cmd)
	case ioWrite:
		return s.handleWrite(ctx, cmd)
	case ioFlush:
		return s.replyStatus(cmd, 0) // Success — SYNC handled by backend in T3
	default:
		return s.replyStatus(cmd, MakeStatusField(SCTGeneric, SCInvalidOpcode, true))
	}
}

// handleFabric processes Fabric-specific commands. Today: only
// Connect (fcConnect). PropertyGet / PropertySet / Disconnect
// land with Batch 11b.
//
// Connect semantics per NVMe-oF §3.3:
//   - CDW10[31:16] carries QID (admin=0, IO>0).
//   - ConnectData at bytes 16–17 carries CNTLID. For admin
//     queue Connect the host sends 0xFFFF (request new); the
//     target allocates a controller and returns the assigned
//     CNTLID in CapsuleResp DW0[15:0]. For IO queue Connect
//     the host echoes its previously-assigned CNTLID; the
//     target validates it against the registered admin
//     controllers (R3).
//
// Errors per NVMe-oF §3.3.1:
//   - Wrong SubNQN → Command Specific / ConnectInvalidParameters
//   - Unknown CNTLID on IO Connect → Command Specific /
//     ConnectInvalidHost (0x82) — distinct from 0x80 so logs
//     disambiguate "wrong subsystem" vs "unknown controller".
//   - Double-Connect on same session → InvalidOpcode
//     (we don't support re-Connect within one TCP conn).
func (s *Session) handleFabric(cmd *CapsuleCommand, inline []byte) error {
	if cmd.FCType != fcConnect {
		return s.replyStatus(cmd, MakeStatusField(SCTGeneric, SCInvalidOpcode, true))
	}
	if s.qidSet {
		// Connect already processed on this session. NVMe-oF
		// does not allow re-Connect on an established session.
		return s.replyStatus(cmd, MakeStatusField(SCTGeneric, SCInvalidOpcode, true))
	}
	if len(inline) < connectDataSize {
		return s.replyStatus(cmd, MakeStatusField(SCTGeneric, SCInvalidField, true))
	}
	var cd ConnectData
	cd.Unmarshal(inline[:connectDataSize])

	qid := uint16(cmd.D10 >> 16)

	// Subsystem identity check (applies to both admin and IO
	// Connect). Reuses ckpt 8 follow-up enforcement.
	if s.expectedSubNQN != "" && cd.SubNQN != s.expectedSubNQN {
		if s.logger != nil {
			s.logger.Printf("nvme: Connect REJECTED (wrong subsys) host=%q wanted=%q got=%q qid=%d",
				cd.HostNQN, s.expectedSubNQN, cd.SubNQN, qid)
		}
		return s.replyStatus(cmd,
			MakeStatusField(SCTCommandSpecific, SCConnectInvalidParameters, true))
	}

	if qid == 0 {
		return s.handleAdminConnect(cmd, &cd, qid)
	}
	return s.handleIOConnect(cmd, &cd, qid)
}

// handleAdminConnect allocates a fresh CNTLID, registers an
// admin controller with the Target, pins it to this session,
// and replies with the assigned CNTLID in CapsuleResp DW0.
func (s *Session) handleAdminConnect(cmd *CapsuleCommand, cd *ConnectData, qid uint16) error {
	// NVMe-oF §3.3: admin Connect MUST set CNTLID=0xFFFF
	// (request new). Some hosts send 0x0 instead; accept both
	// as "allocate new" for L2-OS compat.
	if cd.CNTLID != 0xFFFF && cd.CNTLID != 0x0000 {
		if s.logger != nil {
			s.logger.Printf("nvme: admin Connect with preset CNTLID=0x%04x rejected (must be 0xFFFF)",
				cd.CNTLID)
		}
		return s.replyStatus(cmd,
			MakeStatusField(SCTCommandSpecific, SCConnectInvalidParameters, true))
	}
	// Target may be nil in narrow in-process tests that use
	// newSession directly; in that case we allocate a local
	// controller without a registry.
	var ctrl *adminController
	if s.target != nil {
		// R1: stash VolumeID at admin Connect so Identify
		// builders can derive NGUID/Serial deterministically
		// (sha256 derivation — no "SWF00001" stub).
		ctrl = s.target.allocAdminController(cd.SubNQN, cd.HostNQN, s.target.cfg.VolumeID)
	} else {
		ctrl = &adminController{
			cntlID:   1,
			subNQN:   cd.SubNQN,
			hostNQN:  cd.HostNQN,
			volumeID: "v1",
		}
	}
	s.qid = qid
	s.qidSet = true
	s.ctrl = ctrl
	s.connected.Store(true)
	if s.logger != nil {
		s.logger.Printf("nvme: admin Connect accepted host=%q subsys=%q cntlid=%d",
			cd.HostNQN, cd.SubNQN, ctrl.cntlID)
	}
	// CapsuleResp DW0[15:0] = assigned CNTLID; Linux host reads
	// this and echoes it in subsequent IO queue Connect.
	return s.sendCapsuleResp(cmd.CID, uint32(ctrl.cntlID), 0, 0)
}

// handleIOConnect validates that the host's CNTLID claim points
// to a registered admin controller and pins that controller to
// this IO queue session.
func (s *Session) handleIOConnect(cmd *CapsuleCommand, cd *ConnectData, qid uint16) error {
	var ctrl *adminController
	if s.target != nil {
		ctrl = s.target.lookupAdminController(cd.CNTLID)
	}
	if ctrl == nil {
		if s.logger != nil {
			s.logger.Printf("nvme: IO Connect qid=%d CNTLID=%d not found (admin session must precede)",
				qid, cd.CNTLID)
		}
		// ConnectInvalidHost (SCT=CommandSpecific SC=0x82) is the
		// dedicated code for "host claim doesn't match a known
		// controller" — distinct from 0x80 "wrong SubNQN".
		return s.replyStatus(cmd,
			MakeStatusField(SCTCommandSpecific, SCConnectInvalidHost, true))
	}
	// Cross-check SubNQN matches admin's (V2 parity).
	//
	// NOT cross-checking HostNQN: V2 doesn't, and nvme-cli
	// is spec-allowed to emit slightly different HostNQN per
	// queue (uncommon but legal). An earlier sw draft added
	// HostNQN cross-check as belt-and-suspenders but that
	// diverged from V2 and risked breaking real Linux L2-OS.
	// T8 (auth) owns tighter HostNQN policy.
	if cd.SubNQN != ctrl.subNQN {
		if s.logger != nil {
			s.logger.Printf("nvme: IO Connect CNTLID=%d SubNQN mismatch: got %q want %q",
				cd.CNTLID, cd.SubNQN, ctrl.subNQN)
		}
		return s.replyStatus(cmd,
			MakeStatusField(SCTCommandSpecific, SCConnectInvalidHost, true))
	}
	s.qid = qid
	s.qidSet = true
	s.ctrl = ctrl
	s.connected.Store(true)
	if s.logger != nil {
		s.logger.Printf("nvme: IO Connect accepted host=%q subsys=%q cntlid=%d qid=%d",
			cd.HostNQN, cd.SubNQN, ctrl.cntlID, qid)
	}
	// IO Connect's CapsuleResp echoes CNTLID in DW0 (matches V2).
	return s.sendCapsuleResp(cmd.CID, uint32(ctrl.cntlID), 0, 0)
}

func (s *Session) handleRead(ctx context.Context, cmd *CapsuleCommand) error {
	res := s.handler.Handle(ctx, IOCommand{
		Opcode: cmd.OpCode,
		NSID:   cmd.NSID,
		SLBA:   cmd.Lba(),
		NLB:    cmd.LbaLength(),
	})
	if res.AsError() != nil {
		return s.replyStatus(cmd, MakeStatusField(res.SCT, res.SC, true))
	}
	// Send C2HData with last flag, then a CapsuleResp Success.
	hdr := C2HDataHeader{
		CCCID: cmd.CID,
		DATAO: 0,
		DATAL: uint32(len(res.Data)),
	}
	if err := s.w.SendWithData(pduC2HData, c2hFlagLast, &hdr, c2hDataHdrSize, res.Data); err != nil {
		return err
	}
	return s.sendCapsuleResp(cmd.CID, 0, 0, 0)
}

func (s *Session) handleWrite(ctx context.Context, cmd *CapsuleCommand) error {
	totalBytes := cmd.LbaLength() * s.handler.BlockSize()

	// Issue R2T to request the host send the write payload.
	r2t := R2THeader{
		CCCID: cmd.CID,
		TAG:   1,
		DATAO: 0,
		DATAL: totalBytes,
	}
	if err := s.w.SendHeaderOnly(pduR2T, &r2t, r2tHdrSize); err != nil {
		return err
	}

	// Receive H2CData PDU(s) until we have totalBytes.
	collected := make([]byte, 0, totalBytes)
	for uint32(len(collected)) < totalBytes {
		ch, err := s.r.Dequeue()
		if err != nil {
			return err
		}
		if ch.Type != pduH2CData {
			return fmt.Errorf("expected H2CData, got 0x%x", ch.Type)
		}
		var h H2CDataHeader
		if err := s.r.Receive(&h); err != nil {
			return err
		}
		if h.CCCID != cmd.CID || h.TAG != r2t.TAG {
			return fmt.Errorf("H2CData CCCID/TAG mismatch")
		}
		chunk := make([]byte, s.r.Length())
		if err := s.r.ReceiveData(chunk); err != nil {
			return err
		}
		collected = append(collected, chunk...)
	}

	res := s.handler.Handle(ctx, IOCommand{
		Opcode: cmd.OpCode,
		NSID:   cmd.NSID,
		SLBA:   cmd.Lba(),
		NLB:    cmd.LbaLength(),
		Data:   collected,
	})
	if res.AsError() != nil {
		return s.replyStatus(cmd, MakeStatusField(res.SCT, res.SC, true))
	}
	return s.sendCapsuleResp(cmd.CID, 0, 0, 0)
}

func (s *Session) replyStatus(cmd *CapsuleCommand, status uint16) error {
	return s.sendCapsuleResp(cmd.CID, 0, 0, status)
}

func (s *Session) sendCapsuleResp(cid uint16, dw0, dw1 uint32, status uint16) error {
	s.sqhd++
	resp := CapsuleResponse{
		DW0:     dw0,
		DW1:     dw1,
		SQHD:    s.sqhd,
		QueueID: 0,
		CID:     cid,
		Status:  status,
	}
	return s.w.SendHeaderOnly(pduCapsuleResp, &resp, capsuleRespSize)
}

func (s *Session) close() {
	if s.closed.CompareAndSwap(false, true) {
		// Admin session (QID=0) owns the CNTLID in the Target
		// registry. Release it so IO sessions that outlive the
		// admin session can't see a "phantom" CNTLID. IO
		// sessions that ARE still active hold the ctrl pointer
		// locally and continue operating; they don't re-validate
		// against the registry per-command (port plan §3.1 A10.5
		// note).
		if s.qid == 0 && s.ctrl != nil && s.target != nil {
			s.target.releaseAdminController(s.ctrl.cntlID)
		}
		_ = s.conn.Close()
	}
}
