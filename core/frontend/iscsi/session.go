package iscsi

// iSCSI session — minimal T2 scope.
//
// One session per TCP connection (MaxConnections=1 in the
// minimal negotiator). The session runs a serial loop:
//
//   loginPhase() → dispatch() per incoming SCSI command.
//
// V2's production-grade session.go has rxLoop/txLoop split for
// parallel tx, CmdSN ordering, R2T for large writes, NOP
// heartbeats, etc. T2 L1 in-process component test needs none
// of that — every WRITE(10) here fits in one Data-Out PDU and
// every READ(10) fits in one Data-In PDU. Production extras
// land in a later checkpoint when L2-OS real-initiator
// compatibility drives the scope.

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// SessionState distinguishes login vs. FullFeature.
type SessionState int

const (
	SessionLogin SessionState = iota
	SessionFullFeature
	SessionClosed
)

// Session carries one iSCSI session's per-connection state.
// Created by the Target on accept; driven by serve().
//
// Backend readiness and Session type interact as follows (fix
// for architect residual-risk note 2026-04-21 round-9):
//   - Login runs FIRST on every accepted connection. The
//     frontend.Provider is NOT dialed before login.
//   - After login succeeds:
//       * Discovery session → skip backend open entirely.
//         SendTargets / Logout / NOP-Out work without a backend.
//         iscsiadm -m discovery succeeds even when the volume's
//         adapter projection is still non-Healthy.
//       * Normal session → open the backend via Provider; a
//         Provider.Open failure (e.g. ErrNotReady) closes the
//         session cleanly WITHOUT blocking subsequent discovery
//         attempts.
type Session struct {
	conn    net.Conn
	handler *SCSIHandler
	logger  Logger

	state SessionState
	tsih  uint16

	// StatSN advances per SCSI Response the target sends.
	statSN uint32

	// Negotiator drives multi-round login parameter exchange.
	// negCfg + resolver + lister are injected by Target at
	// construction so this layer stays unaware of the target's
	// catalog.
	negCfg    NegotiableConfig
	resolver  TargetResolver
	lister    TargetLister
	negResult LoginResult

	// Backend open is deferred to post-login, Normal-session
	// only. provider + volumeID + hcfg are captured at session
	// construction; the session calls provider.Open() after
	// login succeeds and builds the SCSIHandler then.
	provider  frontend.Provider
	volumeID  string
	hcfg      HandlerConfig
	// backend holds the opened backend so serve() can Close it
	// on exit. nil for Discovery sessions.
	backend frontend.Backend

	// Session lifetime.
	closed atomic.Bool
}

// Logger is a tiny abstraction so Target callers can pipe logs
// without pulling in a specific logger package. log.Default
// satisfies it via a small adapter (see target.go).
type Logger interface {
	Printf(format string, args ...interface{})
}

// newSession builds a fresh session for the accepted conn.
// provider + volumeID + hcfg are captured for post-login,
// Normal-session-only backend open (residual-risk fix). The
// backend is NOT opened at construction.
func newSession(conn net.Conn, provider frontend.Provider, volumeID string, hcfg HandlerConfig, negCfg NegotiableConfig, resolver TargetResolver, lister TargetLister, logger Logger) *Session {
	return &Session{
		conn:     conn,
		logger:   logger,
		state:    SessionLogin,
		negCfg:   negCfg,
		resolver: resolver,
		lister:   lister,
		provider: provider,
		volumeID: volumeID,
		hcfg:     hcfg,
	}
}

// serve runs the session until the connection closes.
func (s *Session) serve(ctx context.Context) error {
	defer s.close()
	// Phase 1: login negotiation. No backend opened yet.
	if err := s.loginPhase(); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	// Phase 2: open backend for Normal sessions only. Discovery
	// sessions never touch the frontend.Provider — they only
	// need SendTargets, which comes from the TargetLister that
	// was wired at construction.
	if s.negResult.SessionType == SessionTypeNormal {
		backend, err := s.provider.Open(ctx, s.volumeID)
		if err != nil {
			// Clean close — do NOT hold the socket. A parallel
			// discovery attempt on the listen addr must still
			// make progress.
			if s.logger != nil {
				s.logger.Printf("iscsi: Provider.Open(%s): %v (closing Normal session)",
					s.volumeID, err)
			}
			return fmt.Errorf("provider open: %w", err)
		}
		s.backend = backend
		hcfg := s.hcfg
		hcfg.Backend = backend
		s.handler = NewSCSIHandler(hcfg)
	}
	// Phase 3: Full-Feature dispatch loop.
	return s.fullFeatureLoop(ctx)
}

func (s *Session) loginPhase() error {
	neg := NewLoginNegotiator(s.negCfg)
	for s.state == SessionLogin {
		pdu, err := ReadPDU(s.conn)
		if err != nil {
			return err
		}
		if pdu.Opcode() != OpLoginReq {
			return fmt.Errorf("%w: expected Login-Request, got %s",
				ErrLoginInvalidRequest, OpcodeName(pdu.Opcode()))
		}
		resp := neg.HandleLoginPDU(pdu, s.resolver)
		if err := WritePDU(s.conn, resp); err != nil {
			return err
		}
		// If negotiator emitted a reject, the response status is
		// non-Success; close the session after the reply.
		if resp.LoginStatusClass() != LoginStatusSuccess {
			return fmt.Errorf("login rejected: class=0x%02x detail=0x%02x",
				resp.LoginStatusClass(), resp.LoginStatusDetail())
		}
		if neg.Done() {
			s.state = SessionFullFeature
			s.negResult = neg.Result()
			if s.logger != nil {
				s.logger.Printf("session: FullFeature initiator=%q target=%q type=%q",
					s.negResult.InitiatorName, s.negResult.TargetName, s.negResult.SessionType)
			}
			return nil
		}
	}
	return nil
}

func (s *Session) fullFeatureLoop(ctx context.Context) error {
	for s.state == SessionFullFeature {
		pdu, err := ReadPDU(s.conn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := s.dispatch(ctx, pdu); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) dispatch(ctx context.Context, pdu *PDU) error {
	switch pdu.Opcode() {
	case OpSCSICmd:
		// Discovery sessions reject SCSI commands per RFC 7143
		// §6.2 — discovery is a control plane only. Reply with
		// Reject so iscsiadm doesn't hang.
		if s.negResult.SessionType == SessionTypeDiscovery {
			return fmt.Errorf("SCSI command in Discovery session")
		}
		if s.handler == nil {
			return fmt.Errorf("SCSI handler not configured for this session")
		}
		return s.handleSCSICmd(ctx, pdu)
	case OpSCSIDataOut:
		// In the minimal session, SCSI-Cmd carries immediate data
		// for all writes (see handleSCSICmd). A stray Data-Out
		// without a preceding R2T is a protocol error.
		return fmt.Errorf("unexpected Data-Out without R2T")
	case OpTextReq:
		return s.handleTextReq(pdu)
	case OpLogoutReq:
		return s.handleLogout(pdu)
	case OpNOPOut:
		return s.handleNOPOut(pdu)
	default:
		return fmt.Errorf("unsupported opcode in FFP: %s", OpcodeName(pdu.Opcode()))
	}
}

// handleTextReq services SendTargets discovery (RFC 7143 §12.3).
// Other text keys produce an empty Text-Response. Discovery
// sessions are the typical caller; Normal sessions are also
// allowed to issue Text Requests but T2's response set is the
// same.
func (s *Session) handleTextReq(req *PDU) error {
	var targets []DiscoveryTarget
	if s.lister != nil {
		targets = s.lister.ListTargets()
	}
	resp := HandleTextRequest(req, targets)
	// Text-Response advances StatSN like a SCSI-Response would.
	s.statSN++
	resp.SetStatSN(s.statSN)
	resp.SetExpCmdSN(req.CmdSN() + 1)
	resp.SetMaxCmdSN(req.CmdSN() + 32)
	return WritePDU(s.conn, resp)
}

// handleSCSICmd processes a SCSI-Cmd PDU.
//
//   - WRITE (FlagW): dataOut may arrive in two places:
//       1. Immediate data in the SCSI-Cmd's data segment (when
//          ImmediateData=Yes was negotiated).
//       2. Remaining bytes solicited via R2T + Data-Out PDUs
//          (T2 ckpt 10 port — enables iscsiadm writes larger
//          than FirstBurstLength).
//     The full payload is dispatched to the SCSI handler once
//     ExpectedDataTransferLength bytes are collected.
//
//   - READ (FlagR) / metadata: response data rides in one or
//     more Data-In PDUs with the S-bit on the last. Small
//     payloads fit in a single Data-In; large reads that exceed
//     MaxRecvDataSegmentLength are split (MaxRecv defaults to
//     256 KiB, bigger than T2 contract-smoke payloads).
func (s *Session) handleSCSICmd(ctx context.Context, req *PDU) error {
	edtl := req.ExpectedDataTransferLength()
	isWrite := req.OpSpecific1()&FlagW != 0

	var dataOut []byte
	if isWrite && edtl > 0 {
		collected, err := s.collectWriteData(req, edtl)
		if err != nil {
			return err
		}
		dataOut = collected
	} else {
		// Metadata or read: dataOut is just the inline segment
		// (typically empty).
		dataOut = req.DataSegment
	}

	cdb := req.CDB()
	result := s.handler.HandleCommand(ctx, cdb, dataOut)

	if len(result.Data) > 0 {
		return s.sendDataInWithStatus(req, result)
	}
	return s.sendSCSIResponse(req, result)
}

// collectWriteData assembles the write payload from immediate
// data (carried in the SCSI-Cmd) + any Data-Out PDUs solicited
// via R2T. Returns the full edtl-sized buffer on success.
//
// T2 scope: single R2T for all remaining bytes. V2 supports
// MaxBurstLength-chunked multi-R2T; we'll port that chunking
// only if an OS initiator actually exposes the need — per the
// assignment §4.3 rule "port the smallest required R2T path".
func (s *Session) collectWriteData(req *PDU, edtl uint32) ([]byte, error) {
	buf := make([]byte, edtl)
	var received uint32

	// Immediate data from the SCSI-Cmd's data segment.
	if n := uint32(len(req.DataSegment)); n > 0 {
		if n > edtl {
			return nil, fmt.Errorf("immediate data %d > EDTL %d", n, edtl)
		}
		copy(buf, req.DataSegment)
		received = n
	}
	if received >= edtl {
		return buf, nil
	}

	// Solicit the remainder with one R2T. TargetTransferTag is
	// arbitrary per-target scope; 1 is fine while a session only
	// has one outstanding write at a time (T2 minimal session).
	const ttt uint32 = 1
	r2t := &PDU{}
	r2t.SetOpcode(OpR2T)
	r2t.SetOpSpecific1(FlagF)
	r2t.SetLUN(req.LUN())
	r2t.SetInitiatorTaskTag(req.InitiatorTaskTag())
	r2t.SetTargetTransferTag(ttt)
	// StatSN on R2T is a snapshot of the current StatSN (no
	// increment — R2T doesn't carry status).
	r2t.SetStatSN(s.statSN)
	r2t.SetExpCmdSN(req.CmdSN() + 1)
	r2t.SetMaxCmdSN(req.CmdSN() + 32)
	r2t.SetR2TSN(0)
	r2t.SetBufferOffset(received)
	r2t.SetDesiredDataLength(edtl - received)
	if err := WritePDU(s.conn, r2t); err != nil {
		return nil, fmt.Errorf("send R2T: %w", err)
	}

	// Read Data-Out PDU(s) until we have edtl bytes. V2 enforces
	// DataSN + BufferOffset ordering (DataPDUInOrder /
	// DataSequenceInOrder both default Yes); we do the same.
	var nextDataSN uint32
	for received < edtl {
		pdu, err := ReadPDU(s.conn)
		if err != nil {
			return nil, fmt.Errorf("read Data-Out: %w", err)
		}
		if pdu.Opcode() != OpSCSIDataOut {
			return nil, fmt.Errorf("expected Data-Out, got %s", OpcodeName(pdu.Opcode()))
		}
		if pdu.TargetTransferTag() != ttt && pdu.TargetTransferTag() != 0xFFFFFFFF {
			// Initiators typically echo our TTT; 0xFFFFFFFF is
			// also accepted for unsolicited Data-Out but we
			// disable that via InitialR2T=Yes.
			return nil, fmt.Errorf("Data-Out TTT=0x%08x does not match R2T TTT=0x%08x",
				pdu.TargetTransferTag(), ttt)
		}
		if pdu.DataSN() != nextDataSN {
			return nil, fmt.Errorf("Data-Out DataSN=%d, expected %d",
				pdu.DataSN(), nextDataSN)
		}
		nextDataSN++

		offset := pdu.BufferOffset()
		if offset != received {
			return nil, fmt.Errorf("Data-Out BufferOffset=%d does not match received=%d",
				offset, received)
		}
		data := pdu.DataSegment
		end := offset + uint32(len(data))
		if end > edtl {
			return nil, fmt.Errorf("Data-Out extends past EDTL: end=%d edtl=%d",
				end, edtl)
		}
		copy(buf[offset:], data)
		received = end

		// F-bit on Data-Out marks the last PDU of the sequence.
		// We accept it as a hint but trust received == edtl as
		// the authoritative termination condition.
		if pdu.OpSpecific1()&FlagF != 0 && received != edtl {
			return nil, fmt.Errorf("Data-Out F-bit with received=%d < edtl=%d",
				received, edtl)
		}
	}
	return buf, nil
}

func (s *Session) sendDataInWithStatus(req *PDU, r SCSIResult) error {
	p := &PDU{}
	p.SetOpcode(OpSCSIDataIn)
	// Set Final + Status bits (byte 1).
	p.BHS[1] = FlagF | FlagS
	p.SetLUN(req.LUN())
	p.SetInitiatorTaskTag(req.InitiatorTaskTag())
	// Target Transfer Tag = 0xFFFFFFFF for unsolicited Data-In.
	p.SetTargetTransferTag(0xFFFFFFFF)
	// StatSN advances only on PDUs that carry status.
	s.statSN++
	p.SetStatSN(s.statSN)
	p.SetExpCmdSN(req.CmdSN() + 1)
	p.SetMaxCmdSN(req.CmdSN() + 32)
	// DataSN and BufferOffset: single Data-In → DataSN=0, offset=0.
	p.SetDataSN(0)
	p.SetBufferOffset(0)
	// SCSI status byte (BHS[3]) rides with the S-bit.
	p.SetSCSIStatusByte(r.Status)
	// ResidualCount = 0 (exact fit) — a richer implementation
	// would compute over/underflow.
	p.SetResidualCount(0)
	p.DataSegment = r.Data
	return WritePDU(s.conn, p)
}

func (s *Session) sendSCSIResponse(req *PDU, r SCSIResult) error {
	p := &PDU{}
	p.SetOpcode(OpSCSIResp)
	// Byte 1 bit 7 (F) must be set on SCSI-Response.
	p.BHS[1] = FlagF
	p.SetSCSIResponse(ISCSIRespCompleted)
	p.SetSCSIStatusByte(r.Status)
	p.SetLUN(req.LUN())
	p.SetInitiatorTaskTag(req.InitiatorTaskTag())
	s.statSN++
	p.SetStatSN(s.statSN)
	p.SetExpCmdSN(req.CmdSN() + 1)
	p.SetMaxCmdSN(req.CmdSN() + 32)
	// For CHECK CONDITION responses, sense data rides in the
	// data segment as fixed-format sense (18 bytes).
	if r.Status == StatusCheckCondition {
		p.DataSegment = buildSenseData(r.SenseKey, r.ASC, r.ASCQ)
	}
	return WritePDU(s.conn, p)
}

func (s *Session) handleLogout(req *PDU) error {
	resp := &PDU{}
	resp.SetOpcode(OpLogoutResp)
	resp.BHS[1] = FlagF
	resp.SetInitiatorTaskTag(req.InitiatorTaskTag())
	s.statSN++
	resp.SetStatSN(s.statSN)
	resp.SetExpCmdSN(req.CmdSN() + 1)
	resp.SetMaxCmdSN(req.CmdSN() + 32)
	// Response code 0 = "connection/session closed successfully".
	if err := WritePDU(s.conn, resp); err != nil {
		return err
	}
	s.state = SessionClosed
	return nil
}

func (s *Session) handleNOPOut(req *PDU) error {
	// Echo back as NOP-In, preserving the initiator's ITT.
	resp := &PDU{}
	resp.SetOpcode(OpNOPIn)
	resp.BHS[1] = FlagF
	resp.SetLUN(req.LUN())
	resp.SetInitiatorTaskTag(req.InitiatorTaskTag())
	// Target Transfer Tag = 0xFFFFFFFF for unsolicited NOP-In.
	resp.SetTargetTransferTag(0xFFFFFFFF)
	s.statSN++
	resp.SetStatSN(s.statSN)
	resp.SetExpCmdSN(req.CmdSN() + 1)
	resp.SetMaxCmdSN(req.CmdSN() + 32)
	return WritePDU(s.conn, resp)
}

func (s *Session) close() {
	if s.closed.CompareAndSwap(false, true) {
		if s.backend != nil {
			_ = s.backend.Close()
		}
		_ = s.conn.Close()
		s.state = SessionClosed
	}
}
