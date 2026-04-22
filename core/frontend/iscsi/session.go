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
	// catalog. Discovery sessions don't reach the SCSI handler;
	// they exit after the SendTargets exchange + Logout.
	negCfg   NegotiableConfig
	resolver TargetResolver
	lister   TargetLister
	negResult LoginResult

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
// negCfg/resolver/lister come from the Target. handler may be
// nil for Discovery-only sessions; the negotiator decides which
// path applies after login.
func newSession(conn net.Conn, handler *SCSIHandler, negCfg NegotiableConfig, resolver TargetResolver, lister TargetLister, logger Logger) *Session {
	return &Session{
		conn:     conn,
		handler:  handler,
		logger:   logger,
		state:    SessionLogin,
		negCfg:   negCfg,
		resolver: resolver,
		lister:   lister,
	}
}

// serve runs the session until the connection closes.
func (s *Session) serve(ctx context.Context) error {
	defer s.close()
	// Login phase — serial, single-PDU round trips.
	if err := s.loginPhase(); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	// Full-Feature phase — dispatch SCSI commands.
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

// handleSCSICmd processes a SCSI-Cmd PDU. For T2 scope:
//   - WRITE(10): dataOut MUST be carried in the SCSI-Cmd's
//     data segment (ImmediateData=true in the minimal negotiator).
//     A future checkpoint will add R2T + multi-PDU Data-Out for
//     OS initiator compat.
//   - READ(10): target returns Data-In PDU(s) followed by a
//     SCSI-Response. For small (<=one-block) reads we pack both
//     into one Data-In with the S-bit set.
//   - Metadata (TUR, INQUIRY, READ CAPACITY, REPORT LUNS, etc):
//     response data rides in Data-In if any; SCSI-Response
//     carries the status.
func (s *Session) handleSCSICmd(ctx context.Context, req *PDU) error {
	cdb := req.CDB()
	dataOut := req.DataSegment
	result := s.handler.HandleCommand(ctx, cdb, dataOut)

	// Pack response. For commands that return data (READ,
	// INQUIRY, ...), emit one Data-In PDU carrying the full
	// payload with S-bit set so the status is conveyed in the
	// same PDU. No separate SCSI-Response is needed in that case.
	if len(result.Data) > 0 {
		return s.sendDataInWithStatus(req, result)
	}
	return s.sendSCSIResponse(req, result)
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
		_ = s.conn.Close()
		s.state = SessionClosed
	}
}
