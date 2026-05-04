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
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// SessionState distinguishes login vs. FullFeature.
type SessionState int

const (
	SessionLogin SessionState = iota
	SessionFullFeature
	SessionClosed
)

// maxPendingQueue bounds commands queued while a write is collecting
// R2T-solicited Data-Out. Linux initiators can pipeline a later SCSI
// command during an earlier write's Data-Out phase; queueing keeps that
// legal shape from tearing down the session, while the bound prevents an
// unbounded memory sink from a hostile initiator.
const maxPendingQueue = 64

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
	negCfg         NegotiableConfig
	dataOutTimeout time.Duration
	resolver       TargetResolver
	lister         TargetLister
	negResult      LoginResult

	// Backend open is deferred to post-login, Normal-session
	// only. provider + volumeID + hcfg are captured at session
	// construction; the session calls provider.Open() after
	// login succeeds and builds the SCSIHandler then.
	provider frontend.Provider
	volumeID string
	hcfg     HandlerConfig
	// backend holds the opened backend so serve() can Close it
	// on exit. nil for Discovery sessions.
	backend frontend.Backend

	// Session lifetime.
	closed atomic.Bool

	// pending holds non-Data-Out PDUs read while collectWriteData is
	// waiting for R2T-solicited Data-Out. The main loop drains this
	// before reading the socket again, preserving serial command
	// processing without rejecting initiator pipelining.
	pending []*PDU
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
func newSession(conn net.Conn, provider frontend.Provider, volumeID string, hcfg HandlerConfig, negCfg NegotiableConfig, dataOutTimeout time.Duration, resolver TargetResolver, lister TargetLister, logger Logger) *Session {
	return &Session{
		conn:           conn,
		logger:         logger,
		state:          SessionLogin,
		negCfg:         negCfg,
		dataOutTimeout: dataOutTimeout,
		resolver:       resolver,
		lister:         lister,
		provider:       provider,
		volumeID:       volumeID,
		hcfg:           hcfg,
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
		pdu, err := s.nextPDU()
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

func (s *Session) nextPDU() (*PDU, error) {
	if len(s.pending) > 0 {
		pdu := s.pending[0]
		copy(s.pending, s.pending[1:])
		s.pending[len(s.pending)-1] = nil
		s.pending = s.pending[:len(s.pending)-1]
		return pdu, nil
	}
	return ReadPDU(s.conn)
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
//     EDTL is validated against the CDB's transfer length BEFORE
//     any allocation / R2T is issued — otherwise a hostile
//     initiator could inflate EDTL vs. CDB and force
//     make([]byte, edtl) + solicited Data-Out collection before
//     the SCSI layer's own bounds check runs (architect review
//     2026-04-21 ckpt 10 Medium finding).
//
//   - READ (FlagR) / metadata: response data rides in ONE
//     Data-In PDU with the S-bit set. T2 ckpt 10 does NOT split
//     reads across multiple Data-In PDUs (MaxRecvDataSegmentLength
//     currently bounds the whole read at 256 KiB; larger reads
//     exceed T2 contract-smoke payloads). Multi-Data-In splitting
//     lands if an OS initiator needs it.
func (s *Session) handleSCSICmd(ctx context.Context, req *PDU) error {
	cdb := req.CDB()
	edtl := req.ExpectedDataTransferLength()
	isWrite := req.OpSpecific1()&FlagW != 0

	var dataOut []byte
	if isWrite && edtl > 0 {
		// Pre-flight EDTL vs CDB consistency check. For writes
		// we recognize (WRITE(10)), compute the authoritative
		// expected bytes and reject mismatches at CHECK CONDITION
		// BEFORE allocating edtl bytes or issuing R2T. Writes
		// using an opcode we don't recognize here fall through
		// to the handler (which rejects Invalid Opcode).
		if expected, ok := cdbExpectedWriteBytes(cdb, s.handler.BlockSize()); ok {
			if uint64(edtl) != expected {
				return s.sendSCSIResponse(req, illegalRequest(
					ASCInvalidFieldInCDB, 0x00,
					"EDTL does not match CDB transfer length"))
			}
		}
		if result, ok := s.validateWriteTransferLimits(req, edtl); !ok {
			return s.sendSCSIResponse(req, result)
		}
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

	result := s.handler.HandleCommand(ctx, cdb, dataOut)

	if len(result.Data) > 0 {
		return s.sendDataInWithStatus(req, result)
	}
	return s.sendSCSIResponse(req, result)
}

// cdbExpectedWriteBytes computes the data-transfer size in bytes
// a WRITE CDB advertises. For opcodes we don't handle as writes
// (or don't recognize yet), returns (_, false) so the caller
// falls through to the SCSI handler's own per-opcode validation.
//
// Kept as a narrow lookup rather than reaching into scsi.go
// because handleSCSICmd doesn't need to build a SCSIResult — it
// just needs to bound the pre-allocation. Expand as new write
// opcodes are added to scsi.go.
func cdbExpectedWriteBytes(cdb [16]byte, blockSize uint32) (uint64, bool) {
	switch cdb[0] {
	case ScsiWrite10:
		transferLen := uint32(cdb[7])<<8 | uint32(cdb[8])
		return uint64(transferLen) * uint64(blockSize), true
	case ScsiWrite16:
		// 32-bit transferLen in CDB bytes 10-13 (Batch 10.5).
		transferLen := uint32(cdb[10])<<24 | uint32(cdb[11])<<16 |
			uint32(cdb[12])<<8 | uint32(cdb[13])
		return uint64(transferLen) * uint64(blockSize), true
	}
	return 0, false
}

func (s *Session) validateWriteTransferLimits(req *PDU, edtl uint32) (SCSIResult, bool) {
	if uint64(edtl) > uint64(MaxDataSegmentLength) {
		return illegalRequest(
			ASCInvalidFieldInCDB, 0x00,
			"EDTL exceeds target write buffer limit"), false
	}
	if firstBurst := s.negResult.FirstBurstLength; firstBurst > 0 &&
		uint64(len(req.DataSegment)) > uint64(firstBurst) {
		return illegalRequest(
			ASCInvalidFieldInCDB, 0x00,
			"immediate data exceeds negotiated FirstBurstLength"), false
	}
	return SCSIResult{Status: StatusGood}, true
}

// collectWriteData assembles the write payload from immediate
// data (carried in the SCSI-Cmd) + any Data-Out PDUs solicited
// via R2T. Returns the full edtl-sized buffer on success.
//
// Large writes are collected as one or more R2T data sequences.
// Each R2T's DesiredDataLength is capped by the negotiated
// MaxBurstLength; the total WRITE EDTL may be larger.
func (s *Session) collectWriteData(req *PDU, edtl uint32) ([]byte, error) {
	collector := newDataOutCollector(edtl)
	if len(req.DataSegment) > 0 {
		if err := collector.addImmediate(req.DataSegment); err != nil {
			return nil, err
		}
	}
	if collector.done() {
		return collector.data(), nil
	}

	maxBurst := uint32(s.negResult.MaxBurstLength)
	if maxBurst == 0 {
		maxBurst = edtl
	}

	var r2tsn uint32
	for !collector.done() {
		received := collector.receivedBytes()
		remaining := collector.remaining()
		desired := remaining
		if desired > maxBurst {
			desired = maxBurst
		}
		burstEnd := received + desired
		ttt := uint32(1) + r2tsn

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
		r2t.SetR2TSN(r2tsn)
		r2t.SetBufferOffset(received)
		r2t.SetDesiredDataLength(desired)
		if err := WritePDU(s.conn, r2t); err != nil {
			return nil, fmt.Errorf("send R2T: %w", err)
		}

		if s.dataOutTimeout > 0 {
			if err := s.conn.SetReadDeadline(time.Now().Add(s.dataOutTimeout)); err != nil {
				return nil, fmt.Errorf("set Data-Out deadline: %w", err)
			}
			defer s.conn.SetReadDeadline(time.Time{})
		}

		collector.beginR2T()
		for collector.receivedBytes() < burstEnd {
			pdu, err := ReadPDU(s.conn)
			if err != nil {
				return nil, fmt.Errorf("read Data-Out: %w", err)
			}
			if pdu.Opcode() != OpSCSIDataOut {
				if len(s.pending) >= maxPendingQueue {
					return nil, fmt.Errorf("pending queue overflow (%d PDUs)", maxPendingQueue)
				}
				s.pending = append(s.pending, pdu)
				continue
			}
			if err := collector.addDataOut(pdu, ttt, burstEnd); err != nil {
				return nil, err
			}
		}
		r2tsn++
	}
	return collector.data(), nil
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
		// BUG-005 fix (2026-04-22): do NOT close the Backend here.
		// The Provider owns Backend lifecycle (DurableProvider
		// caches one Backend per volumeID across sessions). The
		// session's reference is a borrowed handle; session end
		// should release the TCP conn + session-local state
		// only. See sw-block/design/bugs/005_backend_close_cross_session.md.
		_ = s.conn.Close()
		s.state = SessionClosed
	}
}
