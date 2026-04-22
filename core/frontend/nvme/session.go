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

	connected atomic.Bool // true after Fabric Connect
	closed    atomic.Bool

	// sqhd advances per CapsuleResponse (NVMe submission queue
	// head pointer for queue depth tracking — 0 in T2 scope
	// but bumped so the wire field looks live).
	sqhd uint16
}

// Logger is a tiny indirection for log injection.
type Logger interface {
	Printf(format string, args ...interface{})
}

func newSession(conn net.Conn, h *IOHandler, lg Logger) *Session {
	return &Session{
		conn:    conn,
		r:       NewReader(conn),
		w:       NewWriter(conn),
		handler: h,
		logger:  lg,
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

	if cmd.OpCode == adminFabric {
		return s.handleFabric(&cmd, inline)
	}
	if !s.connected.Load() {
		return s.replyStatus(&cmd, MakeStatusField(SCTGeneric, SCInvalidOpcode, true))
	}

	switch cmd.OpCode {
	case ioRead:
		return s.handleRead(ctx, &cmd)
	case ioWrite:
		return s.handleWrite(ctx, &cmd)
	case ioFlush:
		return s.replyStatus(&cmd, 0) // Success
	default:
		return s.replyStatus(&cmd, MakeStatusField(SCTGeneric, SCInvalidOpcode, true))
	}
}

func (s *Session) handleFabric(cmd *CapsuleCommand, inline []byte) error {
	if cmd.FCType != fcConnect {
		return s.replyStatus(cmd, MakeStatusField(SCTGeneric, SCInvalidOpcode, true))
	}
	// Validate inline ConnectData length but accept any HostNQN /
	// SubNQN — auth lands with T8.
	if len(inline) < connectDataSize {
		return s.replyStatus(cmd, MakeStatusField(SCTGeneric, SCInvalidField, true))
	}
	var cd ConnectData
	cd.Unmarshal(inline[:connectDataSize])
	s.connected.Store(true)
	if s.logger != nil {
		s.logger.Printf("nvme: Connect accepted host=%q subsys=%q", cd.HostNQN, cd.SubNQN)
	}
	// Reply with success; DW0/DW1 carry the assigned controller ID
	// in the low 16 bits of DW0 (we use 1).
	return s.sendCapsuleResp(cmd.CID, 0x00000001, 0, 0)
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
		_ = s.conn.Close()
	}
}
