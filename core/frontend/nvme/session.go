package nvme

// NVMe/TCP session — faithful V2 port of the RX/TX split pattern.
// Reverts the pragmatic per-CID pendingWrites evolution from the
// initial BUG-001 fix (commit eae028a); see BUG-001 §13 for why
// that evolution broke kernel compatibility on m01.
//
// ## What V2 does (port verbatim, with V3 module-path adaptation)
//
// Phase 1 (synchronous): ICReq / ICResp.
//
// Phase 2 (RX/TX split):
//   - rxLoop goroutine — sole reader of s.r after IC. Per iteration:
//       * drain pendingCapsules (Cmds buffered during a prior R2T
//         data collection window) BEFORE reading next wire PDU
//       * read next CommonHeader
//       * CapsuleCmd → parseCapsule + dispatchFromRx
//       * H2CData (outside a collection window) → protocol error
//       * other → protocol error
//   - dispatchFromRx routes by QID:
//       * Fabric / Admin (qid=0): synchronous, handler enqueues
//         response via respCh
//       * IO Write (qid>0, no inline data): collectR2TData inline
//         in rxLoop — enqueues R2T, waits r2tDone, then
//         recvH2CData collects all H2CData for this cmd. If
//         interleaved CapsuleCmds arrive during recvH2CData,
//         bufferInterleaved stashes them in pendingCapsules; rxLoop
//         drains them on its next iteration.
//       * IO Read / Flush / other: handler goroutine, enqueue
//         response via respCh.
//   - txLoop goroutine — sole writer of s.w after IC. Drains
//     respCh, writes CapsuleResp / C2HData / R2T PDUs.
//
// Invariant this preserves and why it matters:
//   AT MOST ONE R2T IS OUTSTANDING AT A TIME on a given session.
//   collectR2TData blocks rxLoop until the current Write's data
//   has been fully received. pipelined Cmds that arrive mid-collect
//   go into pendingCapsules and are processed in order after
//   collectR2TData returns. Linux NVMe-TCP kernel driver (and the
//   NVMe-TCP spec req_state machine) is built around this model:
//   kernel sends H2CData for cmd-N in response to R2T-N and
//   doesn't pre-emit R2T-(N+1) until cmd-N's data is done.
//
// The initial BUG-001 fix (eae028a) deviated from this by spawning
// a handler goroutine per Write and emitting all R2Ts async. That
// passed a Go-client conformance test QA wrote that expected all
// R2Ts before any H2CData, but the real Linux kernel state machine
// does NOT pipeline data across R2Ts — it saw two R2Ts for what it
// tracked as overlapping requests and errored with "r2t len X
// exceeded data len X (Y sent)". See BUG-001 §13 post-mortem.
//
// ## BUG-002 (fcDisconnect) + BUG-004 (SQHD wrap)
//
// These orthogonal fixes are retained — they don't interact with
// the concurrency model:
//   - handleDisconnect: enqueues success CapsuleResp, returns
//     errDisconnect sentinel; serve() maps it to nil.
//   - Connect parses CDW11 low-16 (SQSIZE zero-based +1) + bit 18
//     (CATTR flow-ctrl-off). advanceSQHD wraps at queueSize;
//     writeResponse reports 0xFFFF when flowCtlOff.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// errDisconnect is a sentinel returned by handleDisconnect (BUG-002)
// so serve() can exit gracefully after enqueueing the success
// response.
var errDisconnect = errors.New("disconnect")

// Request represents an in-flight NVMe command being processed.
type Request struct {
	capsule CapsuleCommand
	payload []byte // inline/R2T-collected data for Write
	resp    CapsuleResponse
	c2hData []byte // non-nil for Read/Identify responses
}

// response represents a pending response to be written by txLoop.
// Either a standard CapsuleResp (with optional c2hData) or an R2T
// PDU. r2tDone is closed by txLoop after R2T is flushed to wire
// so collectR2TData (in rxLoop) can proceed to recv H2CData.
type response struct {
	resp    CapsuleResponse
	c2hData []byte

	r2t     *R2THeader
	r2tDone chan struct{}
}

// Session carries per-connection NVMe/TCP state.
type Session struct {
	conn    net.Conn
	r       *Reader
	w       *Writer
	handler *IOHandler
	logger  Logger

	target *Target

	expectedSubNQN string

	connected atomic.Bool

	qid        uint16
	qidSet     bool
	ctrl       *adminController
	queueSize  uint16 // BUG-004
	flowCtlOff bool   // BUG-004

	// BUG-004: SQHD tracked atomically; rxLoop advances on parse,
	// txLoop reads at response-stamp time.
	sqhd atomic.Uint32

	respCh chan *response
	done   chan struct{}
	cmdWg  sync.WaitGroup

	// pendingCapsules holds CapsuleCmds that arrived during a
	// prior R2T data collection window. rxLoop-local; no mutex.
	pendingCapsules []*Request

	closed atomic.Bool
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

	s.respCh = make(chan *response, 128)
	s.done = make(chan struct{})

	txErrCh := make(chan error, 1)
	go func() {
		txErrCh <- s.txLoop()
	}()

	rxErr := s.rxLoop(ctx)

	s.cmdWg.Wait()
	close(s.done)
	txErr := <-txErrCh

	if errors.Is(rxErr, errDisconnect) {
		rxErr = nil
	}
	if rxErr != nil {
		return rxErr
	}
	return txErr
}

// ---------- Phase 1: IC handshake ----------

func (s *Session) handleICReq() error {
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
	resp := ICResponse{
		PDUFormatVersion: req.PDUFormatVersion,
		PDUDataAlignment: 0,
		PDUDataDigest:    0,
		MaxH2CDataLength: 0x8000,
	}
	return s.w.SendHeaderOnly(pduICResp, &resp, icBodySize)
}

// ---------- Phase 2: rxLoop ----------

func (s *Session) rxLoop(ctx context.Context) error {
	for {
		if s.closed.Load() {
			return nil
		}
		// Drain capsules buffered during a prior R2T data
		// collection window before reading the next wire PDU.
		// This is the V2 pipelining invariant: cmds pipelined
		// by kernel during our H2CData collection get processed
		// here in order.
		for len(s.pendingCapsules) > 0 {
			req := s.pendingCapsules[0]
			s.pendingCapsules = s.pendingCapsules[1:]
			if err := s.dispatchFromRx(ctx, req); err != nil {
				return err
			}
		}

		ch, err := s.r.Dequeue()
		if err != nil {
			if err == io.EOF || s.closed.Load() {
				return nil
			}
			return fmt.Errorf("read header: %w", err)
		}
		switch ch.Type {
		case pduCapsuleCmd:
			req, err := s.parseCapsule()
			if err != nil {
				return fmt.Errorf("capsule: %w", err)
			}
			if err := s.dispatchFromRx(ctx, req); err != nil {
				return err
			}
		case pduH2CData:
			return fmt.Errorf("unexpected H2CData PDU outside R2T flow")
		default:
			return fmt.Errorf("unsupported PDU type 0x%x", ch.Type)
		}
	}
}

func (s *Session) parseCapsule() (*Request, error) {
	var capsule CapsuleCommand
	if err := s.r.Receive(&capsule); err != nil {
		return nil, err
	}
	var payload []byte
	if n := s.r.Length(); n > 0 {
		payload = make([]byte, n)
		if err := s.r.ReceiveData(payload); err != nil {
			return nil, err
		}
	}
	req := &Request{capsule: capsule, payload: payload}
	req.resp.CID = capsule.CID
	return req, nil
}

// advanceSQHD (BUG-004) wraps SQHD at queueSize.
func (s *Session) advanceSQHD() {
	cur := uint16(s.sqhd.Load())
	nxt := cur + 1
	if s.queueSize > 0 && nxt >= s.queueSize {
		nxt = 0
	}
	s.sqhd.Store(uint32(nxt))
}

// dispatchFromRx routes a parsed capsule.
//   - Fabric: synchronous.
//   - Admin (qid=0): synchronous.
//   - IO Write: collectR2TData inline in rxLoop (populates
//     pendingCapsules via bufferInterleaved if kernel pipelines),
//     THEN spawn handler goroutine for backend.Write.
//   - IO Read/Flush/other: handler goroutine.
func (s *Session) dispatchFromRx(ctx context.Context, req *Request) error {
	cmd := &req.capsule
	if cmd.OpCode == adminFabric {
		return s.handleFabric(req)
	}
	if !s.connected.Load() {
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidOpcode, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}

	s.advanceSQHD()

	if s.qid == 0 {
		return s.adminDispatch(ctx, req)
	}

	// IO queue. For Write without inline data, collect R2T data
	// inline FIRST (this is where pendingCapsules gets populated
	// via bufferInterleaved during recvH2CData), THEN dispatch.
	if cmd.OpCode == ioWrite && len(req.payload) == 0 {
		if err := s.collectR2TData(req); err != nil {
			return err
		}
	}

	s.cmdWg.Add(1)
	go func() {
		defer s.cmdWg.Done()
		s.dispatchIO(ctx, req)
	}()
	return nil
}

// collectR2TData (called from rxLoop only) enqueues R2T via
// respCh, waits for txLoop to flush it (r2tDone), then drives
// recvH2CData to collect all H2CData bytes for this Write. This
// blocks rxLoop; pipelined Cmds arriving during this window are
// absorbed into pendingCapsules by bufferInterleaved.
func (s *Session) collectR2TData(req *Request) error {
	totalBytes := req.capsule.LbaLength() * s.handler.BlockSize()

	r2tDone := make(chan struct{})
	select {
	case s.respCh <- &response{
		r2t: &R2THeader{
			CCCID: req.capsule.CID,
			TAG:   1,
			DATAO: 0,
			DATAL: totalBytes,
		},
		r2tDone: r2tDone,
	}:
	case <-s.done:
		return nil
	}

	select {
	case <-r2tDone:
	case <-s.done:
		return nil
	}

	data, err := s.recvH2CData(totalBytes)
	if err != nil {
		return err
	}
	req.payload = data
	return nil
}

// recvH2CData reads H2CData PDU(s) off the wire until totalBytes
// have been collected. Called from rxLoop via collectR2TData.
// If a CapsuleCmd PDU arrives during this window (kernel
// pipelined the next cmd on this queue), bufferInterleaved reads
// it fully and appends to pendingCapsules; rxLoop drains them on
// its next iteration.
func (s *Session) recvH2CData(totalBytes uint32) ([]byte, error) {
	buf := make([]byte, totalBytes)
	received := uint32(0)
	for received < totalBytes {
		ch, err := s.r.Dequeue()
		if err != nil {
			return nil, fmt.Errorf("recvH2CData: read header: %w", err)
		}
		if ch.Type == pduCapsuleCmd {
			if err := s.bufferInterleaved(); err != nil {
				return nil, fmt.Errorf("recvH2CData: buffer interleaved capsule: %w", err)
			}
			continue
		}
		if ch.Type != pduH2CData {
			return nil, fmt.Errorf("recvH2CData: expected H2CData (0x6), got 0x%x", ch.Type)
		}
		var h2c H2CDataHeader
		if err := s.r.Receive(&h2c); err != nil {
			return nil, fmt.Errorf("recvH2CData: receive header: %w", err)
		}
		dataLen := s.r.Length()
		if dataLen == 0 {
			return nil, fmt.Errorf("recvH2CData: H2CData PDU has no payload")
		}
		if h2c.DATAO+dataLen > totalBytes {
			return nil, fmt.Errorf("recvH2CData: data exceeds expected size (%d+%d > %d)",
				h2c.DATAO, dataLen, totalBytes)
		}
		if err := s.r.ReceiveData(buf[h2c.DATAO : h2c.DATAO+dataLen]); err != nil {
			return nil, fmt.Errorf("recvH2CData: receive data: %w", err)
		}
		received += dataLen
	}
	return buf, nil
}

// bufferInterleaved reads a complete CapsuleCmd PDU that arrived
// during H2CData collection, appends it to pendingCapsules for
// rxLoop to dispatch after collectR2TData returns.
func (s *Session) bufferInterleaved() error {
	req, err := s.parseCapsule()
	if err != nil {
		return err
	}
	s.pendingCapsules = append(s.pendingCapsules, req)
	return nil
}

// adminDispatch handles admin-queue opcodes synchronously.
func (s *Session) adminDispatch(ctx context.Context, req *Request) error {
	cmd := &req.capsule
	switch cmd.OpCode {
	case adminIdentify:
		return s.handleAdminIdentify(req)
	case adminSetFeatures:
		return s.handleSetFeatures(req)
	case adminGetFeatures:
		return s.handleGetFeatures(req)
	case adminKeepAlive:
		return s.handleKeepAlive(req)
	case adminAsyncEvent:
		return s.handleAsyncEventRequest(req)
	default:
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidOpcode, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
}

// dispatchIO handles IO-queue opcodes (goroutine). Write's data
// has already been collected by collectR2TData in rxLoop.
func (s *Session) dispatchIO(ctx context.Context, req *Request) {
	cmd := &req.capsule
	switch cmd.OpCode {
	case ioRead:
		s.handleRead(ctx, req)
	case ioWrite:
		s.handleWrite(ctx, req)
	case ioFlush:
		s.enqueueResponse(&response{resp: req.resp})
	default:
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidOpcode, true)
		s.enqueueResponse(&response{resp: req.resp})
	}
}

// ---------- Phase 2: txLoop ----------

func (s *Session) txLoop() error {
	var firstErr error
	for {
		select {
		case resp := <-s.respCh:
			if firstErr != nil {
				completeWaiters(resp)
				continue
			}
			if err := s.writeResponse(resp); err != nil {
				firstErr = err
				s.conn.Close()
			}
		case <-s.done:
			for {
				select {
				case resp := <-s.respCh:
					if firstErr == nil {
						if err := s.writeResponse(resp); err != nil {
							firstErr = err
						}
					} else {
						completeWaiters(resp)
					}
				default:
					return firstErr
				}
			}
		}
	}
}

// completeWaiters unblocks r2tDone on a discarded response.
func completeWaiters(resp *response) {
	if resp.r2tDone != nil {
		close(resp.r2tDone)
	}
}

func (s *Session) writeResponse(resp *response) error {
	if resp.r2t != nil {
		err := s.w.SendHeaderOnly(pduR2T, resp.r2t, r2tHdrSize)
		if resp.r2tDone != nil {
			close(resp.r2tDone)
		}
		return err
	}

	if s.flowCtlOff {
		resp.resp.SQHD = 0xFFFF
	} else {
		resp.resp.SQHD = uint16(s.sqhd.Load())
	}

	if len(resp.c2hData) > 0 {
		hdr := C2HDataHeader{
			CCCID: resp.resp.CID,
			DATAO: 0,
			DATAL: uint32(len(resp.c2hData)),
		}
		if err := s.w.SendWithData(pduC2HData, c2hFlagLast, &hdr, c2hDataHdrSize, resp.c2hData); err != nil {
			return err
		}
	}
	return s.w.SendHeaderOnly(pduCapsuleResp, &resp.resp, capsuleRespSize)
}

func (s *Session) enqueueResponse(resp *response) {
	select {
	case s.respCh <- resp:
	case <-s.done:
	}
}

// ---------- IO handlers (run in goroutines) ----------

func (s *Session) handleRead(ctx context.Context, req *Request) {
	res := s.handler.Handle(ctx, IOCommand{
		Opcode: req.capsule.OpCode,
		NSID:   req.capsule.NSID,
		SLBA:   req.capsule.Lba(),
		NLB:    req.capsule.LbaLength(),
	})
	if res.AsError() != nil {
		req.resp.Status = MakeStatusField(res.SCT, res.SC, true)
		s.enqueueResponse(&response{resp: req.resp})
		return
	}
	s.enqueueResponse(&response{resp: req.resp, c2hData: res.Data})
}

func (s *Session) handleWrite(ctx context.Context, req *Request) {
	res := s.handler.Handle(ctx, IOCommand{
		Opcode: req.capsule.OpCode,
		NSID:   req.capsule.NSID,
		SLBA:   req.capsule.Lba(),
		NLB:    req.capsule.LbaLength(),
		Data:   req.payload,
	})
	if res.AsError() != nil {
		req.resp.Status = MakeStatusField(res.SCT, res.SC, true)
	}
	s.enqueueResponse(&response{resp: req.resp})
}

// ---------- Fabric ----------

func (s *Session) handleFabric(req *Request) error {
	cmd := &req.capsule
	switch cmd.FCType {
	case fcPropertyGet:
		return s.handlePropertyGet(req)
	case fcPropertySet:
		return s.handlePropertySet(req)
	case fcDisconnect:
		return s.handleDisconnect(req) // BUG-002
	}
	if cmd.FCType != fcConnect {
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidOpcode, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	if s.qidSet {
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidOpcode, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	if len(req.payload) < connectDataSize {
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	var cd ConnectData
	cd.Unmarshal(req.payload[:connectDataSize])

	qid := uint16(cmd.D10 >> 16)
	// BUG-004: SQSIZE zero-based low-16; CATTR bit 2 at D11 bit 18.
	queueSize := uint16(cmd.D11&0xFFFF) + 1
	cattr := uint8(cmd.D11 >> 16)
	flowCtlOff := (cattr & 0x04) != 0

	if s.expectedSubNQN != "" && cd.SubNQN != s.expectedSubNQN {
		if s.logger != nil {
			s.logger.Printf("nvme: Connect REJECTED (wrong subsys) host=%q wanted=%q got=%q qid=%d",
				cd.HostNQN, s.expectedSubNQN, cd.SubNQN, qid)
		}
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCConnectInvalidParameters, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}

	if qid == 0 {
		return s.handleAdminConnect(req, &cd, qid, queueSize, flowCtlOff)
	}
	return s.handleIOConnect(req, &cd, qid, queueSize, flowCtlOff)
}

func (s *Session) handleAdminConnect(req *Request, cd *ConnectData, qid, queueSize uint16, flowCtlOff bool) error {
	if cd.CNTLID != 0xFFFF && cd.CNTLID != 0x0000 {
		if s.logger != nil {
			s.logger.Printf("nvme: admin Connect with preset CNTLID=0x%04x rejected (must be 0xFFFF)",
				cd.CNTLID)
		}
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCConnectInvalidParameters, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	var ctrl *adminController
	if s.target != nil {
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
	s.queueSize = queueSize
	s.flowCtlOff = flowCtlOff
	// BUG-003: plumb KATO from Connect CDW12 into the controller.
	// V2 fabric.go:41 extracts this at Connect; V3 previously only
	// accepted KATO via Set Features 0x0F, which typical Linux hosts
	// do not call. Store unit matches V2 (uint32 round-trip; QA
	// constraint #2 keeps this store-only, no timer).
	ctrl.setKATO(req.capsule.D12)
	s.connected.Store(true)
	if s.logger != nil {
		s.logger.Printf("nvme: admin Connect accepted host=%q subsys=%q cntlid=%d qsize=%d kato=%d",
			cd.HostNQN, cd.SubNQN, ctrl.cntlID, queueSize, req.capsule.D12)
	}
	s.advanceSQHD()
	req.resp.DW0 = uint32(ctrl.cntlID)
	s.enqueueResponse(&response{resp: req.resp})
	return nil
}

func (s *Session) handleIOConnect(req *Request, cd *ConnectData, qid, queueSize uint16, flowCtlOff bool) error {
	var ctrl *adminController
	if s.target != nil {
		ctrl = s.target.lookupAdminController(cd.CNTLID)
	}
	if ctrl == nil {
		if s.logger != nil {
			s.logger.Printf("nvme: IO Connect qid=%d CNTLID=%d not found", qid, cd.CNTLID)
		}
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCConnectInvalidHost, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	if cd.SubNQN != ctrl.subNQN {
		if s.logger != nil {
			s.logger.Printf("nvme: IO Connect CNTLID=%d SubNQN mismatch: got %q want %q",
				cd.CNTLID, cd.SubNQN, ctrl.subNQN)
		}
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCConnectInvalidHost, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	s.qid = qid
	s.qidSet = true
	s.ctrl = ctrl
	s.queueSize = queueSize
	s.flowCtlOff = flowCtlOff
	s.connected.Store(true)
	if s.logger != nil {
		s.logger.Printf("nvme: IO Connect accepted host=%q subsys=%q cntlid=%d qid=%d qsize=%d",
			cd.HostNQN, cd.SubNQN, ctrl.cntlID, qid, queueSize)
	}
	s.advanceSQHD()
	req.resp.DW0 = uint32(ctrl.cntlID)
	s.enqueueResponse(&response{resp: req.resp})
	return nil
}

// handleDisconnect (BUG-002).
func (s *Session) handleDisconnect(req *Request) error {
	if s.logger != nil && s.ctrl != nil {
		s.logger.Printf("nvme: Disconnect cntlid=%d qid=%d", s.ctrl.cntlID, s.qid)
	}
	s.enqueueResponse(&response{resp: req.resp})
	return errDisconnect
}

// ---------- Shutdown ----------

func (s *Session) close() {
	if s.closed.CompareAndSwap(false, true) {
		if s.qid == 0 && s.ctrl != nil && s.target != nil {
			s.target.releaseAdminController(s.ctrl.cntlID)
		}
		_ = s.conn.Close()
	}
}
