package nvme

// NVMe/TCP session — RX/TX split with per-CID pendingWrite routing.
// BUG-001 / BUG-002 / BUG-004 co-fix (2026-04-22).
//
// ## Relationship to V2 (port discipline note)
//
// V2 `weed/storage/blockvol/nvme/controller.go` uses a two-loop
// pattern (rxLoop + txLoop + respCh) that we port faithfully. V2
// additionally uses **inline R2T data collection inside rxLoop**
// (collectR2TData → recvH2CData → bufferInterleaved). That inline
// pattern works against real Linux NVMe/TCP hosts because the
// kernel always ships H2CData immediately after receiving an R2T
// — so rxLoop never blocks meaningfully inside recvH2CData.
//
// It does NOT work against spec-legal hosts (or QA's conformance
// tests) that pipeline multiple CapsuleCmds BEFORE sending any
// H2CData. V2's inline recvH2CData blocks waiting for H2CData-1
// while cmd-2 sits buffered in pendingCapsules, so R2T-2 is never
// emitted → client never sends H2CData → deadlock.
//
// V3 evolves V2's pattern by moving H2CData collection OUT of
// rxLoop and into per-CID state:
//   - rxLoop reads PDUs strictly by type. No nested read loops.
//   - CapsuleCmd for Write → register a pendingWrite keyed by CID
//     + enqueue R2T + spawn handler goroutine waiting on the
//     pendingWrite's done channel. rxLoop returns immediately to
//     read the next PDU (cmd-N+1 or H2CData-N).
//   - H2CData → route bytes into the matching pendingWrite; when
//     full, close its done channel.
//   - Handler goroutine wakes, calls backend.Write, enqueues
//     CapsuleResp.
//
// Structurally this is still RX/TX split with respCh; the only
// divergence from V2 is that the pipelining invariant now lives
// in a pendingWrites map rather than in a sync recvH2CData call
// with a pendingCapsules fallback. The difference is ~60 LOC.
// See the BUG-001 fix commit message for why this was a required
// evolution rather than an ambitious rewrite.
//
// ## BUG-002 (fcDisconnect)
//
// handleFabric now routes fcDisconnect → handleDisconnect, which
// enqueues a success CapsuleResp and returns the errDisconnect
// sentinel. serve() catches the sentinel and returns nil.
//
// ## BUG-004 (SQHD wrap at queueSize)
//
// Session.queueSize is parsed from Connect CDW11 low-16 (zero-
// based + 1). advanceSQHD wraps at queueSize. CATTR bit 2
// (flowCtlOff) from CDW11 bit 18 reports SQHD=0xFFFF when set.

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
// Two variants:
//   - Standard: resp (+ optional c2hData for Read/Identify)
//   - R2T: r2t non-nil; r2tSent signals when R2T has been flushed
//     to the wire (currently unused by the per-CID design but
//     kept for symmetry with V2's r2tDone shape — future uses).
type response struct {
	resp    CapsuleResponse
	c2hData []byte

	r2t     *R2THeader
	r2tSent chan struct{}
}

// pendingWrite tracks an in-flight IO Write between R2T emission
// and H2CData reception. rxLoop's H2CData path writes into buf;
// when received == totalBytes, rxLoop closes done. The handler
// goroutine blocks on done before calling backend.Write.
type pendingWrite struct {
	cid        uint16
	tag        uint16
	totalBytes uint32
	buf        []byte
	received   uint32
	done       chan struct{}
	// protocolErr is set by rxLoop if the H2CData stream violates
	// framing (out-of-range DATAO, under-size PDU, etc.). Handler
	// goroutine checks it before calling backend.
	protocolErr atomic.Value // error
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
	queueSize  uint16 // BUG-004: parsed from Connect
	flowCtlOff bool   // BUG-004: CATTR bit 2

	// BUG-004: SQHD tracked atomically. rxLoop advances on each
	// capsule parse; txLoop reads at response-stamp time.
	sqhd atomic.Uint32 // lower 16 bits are live

	// RX/TX split state — created in serve() after IC.
	respCh chan *response
	done   chan struct{}
	cmdWg  sync.WaitGroup

	// Per-CID pending Writes. Guarded by pwMu. rxLoop registers
	// on Write-cmd dispatch, unregisters after done; H2CData path
	// looks up by CCCID.
	pwMu          sync.Mutex
	pendingWrites map[uint16]*pendingWrite

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
		pendingWrites:  make(map[uint16]*pendingWrite),
	}
}

func (s *Session) serve(ctx context.Context) error {
	defer s.close()

	// Phase 1: ICReq / ICResp synchronous.
	if err := s.handleICReq(); err != nil {
		return fmt.Errorf("ICReq: %w", err)
	}

	// Phase 2: RX/TX split.
	s.respCh = make(chan *response, 128)
	s.done = make(chan struct{})

	txErrCh := make(chan error, 1)
	go func() {
		txErrCh <- s.txLoop()
	}()

	rxErr := s.rxLoop(ctx)

	// Unblock any handlers still waiting for H2CData so cmdWg
	// can drain.
	s.abortPendingWrites()
	s.cmdWg.Wait()

	close(s.done)
	txErr := <-txErrCh

	// BUG-002: graceful Disconnect is not an error.
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
		MaxH2CDataLength: 0x8000, // 32 KiB
	}
	return s.w.SendHeaderOnly(pduICResp, &resp, icBodySize)
}

// ---------- Phase 2: rxLoop ----------

func (s *Session) rxLoop(ctx context.Context) error {
	for {
		if s.closed.Load() {
			return nil
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
			if err := s.routeH2CData(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported PDU type 0x%x", ch.Type)
		}
	}
}

// parseCapsule reads a CapsuleCmd PDU + optional inline data.
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
	req := &Request{
		capsule: capsule,
		payload: payload,
	}
	req.resp.CID = capsule.CID
	return req, nil
}

// routeH2CData reads an H2CData PDU and routes its bytes to the
// matching pendingWrite by CCCID. Closes done when the write is
// fully received. Framing violations are recorded on the
// pendingWrite's protocolErr and the handler goroutine surfaces
// them as a status-error CapsuleResp.
func (s *Session) routeH2CData() error {
	var h H2CDataHeader
	if err := s.r.Receive(&h); err != nil {
		return fmt.Errorf("H2CData: receive header: %w", err)
	}
	dataLen := s.r.Length()
	pw := s.lookupPending(h.CCCID)
	if pw == nil {
		// Unknown CCCID: drain data to keep the stream framed,
		// then fail the session (protocol violation).
		if dataLen > 0 {
			drain := make([]byte, dataLen)
			_ = s.r.ReceiveData(drain)
		}
		return fmt.Errorf("H2CData for unknown CCCID=%d", h.CCCID)
	}
	if dataLen == 0 {
		err := fmt.Errorf("H2CData: zero-length payload")
		pw.protocolErr.Store(err)
		close(pw.done)
		return err
	}
	if h.DATAO+dataLen > pw.totalBytes {
		err := fmt.Errorf("H2CData: data exceeds expected size (%d+%d > %d)",
			h.DATAO, dataLen, pw.totalBytes)
		pw.protocolErr.Store(err)
		// Drain bytes before returning so the stream stays framed
		// for any future PDUs (txLoop will tear down shortly anyway).
		drain := make([]byte, dataLen)
		_ = s.r.ReceiveData(drain)
		close(pw.done)
		return err
	}
	if err := s.r.ReceiveData(pw.buf[h.DATAO : h.DATAO+dataLen]); err != nil {
		pw.protocolErr.Store(err)
		close(pw.done)
		return err
	}
	pw.received += dataLen
	if pw.received >= pw.totalBytes {
		close(pw.done)
	}
	return nil
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
//   - IO Write: register pendingWrite, enqueue R2T, spawn handler
//     goroutine (waits on pendingWrite.done).
//   - IO Read/Flush/other: spawn handler goroutine.
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

	// IO queue.
	if cmd.OpCode == ioWrite {
		return s.dispatchIOWrite(ctx, req)
	}
	s.cmdWg.Add(1)
	go func() {
		defer s.cmdWg.Done()
		s.dispatchIO(ctx, req)
	}()
	return nil
}

// dispatchIOWrite registers a pendingWrite, enqueues R2T, and
// spawns a handler goroutine that waits for H2CData completion.
func (s *Session) dispatchIOWrite(ctx context.Context, req *Request) error {
	cmd := &req.capsule
	totalBytes := cmd.LbaLength() * s.handler.BlockSize()
	pw := &pendingWrite{
		cid:        cmd.CID,
		tag:        1,
		totalBytes: totalBytes,
		buf:        make([]byte, totalBytes),
		done:       make(chan struct{}),
	}
	if !s.registerPending(pw) {
		// Duplicate CID on a pipelined Write is a protocol
		// violation; respond InvalidField and skip.
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	s.enqueueResponse(&response{
		r2t: &R2THeader{
			CCCID: cmd.CID,
			TAG:   pw.tag,
			DATAO: 0,
			DATAL: totalBytes,
		},
	})
	s.cmdWg.Add(1)
	go func() {
		defer s.cmdWg.Done()
		defer s.unregisterPending(cmd.CID)
		select {
		case <-pw.done:
		case <-s.done:
			return
		}
		if err := pw.protocolErr.Load(); err != nil {
			req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
			s.enqueueResponse(&response{resp: req.resp})
			return
		}
		req.payload = pw.buf
		s.handleWrite(ctx, req)
	}()
	return nil
}

// registerPending adds pw to the pendingWrites map if no existing
// entry has the same CID. Returns true on success, false on CID
// collision (protocol violation — tag reuse within outstanding).
func (s *Session) registerPending(pw *pendingWrite) bool {
	s.pwMu.Lock()
	defer s.pwMu.Unlock()
	if _, exists := s.pendingWrites[pw.cid]; exists {
		return false
	}
	s.pendingWrites[pw.cid] = pw
	return true
}

func (s *Session) unregisterPending(cid uint16) {
	s.pwMu.Lock()
	delete(s.pendingWrites, cid)
	s.pwMu.Unlock()
}

func (s *Session) lookupPending(cid uint16) *pendingWrite {
	s.pwMu.Lock()
	defer s.pwMu.Unlock()
	return s.pendingWrites[cid]
}

// abortPendingWrites closes every pending write's done channel
// with a protocol-error sentinel, so handler goroutines blocked
// on the channel can exit. Called when rxLoop returns.
func (s *Session) abortPendingWrites() {
	s.pwMu.Lock()
	defer s.pwMu.Unlock()
	for _, pw := range s.pendingWrites {
		if pw.protocolErr.Load() == nil {
			pw.protocolErr.Store(errors.New("session closed mid-write"))
		}
		// close(done) may panic if already closed; guard with a
		// recover pattern via a select-on-closed idiom.
		select {
		case <-pw.done:
			// already closed
		default:
			close(pw.done)
		}
	}
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

// dispatchIO handles IO-queue opcodes other than Write (Write
// takes the pendingWrite path above).
func (s *Session) dispatchIO(ctx context.Context, req *Request) {
	cmd := &req.capsule
	switch cmd.OpCode {
	case ioRead:
		s.handleRead(ctx, req)
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

func completeWaiters(resp *response) {
	if resp.r2tSent != nil {
		close(resp.r2tSent)
	}
}

func (s *Session) writeResponse(resp *response) error {
	if resp.r2t != nil {
		err := s.w.SendHeaderOnly(pduR2T, resp.r2t, r2tHdrSize)
		if resp.r2tSent != nil {
			close(resp.r2tSent)
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
		// BUG-002.
		return s.handleDisconnect(req)
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
	// BUG-004: parse queueSize (CDW11 low-16 zero-based) + CATTR.
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
	s.connected.Store(true)
	if s.logger != nil {
		s.logger.Printf("nvme: admin Connect accepted host=%q subsys=%q cntlid=%d qsize=%d",
			cd.HostNQN, cd.SubNQN, ctrl.cntlID, queueSize)
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

// handleDisconnect (BUG-002) enqueues success then returns the
// errDisconnect sentinel.
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
