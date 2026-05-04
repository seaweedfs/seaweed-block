package nvme

// Admin-queue opcode handlers beyond Identify — Batch 11b.
//
// Scope (port plan §7 Batch 11b):
//   - Set Features  (opcode 0x09)
//       FID 0x07  Number of Queues       — grants NSQR/NCQR
//       FID 0x0F  Keep Alive Timer       — store only (no timer)
//   - Get Features  (opcode 0x0A)
//       FID 0x07 / 0x0F                  — round-trip what was set
//   - Keep Alive    (opcode 0x18)        — unconditional Success
//   - Async Event Request (opcode 0x0C)  — non-blocking park
//
// BUG-001 fix (2026-04-22): handlers now take *Request and enqueue
// responses through respCh instead of writing to s.w directly.
// This keeps the single-writer invariant (txLoop owns s.w). The
// QA constraints are unchanged:
//
//   #1 AER non-blocking park: handler returns without enqueueing
//      a response when the slot is free. Slot full → enqueue an
//      AsyncEventRequestLimitExceeded response.
//   #2 KATO store-only, no timer.
//   #3 CC.EN→CSTS.RDY synchronous in controller.go setCC.
//   #4 OAES stays zero.

// Set Features / Get Features feature IDs (NVMe 1.3 §5.21).
const (
	featNumberOfQueues uint8 = 0x07
	featKeepAliveTimer uint8 = 0x0F
)

// handleSetFeatures services admin opcode 0x09.
//
// Wire layout: CDW10 low 8 bits = Feature ID, bit 31 = SV
// (save across reset — ignored). CDW11 carries the feature-
// specific value.
func (s *Session) handleSetFeatures(req *Request) error {
	cmd := &req.capsule
	if s.ctrl == nil {
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	fid := uint8(cmd.D10 & 0xFF)
	switch fid {
	case featNumberOfQueues:
		// CDW11 low 16 = NSQR, high 16 = NCQR, both zero-based.
		// Cap each at 7 (T2 defensive cap).
		requested := cmd.D11
		granted := requested
		if nsqr := granted & 0xFFFF; nsqr > 7 {
			granted = (granted &^ 0xFFFF) | 7
		}
		if ncqr := (granted >> 16) & 0xFFFF; ncqr > 7 {
			granted = (granted & 0xFFFF) | (7 << 16)
		}
		s.ctrl.setNumQueues(granted)
		req.resp.DW0 = granted
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	case featKeepAliveTimer:
		// CDW11 = KATO in ms (V3 unit; Set Features round-trips
		// whatever the host wrote).
		s.ctrl.setKATO(cmd.D11)
		req.resp.DW0 = cmd.D11
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	default:
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
}

// handleGetFeatures services admin opcode 0x0A.
func (s *Session) handleGetFeatures(req *Request) error {
	cmd := &req.capsule
	if s.ctrl == nil {
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	fid := uint8(cmd.D10 & 0xFF)
	sel := uint8((cmd.D10 >> 8) & 0x07)
	if sel > 1 {
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	switch fid {
	case featNumberOfQueues:
		req.resp.DW0 = s.ctrl.getNumQueues()
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	case featKeepAliveTimer:
		req.resp.DW0 = s.ctrl.getKATO()
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	default:
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
}

// handleKeepAlive services admin opcode 0x18.
//
// Per QA constraint #2: unconditional Success. No timer reset
// (there is no timer in T2).
func (s *Session) handleKeepAlive(req *Request) error {
	s.enqueueResponse(&response{resp: req.resp})
	return nil
}

// handleAsyncEventRequest services admin opcode 0x0C.
//
// Per QA constraint #1: first AER parks (return nil, no
// enqueueResponse). Subsequent AER while slot is full → enqueue
// AsyncEventRequestLimitExceeded.
func (s *Session) handleAsyncEventRequest(req *Request) error {
	cmd := &req.capsule
	if s.ctrl == nil {
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	if !s.ctrl.tryParkAER(cmd.CID) {
		if s.logger != nil {
			s.logger.Printf("nvme: AER limit exceeded (one already pending) cid=%d", cmd.CID)
		}
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCAsyncEventRequestLimitExceeded, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	if s.logger != nil {
		s.logger.Printf("nvme: AER parked cid=%d (T2 produces no events)", cmd.CID)
	}
	// No CapsuleResp. Session loop continues reading next PDU.
	return nil
}
