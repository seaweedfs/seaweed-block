package nvme

// NVMe-oF Fabric commands — PropertyGet (FCType 0x04) + PropertySet
// (FCType 0x00). Connect (fcConnect) and Disconnect (fcDisconnect,
// BUG-002) live in session.go because they manipulate per-session
// state (queue binding, CNTLID).
//
// BUG-001 fix (2026-04-22): handlers take *Request and enqueue
// responses through respCh instead of writing to s.w directly.
//
// Register offsets (per NVMe 1.3 §3.1):
//   0x00 CAP   (8 bytes, read-only)
//   0x08 VS    (4 bytes, read-only)
//   0x14 CC    (4 bytes, writable)
//   0x1C CSTS  (4 bytes, read-only, derived from CC)

import "fmt"

// Property register offsets (NVMe 1.3 §3.1.1+).
const (
	propCAP  uint32 = 0x00
	propVS   uint32 = 0x08
	propCC   uint32 = 0x14
	propCSTS uint32 = 0x1C
)

// handlePropertyGet services Fabric PropertyGet.
func (s *Session) handlePropertyGet(req *Request) error {
	cmd := &req.capsule
	if s.ctrl == nil {
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCConnectInvalidParameters, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	size8 := (cmd.D10 & 0x01) != 0
	offset := cmd.D11

	dw0, dw1, err := s.getPropertyValue(offset, size8)
	if err != nil {
		if s.logger != nil {
			s.logger.Printf("nvme: PropertyGet rejected: offset=0x%x size8=%v: %v",
				offset, size8, err)
		}
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCConnectInvalidParameters, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	req.resp.DW0 = dw0
	req.resp.DW1 = dw1
	s.enqueueResponse(&response{resp: req.resp})
	return nil
}

// handlePropertySet services Fabric PropertySet. Only CC is writable;
// CC writes trigger synchronous CSTS.RDY update via setCC (QA #3).
func (s *Session) handlePropertySet(req *Request) error {
	cmd := &req.capsule
	if s.ctrl == nil {
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCConnectInvalidParameters, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	size8 := (cmd.D10 & 0x01) != 0
	offset := cmd.D11

	val := uint64(cmd.D12)
	if size8 {
		val |= uint64(cmd.D13) << 32
	}

	switch offset {
	case propCC:
		if size8 {
			req.resp.Status = MakeStatusField(SCTCommandSpecific,
				SCConnectInvalidParameters, true)
			s.enqueueResponse(&response{resp: req.resp})
			return nil
		}
		newCSTS := s.ctrl.setCC(uint32(val))
		if s.logger != nil {
			s.logger.Printf("nvme: PropertySet(CC)=0x%08x → CSTS=0x%08x (RDY=%d)",
				uint32(val), newCSTS, newCSTS&0x01)
		}
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	case propCAP, propVS, propCSTS:
		// Read-only.
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCConnectInvalidParameters, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	default:
		req.resp.Status = MakeStatusField(SCTCommandSpecific,
			SCConnectInvalidParameters, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
}

// getPropertyValue returns (dw0, dw1) for a given offset + size.
func (s *Session) getPropertyValue(offset uint32, size8 bool) (uint32, uint32, error) {
	switch offset {
	case propCAP:
		if !size8 {
			return 0, 0, fmt.Errorf("CAP requires 8-byte access (size bit=1)")
		}
		cap := s.ctrl.getCAP()
		return uint32(cap), uint32(cap >> 32), nil
	case propVS:
		if size8 {
			return 0, 0, fmt.Errorf("VS is 4-byte only")
		}
		return s.ctrl.getVS(), 0, nil
	case propCC:
		if size8 {
			return 0, 0, fmt.Errorf("CC is 4-byte only")
		}
		return s.ctrl.getCC(), 0, nil
	case propCSTS:
		if size8 {
			return 0, 0, fmt.Errorf("CSTS is 4-byte only")
		}
		return s.ctrl.getCSTS(), 0, nil
	default:
		return 0, 0, fmt.Errorf("unknown property offset 0x%x", offset)
	}
}
