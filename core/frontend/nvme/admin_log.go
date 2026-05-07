package nvme

import "encoding/binary"

const (
	anaLogSize   = 40
	smartLogSize = 512
)

// handleGetLogPage services admin opcode 0x02.
//
// P3 initially implements only ANA log page 0x0c. Identify still does not
// advertise ANA until the host-visible fields are flipped in the later P3-C
// slice; serving the page first lets us test the payload without letting Linux
// multipath depend on it prematurely.
func (s *Session) handleGetLogPage(req *Request) error {
	cmd := &req.capsule
	lid := uint8(cmd.D10 & 0xFF)
	numdl := (cmd.D10 >> 16) & 0xFFFF
	numdu := cmd.D11 & 0xFFFF
	numd := (numdu << 16) | numdl
	length := (numd + 1) * 4

	switch lid {
	case logPageSMART:
		return s.handleSMARTLogPage(req, length)
	case logPageANA:
		return s.handleANALogPage(req, length)
	default:
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
}

func (s *Session) handleSMARTLogPage(req *Request, length uint32) error {
	if length > smartLogSize {
		length = smartLogSize
	}
	if length == 0 {
		length = 4
	}

	buf := make([]byte, smartLogSize)
	// Minimal SMART / Health Information log. Linux fetches this during
	// controller bring-up; returning success avoids a slow retry path while
	// keeping all health counters conservative.
	binary.LittleEndian.PutUint16(buf[1:], 300) // temperature: 300 K
	buf[3] = 100                                // available spare
	buf[4] = 10                                 // available spare threshold
	s.enqueueResponse(&response{resp: req.resp, c2hData: buf[:length]})
	return nil
}

func (s *Session) handleANALogPage(req *Request, length uint32) error {
	prov := s.handler.ANAProvider()
	if prov == nil {
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	if length > anaLogSize {
		length = anaLogSize
	}
	if length == 0 {
		length = 4
	}

	buf := make([]byte, anaLogSize)
	changeCount := prov.ANAChangeCount()
	groupID := prov.ANAGroupID()
	if groupID == 0 {
		groupID = 1
	}

	binary.LittleEndian.PutUint64(buf[0:], changeCount)
	binary.LittleEndian.PutUint16(buf[8:], 1) // NGRPS

	// Single ANA group descriptor:
	// [16:20] ANAGRPID
	// [20:24] NNSID
	// [24:32] group change count
	// [32]    ANA state
	// [36:40] NSID
	binary.LittleEndian.PutUint32(buf[16:], groupID)
	binary.LittleEndian.PutUint32(buf[20:], 1)
	binary.LittleEndian.PutUint64(buf[24:], changeCount)
	buf[32] = byte(prov.ANAState())
	binary.LittleEndian.PutUint32(buf[36:], s.handler.NSID())

	s.enqueueResponse(&response{resp: req.resp, c2hData: buf[:length]})
	return nil
}
