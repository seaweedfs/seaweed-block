package nvme

import (
	"io"
)

// sessionTransport is the narrow wire adapter used by Session. The current
// implementation is NVMe/TCP PDU framing; a future RDMA adapter must satisfy
// this command/response boundary without pretending RDMA is a TCP byte stream.
type sessionTransport interface {
	Dequeue() (*CommonHeader, error)
	Receive(PDU) error
	Length() uint32
	ReceiveData([]byte) error
	SendHeaderOnly(pduType uint8, pdu PDU, specificLen uint8) error
	SendWithData(pduType, flags uint8, pdu PDU, specificLen uint8, data []byte) error
}

type tcpPDUTransport struct {
	r *Reader
	w *Writer
}

func newTCPPDUTransport(rw io.ReadWriter) *tcpPDUTransport {
	return &tcpPDUTransport{
		r: NewReader(rw),
		w: NewWriter(rw),
	}
}

func (t *tcpPDUTransport) Dequeue() (*CommonHeader, error) {
	return t.r.Dequeue()
}

func (t *tcpPDUTransport) Receive(pdu PDU) error {
	return t.r.Receive(pdu)
}

func (t *tcpPDUTransport) Length() uint32 {
	return t.r.Length()
}

func (t *tcpPDUTransport) ReceiveData(buf []byte) error {
	return t.r.ReceiveData(buf)
}

func (t *tcpPDUTransport) SendHeaderOnly(pduType uint8, pdu PDU, specificLen uint8) error {
	return t.w.SendHeaderOnly(pduType, pdu, specificLen)
}

func (t *tcpPDUTransport) SendWithData(pduType, flags uint8, pdu PDU, specificLen uint8, data []byte) error {
	return t.w.SendWithData(pduType, flags, pdu, specificLen, data)
}
