package nvme

// NVMe/TCP PDU stream reader/writer. Adapted from
// weed/storage/blockvol/nvme/wire.go — bounds checks
// preserved (defends against malformed PDUs from a
// hostile peer).

import (
	"bufio"
	"fmt"
	"io"
)

// Reader decodes NVMe/TCP PDUs from a stream.
//
// Usage:
//
//	hdr, _ := r.Dequeue()       // 8-byte CommonHeader
//	r.Receive(&capsuleCmd)      // remaining specific header
//	if r.Length() > 0 {
//	    payload := make([]byte, r.Length())
//	    r.ReceiveData(payload)
//	}
type Reader struct {
	rd     io.Reader
	CH     CommonHeader
	header [maxHeaderSize]byte
	padBuf [maxHeaderSize]byte
}

// NewReader wraps an io.Reader with a default-sized bufio.Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{rd: bufio.NewReader(r)}
}

// Dequeue reads the 8-byte CommonHeader and validates bounds.
func (r *Reader) Dequeue() (*CommonHeader, error) {
	if _, err := io.ReadFull(r.rd, r.header[:commonHeaderSize]); err != nil {
		return nil, err
	}
	r.CH.Unmarshal(r.header[:commonHeaderSize])

	if r.CH.HeaderLength < commonHeaderSize {
		return nil, fmt.Errorf("nvme: HeaderLength %d < minimum %d", r.CH.HeaderLength, commonHeaderSize)
	}
	if r.CH.HeaderLength > maxHeaderSize {
		return nil, fmt.Errorf("nvme: HeaderLength %d > maximum %d", r.CH.HeaderLength, maxHeaderSize)
	}
	if r.CH.DataOffset != 0 && r.CH.DataOffset < r.CH.HeaderLength {
		return nil, fmt.Errorf("nvme: DataOffset %d < HeaderLength %d", r.CH.DataOffset, r.CH.HeaderLength)
	}
	if r.CH.DataOffset != 0 && uint32(r.CH.DataOffset) > r.CH.DataLength {
		return nil, fmt.Errorf("nvme: DataOffset %d > DataLength %d", r.CH.DataOffset, r.CH.DataLength)
	}
	if r.CH.DataLength < uint32(r.CH.HeaderLength) {
		return nil, fmt.Errorf("nvme: DataLength %d < HeaderLength %d", r.CH.DataLength, r.CH.HeaderLength)
	}
	if r.CH.DataOffset == 0 && r.CH.DataLength != uint32(r.CH.HeaderLength) {
		return nil, fmt.Errorf("nvme: DataOffset=0 but DataLength %d != HeaderLength %d",
			r.CH.DataLength, r.CH.HeaderLength)
	}
	return &r.CH, nil
}

// Receive reads the PDU-specific header (HeaderLength - 8 bytes)
// and unmarshals it into pdu. Skips any padding before data.
func (r *Reader) Receive(pdu PDU) error {
	remain := int(r.CH.HeaderLength) - commonHeaderSize
	if remain > 0 {
		if _, err := io.ReadFull(r.rd, r.header[commonHeaderSize:r.CH.HeaderLength]); err != nil {
			return err
		}
		pdu.Unmarshal(r.header[commonHeaderSize:r.CH.HeaderLength])
	}
	pad := int(r.CH.DataOffset) - int(r.CH.HeaderLength)
	for pad > 0 {
		n := pad
		if n > len(r.padBuf) {
			n = len(r.padBuf)
		}
		if _, err := io.ReadFull(r.rd, r.padBuf[:n]); err != nil {
			return err
		}
		pad -= n
	}
	return nil
}

// Length returns the payload byte count (DataLength - DataOffset).
func (r *Reader) Length() uint32 {
	if r.CH.DataOffset != 0 {
		return r.CH.DataLength - uint32(r.CH.DataOffset)
	}
	return 0
}

// ReceiveData reads exactly len(buf) payload bytes.
func (r *Reader) ReceiveData(buf []byte) error {
	_, err := io.ReadFull(r.rd, buf)
	return err
}

// Writer encodes NVMe/TCP PDUs to a stream.
type Writer struct {
	wr     *bufio.Writer
	CH     CommonHeader
	header [maxHeaderSize]byte
}

// NewWriter wraps an io.Writer for NVMe/TCP PDU encoding.
func NewWriter(w io.Writer) *Writer {
	return &Writer{wr: bufio.NewWriter(w)}
}

// SendHeaderOnly writes a complete header-only PDU.
func (w *Writer) SendHeaderOnly(pduType uint8, pdu PDU, specificLen uint8) error {
	w.prepareHeaderOnly(pduType, pdu, specificLen)
	if err := w.flushHeader(); err != nil {
		return err
	}
	return w.wr.Flush()
}

// SendWithData writes a header + payload PDU.
func (w *Writer) SendWithData(pduType, flags uint8, pdu PDU, specificLen uint8, data []byte) error {
	w.prepareWithData(pduType, flags, pdu, specificLen, data)
	if err := w.flushHeader(); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := w.wr.Write(data); err != nil {
			return err
		}
	}
	return w.wr.Flush()
}

func (w *Writer) prepareHeaderOnly(pduType uint8, pdu PDU, specificLen uint8) {
	w.CH.Type = pduType
	w.CH.Flags = 0
	w.CH.HeaderLength = commonHeaderSize + specificLen
	w.CH.DataOffset = 0
	w.CH.DataLength = uint32(w.CH.HeaderLength)
	pdu.Marshal(w.header[commonHeaderSize:])
}

func (w *Writer) prepareWithData(pduType, flags uint8, pdu PDU, specificLen uint8, data []byte) {
	w.CH.Type = pduType
	w.CH.Flags = flags
	w.CH.HeaderLength = commonHeaderSize + specificLen
	if data != nil {
		w.CH.DataOffset = w.CH.HeaderLength
		w.CH.DataLength = uint32(w.CH.HeaderLength) + uint32(len(data))
	} else {
		w.CH.DataOffset = 0
		w.CH.DataLength = uint32(w.CH.HeaderLength)
	}
	pdu.Marshal(w.header[commonHeaderSize:])
}

func (w *Writer) flushHeader() error {
	w.CH.Marshal(w.header[:commonHeaderSize])
	if _, err := w.wr.Write(w.header[:w.CH.HeaderLength]); err != nil {
		return err
	}
	return nil
}
