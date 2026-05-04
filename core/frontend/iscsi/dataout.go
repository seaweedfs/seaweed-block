package iscsi

import (
	"errors"
	"fmt"
)

var (
	ErrDataOutOverflow    = errors.New("iscsi: Data-Out exceeds expected transfer length")
	ErrDataOutDataSN      = errors.New("iscsi: Data-Out DataSN out of order")
	ErrDataOutOffset      = errors.New("iscsi: Data-Out buffer offset out of order")
	ErrDataOutPrematureF  = errors.New("iscsi: Data-Out F-bit before requested bytes")
	ErrDataOutWrongTTT    = errors.New("iscsi: Data-Out TTT does not match R2T")
	ErrDataOutBeyondBurst = errors.New("iscsi: Data-Out exceeds R2T burst")
)

// dataOutCollector owns assembly of one SCSI write payload. It is a
// protocol executor helper, not a storage object: it validates
// immediate-data and R2T-solicited Data-Out sequencing before the SCSI
// handler gets a complete byte slice.
type dataOutCollector struct {
	expectedLen uint32
	buf         []byte
	received    uint32
	nextDataSN  uint32
}

func newDataOutCollector(expectedLen uint32) *dataOutCollector {
	return &dataOutCollector{
		expectedLen: expectedLen,
		buf:         make([]byte, expectedLen),
	}
}

func (c *dataOutCollector) addImmediate(data []byte) error {
	if uint32(len(data)) > c.expectedLen {
		return fmt.Errorf("%w: immediate=%d expected=%d", ErrDataOutOverflow, len(data), c.expectedLen)
	}
	copy(c.buf, data)
	c.received = uint32(len(data))
	return nil
}

func (c *dataOutCollector) done() bool {
	return c.received >= c.expectedLen
}

func (c *dataOutCollector) receivedBytes() uint32 {
	return c.received
}

func (c *dataOutCollector) remaining() uint32 {
	if c.received >= c.expectedLen {
		return 0
	}
	return c.expectedLen - c.received
}

func (c *dataOutCollector) data() []byte {
	return c.buf
}

func (c *dataOutCollector) beginR2T() {
	c.nextDataSN = 0
}

func (c *dataOutCollector) addDataOut(pdu *PDU, ttt, burstEnd uint32) error {
	if pdu.TargetTransferTag() != ttt {
		return fmt.Errorf("%w: got=0x%08x want=0x%08x", ErrDataOutWrongTTT, pdu.TargetTransferTag(), ttt)
	}
	if pdu.DataSN() != c.nextDataSN {
		return fmt.Errorf("%w: got=%d want=%d", ErrDataOutDataSN, pdu.DataSN(), c.nextDataSN)
	}
	c.nextDataSN++

	offset := pdu.BufferOffset()
	if offset != c.received {
		return fmt.Errorf("%w: got=%d want=%d", ErrDataOutOffset, offset, c.received)
	}

	end := offset + uint32(len(pdu.DataSegment))
	if end > c.expectedLen {
		return fmt.Errorf("%w: end=%d expected=%d", ErrDataOutOverflow, end, c.expectedLen)
	}
	if end > burstEnd {
		return fmt.Errorf("%w: end=%d burstEnd=%d", ErrDataOutBeyondBurst, end, burstEnd)
	}

	copy(c.buf[offset:], pdu.DataSegment)
	c.received = end

	if pdu.OpSpecific1()&FlagF != 0 && c.received != burstEnd {
		return fmt.Errorf("%w: received=%d burstEnd=%d", ErrDataOutPrematureF, c.received, burstEnd)
	}
	return nil
}
