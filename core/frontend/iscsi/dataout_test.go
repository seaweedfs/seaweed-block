package iscsi

import (
	"bytes"
	"errors"
	"testing"
)

func dataOutPDU(ttt, dataSN, offset uint32, final bool, data []byte) *PDU {
	p := &PDU{}
	p.SetOpcode(OpSCSIDataOut)
	if final {
		p.SetOpSpecific1(FlagF)
	}
	p.SetTargetTransferTag(ttt)
	p.SetDataSN(dataSN)
	p.SetBufferOffset(offset)
	p.DataSegment = data
	return p
}

func TestDataOutCollector_ImmediateAndDataOut(t *testing.T) {
	c := newDataOutCollector(8)
	if err := c.addImmediate([]byte{1, 2, 3}); err != nil {
		t.Fatalf("addImmediate: %v", err)
	}
	if c.receivedBytes() != 3 {
		t.Fatalf("received=%d want 3", c.receivedBytes())
	}
	c.beginR2T()
	if err := c.addDataOut(dataOutPDU(7, 0, 3, true, []byte{4, 5, 6, 7, 8}), 7, 8); err != nil {
		t.Fatalf("addDataOut: %v", err)
	}
	if !c.done() {
		t.Fatal("collector not done")
	}
	if !bytes.Equal(c.data(), []byte{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("data=%v", c.data())
	}
}

func TestDataOutCollector_MultiPDUDataOut(t *testing.T) {
	c := newDataOutCollector(6)
	c.beginR2T()
	if err := c.addDataOut(dataOutPDU(9, 0, 0, false, []byte{1, 2}), 9, 6); err != nil {
		t.Fatalf("first Data-Out: %v", err)
	}
	if err := c.addDataOut(dataOutPDU(9, 1, 2, false, []byte{3, 4}), 9, 6); err != nil {
		t.Fatalf("second Data-Out: %v", err)
	}
	if err := c.addDataOut(dataOutPDU(9, 2, 4, true, []byte{5, 6}), 9, 6); err != nil {
		t.Fatalf("third Data-Out: %v", err)
	}
	if !bytes.Equal(c.data(), []byte{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("data=%v", c.data())
	}
}

func TestDataOutCollector_RejectsWrongDataSN(t *testing.T) {
	c := newDataOutCollector(4)
	c.beginR2T()
	err := c.addDataOut(dataOutPDU(1, 1, 0, true, []byte{1, 2, 3, 4}), 1, 4)
	if !errors.Is(err, ErrDataOutDataSN) {
		t.Fatalf("err=%v want ErrDataOutDataSN", err)
	}
}

func TestDataOutCollector_RejectsWrongOffset(t *testing.T) {
	c := newDataOutCollector(4)
	c.beginR2T()
	err := c.addDataOut(dataOutPDU(1, 0, 2, true, []byte{1, 2}), 1, 4)
	if !errors.Is(err, ErrDataOutOffset) {
		t.Fatalf("err=%v want ErrDataOutOffset", err)
	}
}

func TestDataOutCollector_RejectsOverflow(t *testing.T) {
	c := newDataOutCollector(4)
	c.beginR2T()
	err := c.addDataOut(dataOutPDU(1, 0, 0, true, []byte{1, 2, 3, 4, 5}), 1, 8)
	if !errors.Is(err, ErrDataOutOverflow) {
		t.Fatalf("err=%v want ErrDataOutOverflow", err)
	}
}

func TestDataOutCollector_RejectsBeyondBurst(t *testing.T) {
	c := newDataOutCollector(8)
	c.beginR2T()
	err := c.addDataOut(dataOutPDU(1, 0, 0, true, []byte{1, 2, 3, 4, 5}), 1, 4)
	if !errors.Is(err, ErrDataOutBeyondBurst) {
		t.Fatalf("err=%v want ErrDataOutBeyondBurst", err)
	}
}

func TestDataOutCollector_RejectsPrematureFinal(t *testing.T) {
	c := newDataOutCollector(8)
	c.beginR2T()
	err := c.addDataOut(dataOutPDU(1, 0, 0, true, []byte{1, 2}), 1, 4)
	if !errors.Is(err, ErrDataOutPrematureF) {
		t.Fatalf("err=%v want ErrDataOutPrematureF", err)
	}
}

func TestDataOutCollector_RejectsWrongTTT(t *testing.T) {
	c := newDataOutCollector(4)
	c.beginR2T()
	err := c.addDataOut(dataOutPDU(2, 0, 0, true, []byte{1, 2, 3, 4}), 1, 4)
	if !errors.Is(err, ErrDataOutWrongTTT) {
		t.Fatalf("err=%v want ErrDataOutWrongTTT", err)
	}
}
