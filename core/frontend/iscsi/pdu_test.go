// Ownership: sw unit tests for PDU framing. Not in QA test spec
// inventory (which focuses on protocol-level semantics), but
// the wire layer needs its own regression coverage so pad /
// AHS / truncation behaviors stay locked.
package iscsi_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

func TestPDU_RoundtripBHSOnly(t *testing.T) {
	src := &iscsi.PDU{}
	src.SetOpcode(iscsi.OpSCSICmd)
	src.SetImmediate(true)
	src.SetLUN(0x0100000000000000)
	src.SetInitiatorTaskTag(0x12345678)
	src.SetCmdSN(42)
	src.SetExpStatSN(7)
	cdb := [16]byte{0x28, 0, 0, 0, 0, 0, 0, 0, 1}
	src.SetCDB(cdb)

	var buf bytes.Buffer
	if err := iscsi.WritePDU(&buf, src); err != nil {
		t.Fatalf("WritePDU: %v", err)
	}

	got, err := iscsi.ReadPDU(&buf)
	if err != nil {
		t.Fatalf("ReadPDU: %v", err)
	}
	if got.Opcode() != iscsi.OpSCSICmd {
		t.Fatalf("Opcode=0x%02x", got.Opcode())
	}
	if !got.Immediate() {
		t.Fatal("Immediate lost")
	}
	if got.LUN() != 0x0100000000000000 {
		t.Fatalf("LUN=0x%x", got.LUN())
	}
	if got.InitiatorTaskTag() != 0x12345678 {
		t.Fatalf("ITT=0x%x", got.InitiatorTaskTag())
	}
	if got.CmdSN() != 42 {
		t.Fatalf("CmdSN=%d", got.CmdSN())
	}
	if got.CDB() != cdb {
		t.Fatalf("CDB=%x", got.CDB())
	}
}

func TestPDU_RoundtripWithDataSegmentAndPadding(t *testing.T) {
	src := &iscsi.PDU{}
	src.SetOpcode(iscsi.OpSCSIDataOut)
	// 5-byte data segment → 3 pad bytes on wire to reach 8-byte
	// boundary (pad4 rounds 5 → 8).
	src.DataSegment = []byte("hello")

	var buf bytes.Buffer
	if err := iscsi.WritePDU(&buf, src); err != nil {
		t.Fatalf("WritePDU: %v", err)
	}
	// Total wire length = BHS(48) + data(5) + pad(3) = 56.
	if buf.Len() != 56 {
		t.Fatalf("wire size=%d want 56", buf.Len())
	}
	got, err := iscsi.ReadPDU(&buf)
	if err != nil {
		t.Fatalf("ReadPDU: %v", err)
	}
	if !bytes.Equal(got.DataSegment, []byte("hello")) {
		t.Fatalf("DataSegment=%q", got.DataSegment)
	}
}

func TestPDU_RoundtripWithAHS(t *testing.T) {
	src := &iscsi.PDU{}
	src.SetOpcode(iscsi.OpSCSICmd)
	src.AHS = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	var buf bytes.Buffer
	if err := iscsi.WritePDU(&buf, src); err != nil {
		t.Fatalf("WritePDU: %v", err)
	}
	got, err := iscsi.ReadPDU(&buf)
	if err != nil {
		t.Fatalf("ReadPDU: %v", err)
	}
	if !bytes.Equal(got.AHS, src.AHS) {
		t.Fatalf("AHS=%x", got.AHS)
	}
	if got.TotalAHSLength() != 2 { // 8 bytes / 4 = 2 words
		t.Fatalf("TotalAHSLength=%d want 2", got.TotalAHSLength())
	}
}

func TestPDU_InvalidAHSLength_WriteRejects(t *testing.T) {
	src := &iscsi.PDU{}
	src.AHS = []byte{0x01, 0x02, 0x03} // not multiple of 4
	var buf bytes.Buffer
	err := iscsi.WritePDU(&buf, src)
	if err == nil {
		t.Fatal("WritePDU accepted AHS not multiple of 4")
	}
}

func TestPDU_TruncatedBHS_ReadReturnsTruncated(t *testing.T) {
	// Only 20 bytes of a 48-byte BHS.
	buf := bytes.NewReader(make([]byte, 20))
	_, err := iscsi.ReadPDU(buf)
	if err != iscsi.ErrPDUTruncated {
		t.Fatalf("got %v, want ErrPDUTruncated", err)
	}
}

func TestPDU_TruncatedDataSegment_ReadReturnsTruncated(t *testing.T) {
	// Valid BHS declaring a 16-byte data segment, but we only
	// supply 8 bytes of data.
	src := &iscsi.PDU{}
	src.SetOpcode(iscsi.OpSCSIDataOut)
	src.DataSegment = make([]byte, 16)
	var raw bytes.Buffer
	_ = iscsi.WritePDU(&raw, src)
	// Truncate to BHS + 8 bytes.
	trunc := raw.Bytes()[:iscsi.BHSLength+8]
	_, err := iscsi.ReadPDU(bytes.NewReader(trunc))
	if err != iscsi.ErrPDUTruncated {
		t.Fatalf("got %v, want ErrPDUTruncated", err)
	}
}

func TestPDU_DataSegmentTooLarge_ReadRejects(t *testing.T) {
	// Craft a BHS declaring the max 24-bit DSL value 0xFFFFFF
	// (≈16 MiB - 1), which exceeds MaxDataSegmentLength (8 MiB).
	bhs := make([]byte, iscsi.BHSLength)
	bhs[0] = iscsi.OpSCSIDataOut
	// DSL field at bytes 5-7, big-endian 3-byte length.
	bhs[5], bhs[6], bhs[7] = 0xff, 0xff, 0xff
	_, err := iscsi.ReadPDU(bytes.NewReader(bhs))
	if err == nil {
		t.Fatal("ReadPDU accepted oversized data segment")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("maximum")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPDU_EOFBeforeBHS_ReturnsEOF(t *testing.T) {
	_, err := iscsi.ReadPDU(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestPDU_LoginStageFlags_RoundTrip(t *testing.T) {
	p := &iscsi.PDU{}
	p.SetOpcode(iscsi.OpLoginReq)
	p.SetLoginStages(iscsi.StageSecurityNeg, iscsi.StageLoginOp)
	p.SetLoginTransit(true)
	if p.LoginCSG() != iscsi.StageSecurityNeg {
		t.Fatalf("CSG=%d", p.LoginCSG())
	}
	if p.LoginNSG() != iscsi.StageLoginOp {
		t.Fatalf("NSG=%d", p.LoginNSG())
	}
	if !p.LoginTransit() {
		t.Fatal("transit lost")
	}
	p.SetLoginTransit(false)
	if p.LoginTransit() {
		t.Fatal("transit should clear")
	}
}

func TestPDU_OpcodeName_CoversAllKnown(t *testing.T) {
	known := []uint8{
		iscsi.OpNOPOut, iscsi.OpSCSICmd, iscsi.OpLoginReq, iscsi.OpTextReq,
		iscsi.OpSCSIDataOut, iscsi.OpLogoutReq,
		iscsi.OpNOPIn, iscsi.OpSCSIResp, iscsi.OpLoginResp, iscsi.OpTextResp,
		iscsi.OpSCSIDataIn, iscsi.OpLogoutResp, iscsi.OpR2T, iscsi.OpReject,
	}
	for _, op := range known {
		if iscsi.OpcodeName(op) == "" {
			t.Fatalf("no name for opcode 0x%02x", op)
		}
	}
	// Unknown opcode should produce a non-empty hex string rather
	// than panic or empty.
	if name := iscsi.OpcodeName(0xaa); name == "" {
		t.Fatal("unknown opcode yielded empty name")
	}
}
