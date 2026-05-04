package iscsi

import "testing"

func dataInReq(itt uint32, expected uint32) *PDU {
	req := &PDU{}
	req.SetOpcode(OpSCSICmd)
	req.SetInitiatorTaskTag(itt)
	req.SetExpectedDataTransferLength(expected)
	return req
}

func TestDataInWriter_SplitsAndFinalCarriesStatus(t *testing.T) {
	w := newDataInWriter(4)
	req := dataInReq(11, 10)
	pdus := w.build(req, SCSIResult{Status: StatusGood, Data: []byte("abcdefghij")})
	if len(pdus) != 3 {
		t.Fatalf("pdus=%d want 3", len(pdus))
	}
	for i, p := range pdus {
		if p.InitiatorTaskTag() != 11 {
			t.Fatalf("pdu[%d] ITT=%d want 11", i, p.InitiatorTaskTag())
		}
		if p.DataSN() != uint32(i) {
			t.Fatalf("pdu[%d] DataSN=%d want %d", i, p.DataSN(), i)
		}
		if p.BufferOffset() != uint32(i*4) {
			t.Fatalf("pdu[%d] offset=%d want %d", i, p.BufferOffset(), i*4)
		}
		if i < 2 && p.OpSpecific1()&(FlagS|FlagF) != 0 {
			t.Fatalf("intermediate pdu[%d] flags=0x%02x", i, p.OpSpecific1())
		}
	}
	final := pdus[2]
	if final.OpSpecific1()&(FlagS|FlagF) != FlagS|FlagF {
		t.Fatalf("final flags=0x%02x want S|F", final.OpSpecific1())
	}
	if final.SCSIStatusByte() != StatusGood {
		t.Fatalf("final status=0x%02x want Good", final.SCSIStatusByte())
	}
	if final.ResidualCount() != 0 {
		t.Fatalf("final residual=%d want 0", final.ResidualCount())
	}
}

func TestDataInWriter_UnderflowResidual(t *testing.T) {
	w := newDataInWriter(16)
	req := dataInReq(12, 10)
	pdus := w.build(req, SCSIResult{Status: StatusGood, Data: []byte("abcd")})
	if len(pdus) != 1 {
		t.Fatalf("pdus=%d want 1", len(pdus))
	}
	if got := pdus[0].ResidualCount(); got != 6 {
		t.Fatalf("residual=%d want 6", got)
	}
	if flags := pdus[0].OpSpecific1(); flags&FlagU == 0 || flags&FlagO != 0 {
		t.Fatalf("flags=0x%02x want underflow only", flags)
	}
}

func TestDataInWriter_OverflowResidual(t *testing.T) {
	w := newDataInWriter(16)
	req := dataInReq(14, 4)
	pdus := w.build(req, SCSIResult{Status: StatusGood, Data: []byte("abcdefghij")})
	if len(pdus) != 1 {
		t.Fatalf("pdus=%d want 1", len(pdus))
	}
	if got := pdus[0].ResidualCount(); got != 6 {
		t.Fatalf("residual=%d want 6", got)
	}
	if flags := pdus[0].OpSpecific1(); flags&FlagO == 0 || flags&FlagU != 0 {
		t.Fatalf("flags=0x%02x want overflow only", flags)
	}
}

func TestDataInWriter_ZeroLengthReadCarriesStatus(t *testing.T) {
	w := newDataInWriter(16)
	req := dataInReq(13, 8)
	pdus := w.build(req, SCSIResult{Status: StatusGood})
	if len(pdus) != 1 {
		t.Fatalf("pdus=%d want 1", len(pdus))
	}
	p := pdus[0]
	if p.OpSpecific1()&(FlagS|FlagF) != FlagS|FlagF {
		t.Fatalf("flags=0x%02x want S|F", p.OpSpecific1())
	}
	if p.ResidualCount() != 8 {
		t.Fatalf("residual=%d want 8", p.ResidualCount())
	}
	if flags := p.OpSpecific1(); flags&FlagU == 0 || flags&FlagO != 0 {
		t.Fatalf("flags=0x%02x want underflow only", flags)
	}
}
