package iscsi

import "testing"

func TestCDBExpectedWriteBytes_Write16UsesUint64(t *testing.T) {
	var cdb [16]byte
	cdb[0] = ScsiWrite16
	cdb[10], cdb[11], cdb[12], cdb[13] = 0xff, 0xff, 0xff, 0xff

	got, ok := cdbExpectedWriteBytes(cdb, DefaultBlockSize)
	if !ok {
		t.Fatal("WRITE(16) expected-byte calculation not recognized")
	}
	want := uint64(0xffffffff) * uint64(DefaultBlockSize)
	if got != want {
		t.Fatalf("expected bytes=%d want %d", got, want)
	}
	if got <= uint64(^uint32(0)) {
		t.Fatalf("expected bytes=%d should exceed uint32 max", got)
	}
}

func TestSessionValidateWriteTransferLimitsRejectsOversizedEDTL(t *testing.T) {
	s := &Session{
		negResult: LoginResult{
			MaxBurstLength:   64 * 1024,
			FirstBurstLength: 16 * 1024,
		},
	}
	req := &PDU{}
	req.DataSegment = make([]byte, 4*1024)

	result, ok := s.validateWriteTransferLimits(req, 128*1024)
	if ok {
		t.Fatal("oversized EDTL accepted")
	}
	if result.Status != StatusCheckCondition || result.ASC != ASCInvalidFieldInCDB {
		t.Fatalf("unexpected result: status=0x%02x asc=0x%02x", result.Status, result.ASC)
	}
}

func TestSessionValidateWriteTransferLimitsRejectsOversizedImmediateData(t *testing.T) {
	s := &Session{
		negResult: LoginResult{
			MaxBurstLength:   64 * 1024,
			FirstBurstLength: 4 * 1024,
		},
	}
	req := &PDU{}
	req.DataSegment = make([]byte, 8*1024)

	result, ok := s.validateWriteTransferLimits(req, 8*1024)
	if ok {
		t.Fatal("oversized immediate data accepted")
	}
	if result.Status != StatusCheckCondition || result.ASC != ASCInvalidFieldInCDB {
		t.Fatalf("unexpected result: status=0x%02x asc=0x%02x", result.Status, result.ASC)
	}
}

func TestSessionValidateWriteTransferLimitsAcceptsNegotiatedWrite(t *testing.T) {
	s := &Session{
		negResult: LoginResult{
			MaxBurstLength:   64 * 1024,
			FirstBurstLength: 16 * 1024,
		},
	}
	req := &PDU{}
	req.DataSegment = make([]byte, 8*1024)

	if result, ok := s.validateWriteTransferLimits(req, 32*1024); !ok {
		t.Fatalf("valid write rejected: status=0x%02x asc=0x%02x", result.Status, result.ASC)
	}
}
