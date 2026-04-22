// Ownership: QA (from sketch-approved test spec v3-phase-15-t2-test-spec.md for T2 First Real Frontends).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t2-test-spec.md
// Maps to ledger rows: INV-FRONTEND-ISCSI-001, INV-FRONTEND-PROTOCOL-001,
//                      INV-FRONTEND-PROTOCOL-002.WRITE, INV-FRONTEND-PROTOCOL-002.READ
//
// Test layer: Unit (L0)
// Protocol: iSCSI
// Bounded fate:
//   - SCSI READ(10) and WRITE(10) route every byte through
//     frontend.Backend with correct offset/length.
//   - frontend.ErrStalePrimary maps to CHECK CONDITION + NOT READY
//     + stale-lineage ASC; no success ACK on stale WRITE; no bytes
//     returned on stale READ.
//   - Invalid input (LBA out of range, short dataOut) maps to
//     CHECK CONDITION + ILLEGAL REQUEST without reaching the backend.
//
// Handler-layer L0 tests: the SCSIHandler is the deterministic
// seam between a SCSI CDB+dataOut and the frontend.Backend
// contract. These tests bypass wire framing (PDU) — the PDU
// coverage lives in pdu_test.go (planned next session). Handler
// + backend fakes are sufficient to pin the protocol semantics
// the test-spec demands.
//
// Spec adaptation note: test-spec §4 examples use a session-
// level API (`sess.WriteCmd(ctx, 0, payload)` returning
// *iscsi.SCSIError). This checkpoint delivers the HANDLER-level
// equivalent; the session-level wrapper (Login/Session/TCP)
// lands with PDU framing in a later session. Same invariants,
// same assertions — simply a shorter call path for L0.
package iscsi_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// writeCDB builds a SCSI WRITE(10) CDB at (lba, transferLen).
func writeCDB(lba uint32, transferLen uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], transferLen)
	return cdb
}

// readCDB builds a SCSI READ(10) CDB at (lba, transferLen).
func readCDB(lba uint32, transferLen uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], transferLen)
	return cdb
}

// T2.L0.i1 — WRITE(10) + READ(10) route through frontend.Backend
// exactly once each, at the correct offset, with correct data.
func TestT2ISCSI_CommandReadWrite_UsesFrontendBackend(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3,
	})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
	ctx := context.Background()

	// WRITE 1 block (512 bytes) at LBA 0.
	payload := make([]byte, iscsi.DefaultBlockSize)
	copy(payload, []byte("hello-scsi-t2"))
	r := h.HandleCommand(ctx, writeCDB(0, 1), payload)
	if r.AsError() != nil {
		t.Fatalf("WRITE(10): got err=%v", r.AsError())
	}
	if rec.WriteCount() != 1 {
		t.Fatalf("backend Write invoked %d times, want 1", rec.WriteCount())
	}
	w := rec.WriteAt(0)
	if w.Offset != 0 {
		t.Fatalf("backend Write offset=%d want 0", w.Offset)
	}
	if !bytes.Equal(w.Data, payload) {
		t.Fatalf("backend Write data mismatch")
	}

	// READ 1 block at LBA 0 — data round-trips through the
	// recording backend's byte slab.
	rr := h.HandleCommand(ctx, readCDB(0, 1), nil)
	if rr.AsError() != nil {
		t.Fatalf("READ(10): got err=%v", rr.AsError())
	}
	if len(rr.Data) != int(iscsi.DefaultBlockSize) {
		t.Fatalf("READ data length=%d want %d", len(rr.Data), iscsi.DefaultBlockSize)
	}
	if !bytes.Equal(rr.Data, payload) {
		t.Fatalf("READ data != written payload")
	}
	if rec.ReadCount() != 1 {
		t.Fatalf("backend Read invoked %d times, want 1", rec.ReadCount())
	}
}

// T2.L0.i2 — stale WRITE returns CHECK CONDITION with non-empty
// sense; MUST NOT be success. Implements the load-bearing rule
// in sketch §6: silent stale-ACK is a safety violation.
func TestT2ISCSI_StaleWrite_ReturnsCheckCondition(t *testing.T) {
	stale := testback.NewStaleRejectingBackend()
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: stale})
	ctx := context.Background()

	payload := make([]byte, iscsi.DefaultBlockSize)
	r := h.HandleCommand(ctx, writeCDB(0, 1), payload)

	if r.Status == iscsi.StatusGood {
		t.Fatal("stale WRITE returned Good — silent stale-ACK is a correctness violation")
	}
	se := r.AsError()
	if se == nil {
		t.Fatal("AsError nil for non-Good status")
	}
	if se.Status != iscsi.StatusCheckCondition {
		t.Fatalf("Status=0x%02x want CHECK CONDITION (0x02)", se.Status)
	}
	if se.SenseKey == iscsi.SenseNone {
		t.Fatal("SenseKey is NoSense after stale-write rejection — must be non-empty")
	}
	if se.SenseKey != iscsi.SenseNotReady {
		t.Fatalf("SenseKey=0x%02x want NotReady (0x%02x)", se.SenseKey, iscsi.SenseNotReady)
	}
	if se.ASC != iscsi.ASCStaleLineage {
		t.Fatalf("ASC=0x%02x want stale-lineage (0x%02x)", se.ASC, iscsi.ASCStaleLineage)
	}
}

// T2.L0.i3 — stale READ returns CHECK CONDITION; MUST NOT return
// stale bytes to initiator. Load-bearing parity with i2.
func TestT2ISCSI_StaleRead_ReturnsCheckCondition(t *testing.T) {
	stale := testback.NewStaleRejectingBackend()
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: stale})
	ctx := context.Background()

	r := h.HandleCommand(ctx, readCDB(0, 1), nil)

	if r.Status == iscsi.StatusGood {
		t.Fatal("stale READ returned Good — stale-read data leak is a correctness violation")
	}
	if len(r.Data) != 0 {
		t.Fatalf("stale READ returned %d bytes of data — must not leak stale bytes to initiator", len(r.Data))
	}
	se := r.AsError()
	if se == nil || se.Status != iscsi.StatusCheckCondition {
		t.Fatalf("expected CHECK CONDITION, got %+v", r)
	}
	if se.SenseKey != iscsi.SenseNotReady {
		t.Fatalf("SenseKey=0x%02x want NotReady", se.SenseKey)
	}
}

// T2.L0.i4 — invalid input fails closed BEFORE touching the
// backend. Table-driven: LBA out of range (high), LBA out of
// range (overflow), short dataOut. Zero-length transfer is a
// valid no-op and tested at Good.
func TestT2ISCSI_InvalidInput_FailsClosed(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
	ctx := context.Background()

	cases := []struct {
		name   string
		cdb    [16]byte
		data   []byte
		wantSK uint8
		wantASC uint8
	}{
		{
			name:    "read_lba_past_end",
			cdb:     readCDB(uint32(iscsi.DefaultVolumeBlocks+1), 1),
			wantSK:  iscsi.SenseIllegalRequest,
			wantASC: iscsi.ASCLBAOutOfRange,
		},
		{
			name:    "write_lba_overflow_range",
			cdb:     writeCDB(uint32(iscsi.DefaultVolumeBlocks-1), 4),
			data:    make([]byte, 4*iscsi.DefaultBlockSize),
			wantSK:  iscsi.SenseIllegalRequest,
			wantASC: iscsi.ASCLBAOutOfRange,
		},
		{
			name:    "write_short_dataout",
			cdb:     writeCDB(0, 4),
			data:    make([]byte, iscsi.DefaultBlockSize), // need 4*bs, supplied 1*bs
			wantSK:  iscsi.SenseIllegalRequest,
			wantASC: iscsi.ASCInvalidFieldInCDB,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beforeWrites := rec.WriteCount()
			beforeReads := rec.ReadCount()
			r := h.HandleCommand(ctx, tc.cdb, tc.data)
			if r.Status == iscsi.StatusGood {
				t.Fatalf("invalid input %s returned Good — must fail closed", tc.name)
			}
			if r.SenseKey != tc.wantSK {
				t.Fatalf("SenseKey=0x%02x want 0x%02x", r.SenseKey, tc.wantSK)
			}
			if r.ASC != tc.wantASC {
				t.Fatalf("ASC=0x%02x want 0x%02x", r.ASC, tc.wantASC)
			}
			// Most important: backend NOT invoked for any invalid input.
			if rec.WriteCount() != beforeWrites {
				t.Fatalf("backend Write invoked under invalid input %s", tc.name)
			}
			if rec.ReadCount() != beforeReads {
				t.Fatalf("backend Read invoked under invalid input %s", tc.name)
			}
		})
	}

	// Zero-length transfer is a valid no-op: Good status, backend
	// never invoked.
	beforeWrites := rec.WriteCount()
	r := h.HandleCommand(ctx, writeCDB(0, 0), nil)
	if r.Status != iscsi.StatusGood {
		t.Fatalf("zero-length WRITE: got status=0x%02x, want Good", r.Status)
	}
	if rec.WriteCount() != beforeWrites {
		t.Fatal("zero-length WRITE invoked backend")
	}
}

// ErrBackendClosed should also map to CHECK CONDITION, but via
// NotReady/NotReadyManualIntv (distinct from stale-lineage ASC).
// Not in the QA spec inventory but worth a regression test so
// the two fail-closed paths stay distinguishable.
func TestT2ISCSI_BackendClosed_MapsToNotReady(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	_ = rec.Close()
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
	ctx := context.Background()
	r := h.HandleCommand(ctx, writeCDB(0, 1), make([]byte, iscsi.DefaultBlockSize))
	if r.Status != iscsi.StatusCheckCondition {
		t.Fatalf("closed-backend Write: Status=0x%02x want CHECK CONDITION", r.Status)
	}
	// Pin the distinction: stale-lineage uses (0x04, 0x0A)
	// "asymmetric access state transition"; closed-backend uses
	// (0x04, 0x00) "cause not reportable". Same sense key
	// (NotReady), same ASC byte, different ASCQ — operator can
	// tell them apart in logs.
	if r.ASC == iscsi.ASCStaleLineage && r.ASCQ == iscsi.ASCQStaleLineage {
		t.Fatalf("closed-backend must NOT use stale-lineage ASC/ASCQ tuple (would conflate two fail modes)")
	}
	if r.ASC != iscsi.ASCNotReady {
		t.Fatalf("closed-backend: ASC=0x%02x want NotReady (0x%02x)", r.ASC, iscsi.ASCNotReady)
	}
}

// Sanity: metadata commands work so an initiator's first few
// handshake PDUs (TUR, INQUIRY, READ CAPACITY, REPORT LUNS)
// don't immediately fail. Full coverage is in L1 (later session)
// against real PDU framing.
func TestT2ISCSI_MetadataCommands_SuccessForT2Subset(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
	ctx := context.Background()

	for _, op := range []struct {
		name string
		cdb  [16]byte
	}{
		{"TEST_UNIT_READY", [16]byte{iscsi.ScsiTestUnitReady}},
		{"INQUIRY", [16]byte{iscsi.ScsiInquiry, 0, 0, 0, 36}},
		{"READ_CAPACITY_10", [16]byte{iscsi.ScsiReadCapacity10}},
		{"REPORT_LUNS", [16]byte{iscsi.ScsiReportLuns}},
		{"REQUEST_SENSE", [16]byte{iscsi.ScsiRequestSense, 0, 0, 0, 18}},
	} {
		t.Run(op.name, func(t *testing.T) {
			r := h.HandleCommand(ctx, op.cdb, nil)
			if r.Status != iscsi.StatusGood {
				t.Fatalf("%s: Status=0x%02x want Good", op.name, r.Status)
			}
		})
	}
}

// Unknown opcode must not panic and must not reach the backend.
func TestT2ISCSI_UnknownOpcode_ReturnsIllegalRequest(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
	ctx := context.Background()
	r := h.HandleCommand(ctx, [16]byte{0xff}, nil)
	if r.SenseKey != iscsi.SenseIllegalRequest {
		t.Fatalf("unknown opcode: SenseKey=0x%02x want IllegalRequest", r.SenseKey)
	}
	if rec.ReadCount()+rec.WriteCount() != 0 {
		t.Fatal("unknown opcode reached backend")
	}
}

// AsError on a Good result must be nil.
func TestSCSIResult_AsError_NilOnGood(t *testing.T) {
	r := iscsi.SCSIResult{Status: iscsi.StatusGood}
	if r.AsError() != nil {
		t.Fatal("AsError non-nil on Good")
	}
}

// Sanity: the error type still supports errors.As through
// SCSIResult.AsError().
func TestSCSIResult_AsError_SupportsErrorsAs(t *testing.T) {
	r := iscsi.SCSIResult{Status: iscsi.StatusCheckCondition, SenseKey: iscsi.SenseNotReady, ASC: iscsi.ASCStaleLineage}
	var err error = r.AsError()
	var se *iscsi.SCSIError
	if !errors.As(err, &se) {
		t.Fatal("errors.As failed")
	}
	if se.ASC != iscsi.ASCStaleLineage {
		t.Fatalf("unwrapped ASC=0x%02x", se.ASC)
	}
}
