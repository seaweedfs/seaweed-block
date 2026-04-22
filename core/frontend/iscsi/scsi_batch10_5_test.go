// Ownership: sw port from V2 scsi_test.go.
// Batch 10.5 — port plan §4, locked 2026-04-22.
// Covers INQUIRY VPD (0x00 / 0x80 / 0x83 non-ALUA) +
// READ_CAPACITY(16) + READ(16) / WRITE(16).
//
// VPD 0xB0 / 0xB2 are NOT implemented (port plan §3.2); tests
// that probe them confirm InvalidFieldInCDB.
package iscsi_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// inquiryVPDCDB builds an INQUIRY CDB with EVPD=1 for the given
// page, allocating up to 255 bytes.
func inquiryVPDCDB(page uint8, allocLen uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiInquiry
	cdb[1] = 0x01 // EVPD
	cdb[2] = page
	binary.BigEndian.PutUint16(cdb[3:5], allocLen)
	return cdb
}

// Standard INQUIRY (EVPD=0): V2 tuned 96-byte response with
// Additional Length=91, CmdQue=1. PM Medium finding 2026-04-22.
func TestT2Batch10_5_Inquiry_StandardResponse_V2TunedShape(t *testing.T) {
	h := newHandlerForTest(t)
	var cdb [16]byte
	cdb[0] = iscsi.ScsiInquiry
	binary.BigEndian.PutUint16(cdb[3:5], 96)
	r := h.HandleCommand(context.Background(), cdb, nil)
	if r.AsError() != nil {
		t.Fatalf("INQUIRY: %v", r.AsError())
	}
	if len(r.Data) != 96 {
		t.Fatalf("len=%d want 96 (V2-tuned shape)", len(r.Data))
	}
	if r.Data[0] != 0x00 { // SBC direct-access
		t.Fatalf("byte 0 = 0x%02x want 0x00", r.Data[0])
	}
	if r.Data[2] != 0x06 { // SPC-4 version
		t.Fatalf("byte 2 = 0x%02x want 0x06 (SPC-4)", r.Data[2])
	}
	if r.Data[3] != 0x02 { // Response data format
		t.Fatalf("byte 3 = 0x%02x want 0x02", r.Data[3])
	}
	if r.Data[4] != 91 { // Additional length = 96 - 5
		t.Fatalf("additional length=%d want 91", r.Data[4])
	}
	// TPGS (byte 5 bits 5:4) MUST be zero — ALUA is skip-list.
	if r.Data[5]&0x30 != 0 {
		t.Fatalf("TPGS bits set in byte 5 (0x%02x) — ALUA is skip-list", r.Data[5])
	}
	// CmdQue (byte 7 bit 1) MUST be 1 per V2 tuning.
	if r.Data[7]&0x02 == 0 {
		t.Fatalf("CmdQue=0 in byte 7 (0x%02x); V2 tuning requires CmdQue=1", r.Data[7])
	}
}

// A caller-supplied short allocLen MUST truncate. Same SPC-5
// §6.6 rule as READ_CAPACITY(16): initiator allocation length
// is authoritative.
func TestT2Batch10_5_Inquiry_ShortAllocLen_Truncates(t *testing.T) {
	h := newHandlerForTest(t)
	cases := []uint16{0, 1, 8, 36, 64, 95}
	for _, al := range cases {
		var cdb [16]byte
		cdb[0] = iscsi.ScsiInquiry
		binary.BigEndian.PutUint16(cdb[3:5], al)
		r := h.HandleCommand(context.Background(), cdb, nil)
		if r.AsError() != nil {
			t.Fatalf("allocLen=%d: %v", al, r.AsError())
		}
		want := int(al)
		if al == 0 {
			// V2 + SPC convention: allocLen=0 is treated as the
			// natural default (we picked 96, V2 picked 36).
			// Either way the response must not exceed 96.
			want = 96
		}
		if len(r.Data) != want {
			t.Fatalf("allocLen=%d: returned %d bytes, want %d",
				al, len(r.Data), want)
		}
	}
}

func TestT2Batch10_5_InquiryVPD00_ReturnsSupportedPagesList(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), inquiryVPDCDB(0x00, 255), nil)
	if r.AsError() != nil {
		t.Fatalf("VPD 0x00: %v", r.AsError())
	}
	// Header(4) + page list.
	if len(r.Data) < 4 {
		t.Fatalf("len=%d want >=4", len(r.Data))
	}
	if r.Data[0] != 0x00 {
		t.Fatalf("device type=0x%02x want 0x00", r.Data[0])
	}
	if r.Data[1] != 0x00 {
		t.Fatalf("page code=0x%02x want 0x00", r.Data[1])
	}
	pageListLen := int(binary.BigEndian.Uint16(r.Data[2:4]))
	if pageListLen != len(r.Data)-4 {
		t.Fatalf("declared page-list length=%d != actual %d", pageListLen, len(r.Data)-4)
	}
	// Must advertise EXACTLY the implemented set per port plan §3.3 N3.
	// (The QA A-tier Phase 3 test asserts this strictly too; this
	// sw-side test pins the core shape.)
	got := r.Data[4 : 4+pageListLen]
	if !bytes.Equal(got, []byte{0x00, 0x80, 0x83}) {
		t.Fatalf("advertised pages=%x want {0x00, 0x80, 0x83}", got)
	}
}

func TestT2Batch10_5_InquiryVPD80_ReturnsSerial(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), inquiryVPDCDB(0x80, 255), nil)
	if r.AsError() != nil {
		t.Fatalf("VPD 0x80: %v", r.AsError())
	}
	if r.Data[0] != 0x00 || r.Data[1] != 0x80 {
		t.Fatalf("header: device=0x%02x page=0x%02x", r.Data[0], r.Data[1])
	}
	pageLen := int(binary.BigEndian.Uint16(r.Data[2:4]))
	if pageLen != len(r.Data)-4 {
		t.Fatalf("declared page length=%d != actual %d", pageLen, len(r.Data)-4)
	}
	// With cfg.SerialNo unset (newHandlerForTest passes no
	// SerialNo), the default derives from VolumeID per §3.3 N1
	// symmetry (QA review 2026-04-22). VolumeID="v1" → 16
	// lowercase hex chars. Assert shape only; the QA A-tier
	// determinism/collision test pins the exact invariant.
	serial := r.Data[4:]
	if len(serial) != 16 {
		t.Fatalf("serial length=%d want 16 (hex-encoded sha256[:8])", len(serial))
	}
	for _, b := range serial {
		// Must be lowercase hex.
		if !((b >= '0' && b <= '9') || (b >= 'a' && b <= 'f')) {
			t.Fatalf("serial %q contains non-hex byte 0x%02x", serial, b)
		}
	}
	// Explicitly pin the N1 invariant at this layer: serial must
	// NOT be the old "SWF00001" stub (regression guard).
	if bytes.HasPrefix(serial, []byte("SWF00001")) {
		t.Fatal("serial still uses old hardcoded stub; N1 symmetry broken")
	}
}

// Operator override path: cfg.SerialNo explicitly set is honored
// verbatim. Lets asset-tracked environments keep a specific
// serial (e.g. matching storage asset tags) without losing the
// default derivation for the common case.
func TestT2Batch10_5_InquiryVPD80_RespectsOperatorOverride(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{
		Backend:  rec,
		SerialNo: "ASSET-TAG-007",
	})
	r := h.HandleCommand(context.Background(), inquiryVPDCDB(0x80, 255), nil)
	if r.AsError() != nil {
		t.Fatalf("VPD 0x80: %v", r.AsError())
	}
	serial := r.Data[4:]
	if !bytes.Equal(serial, []byte("ASSET-TAG-007")) {
		t.Fatalf("serial %q did not honor operator override", serial)
	}
}

func TestT2Batch10_5_InquiryVPD83_HasNAA6Designator(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), inquiryVPDCDB(0x83, 255), nil)
	if r.AsError() != nil {
		t.Fatalf("VPD 0x83: %v", r.AsError())
	}
	// Header(4) + designator header(4) + NAA payload(8) = 16.
	if len(r.Data) < 16 {
		t.Fatalf("len=%d want >=16", len(r.Data))
	}
	if r.Data[0] != 0x00 || r.Data[1] != 0x83 {
		t.Fatalf("header page=0x%02x", r.Data[1])
	}
	// Designator 1 offset 4:
	//   [0] code set (1 = binary)
	//   [1] PIV/assoc/type — type=3 (NAA) in low nibble
	//   [2] reserved
	//   [3] identifier length = 8
	if r.Data[4] != 0x01 {
		t.Fatalf("designator code set=0x%02x want 0x01", r.Data[4])
	}
	if r.Data[5]&0x0f != 0x03 {
		t.Fatalf("designator type=0x%02x want NAA (3) in low nibble", r.Data[5])
	}
	if r.Data[7] != 0x08 {
		t.Fatalf("designator length=%d want 8", r.Data[7])
	}
	// NAA payload at offset 8, 8 bytes. High nibble of first
	// byte must be 0x6 (NAA-6 Registered Extended).
	naa := r.Data[8:16]
	if naa[0]>>4 != 0x6 {
		t.Fatalf("NAA high nibble=0x%x want 0x6", naa[0]>>4)
	}
}

func TestT2Batch10_5_InquiryVPDB0_NotImplemented(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), inquiryVPDCDB(0xB0, 255), nil)
	// Port plan §3.2: 0xB0 is explicitly NOT implemented.
	if r.Status == iscsi.StatusGood {
		t.Fatalf("VPD 0xB0 returned Good; port plan §3.2 says it must be rejected until durable backend lands")
	}
	if r.SenseKey != iscsi.SenseIllegalRequest {
		t.Fatalf("sense key=0x%02x want IllegalRequest", r.SenseKey)
	}
}

func TestT2Batch10_5_InquiryVPDB2_NotImplemented(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), inquiryVPDCDB(0xB2, 255), nil)
	if r.Status == iscsi.StatusGood {
		t.Fatalf("VPD 0xB2 returned Good; port plan §3.2 says it must be rejected")
	}
	if r.SenseKey != iscsi.SenseIllegalRequest {
		t.Fatalf("sense key=0x%02x want IllegalRequest", r.SenseKey)
	}
}

func TestT2Batch10_5_InquiryVPD_UnknownPage_Rejected(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), inquiryVPDCDB(0xFF, 255), nil)
	if r.Status == iscsi.StatusGood {
		t.Fatal("unknown VPD page returned Good")
	}
	if r.SenseKey != iscsi.SenseIllegalRequest {
		t.Fatalf("sense key=0x%02x want IllegalRequest", r.SenseKey)
	}
}

// READ_CAPACITY(16).

func readCap16CDB(allocLen uint32) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiServiceActionIn16
	cdb[1] = iscsi.SaiReadCapacity16
	binary.BigEndian.PutUint32(cdb[10:14], allocLen)
	return cdb
}

func TestT2Batch10_5_ReadCapacity16_ReturnsLastLBAAndBlockSize(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), readCap16CDB(32), nil)
	if r.AsError() != nil {
		t.Fatalf("READ_CAPACITY(16): %v", r.AsError())
	}
	if len(r.Data) != 32 {
		t.Fatalf("len=%d want 32", len(r.Data))
	}
	lastLBA := binary.BigEndian.Uint64(r.Data[0:8])
	if lastLBA != iscsi.DefaultVolumeBlocks-1 {
		t.Fatalf("last LBA=%d want %d", lastLBA, iscsi.DefaultVolumeBlocks-1)
	}
	blockSize := binary.BigEndian.Uint32(r.Data[8:12])
	if blockSize != iscsi.DefaultBlockSize {
		t.Fatalf("block size=%d want %d", blockSize, iscsi.DefaultBlockSize)
	}
	// LBPME (byte 14 bit 7) MUST be zero — Batch 10.5 does not
	// implement UNMAP / VPD 0xB2 (skip list §3.2), so advertising
	// provisioning management is an overclaim. PM High / architect
	// Medium finding 2026-04-22.
	if r.Data[14]&0x80 != 0 {
		t.Fatalf("LBPME bit set in byte 14 (0x%02x); must be 0 while UNMAP + VPD 0xB2 are skip-list",
			r.Data[14])
	}
}

// Architect Medium finding 2026-04-22: a short allocLen MUST
// truncate the 32-byte response, not be forced up. V2 parity
// applies to the response SHAPE (fields + byte positions),
// not to the length — caller-supplied allocLen is authoritative.
func TestT2Batch10_5_ReadCapacity16_ShortAllocLen_Truncates(t *testing.T) {
	h := newHandlerForTest(t)
	cases := []uint32{0, 1, 7, 8, 12, 20, 31}
	for _, al := range cases {
		r := h.HandleCommand(context.Background(), readCap16CDB(al), nil)
		if r.AsError() != nil {
			t.Fatalf("allocLen=%d: %v", al, r.AsError())
		}
		if uint32(len(r.Data)) != al {
			t.Fatalf("allocLen=%d: returned %d bytes — must truncate to %d",
				al, len(r.Data), al)
		}
	}
	// A caller asking for MORE than 32 bytes still gets 32
	// (server never pads beyond the natural response size).
	r := h.HandleCommand(context.Background(), readCap16CDB(100), nil)
	if len(r.Data) != 32 {
		t.Fatalf("allocLen=100: returned %d, want 32 (natural size cap)", len(r.Data))
	}
}

// READ(16) / WRITE(16) — same round-trip invariant as 10-byte
// variants, but with 64-bit LBA + 32-bit transferLen CDBs.

func write16CDB(lba uint64, transferLen uint32) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiWrite16
	binary.BigEndian.PutUint64(cdb[2:10], lba)
	binary.BigEndian.PutUint32(cdb[10:14], transferLen)
	return cdb
}

func read16CDB(lba uint64, transferLen uint32) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiRead16
	binary.BigEndian.PutUint64(cdb[2:10], lba)
	binary.BigEndian.PutUint32(cdb[10:14], transferLen)
	return cdb
}

func TestT2Batch10_5_Write16Read16_RoundTrip(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
	ctx := context.Background()

	payload := make([]byte, iscsi.DefaultBlockSize)
	copy(payload, []byte("batch10_5-write16"))
	r := h.HandleCommand(ctx, write16CDB(0, 1), payload)
	if r.AsError() != nil {
		t.Fatalf("WRITE(16): %v", r.AsError())
	}
	if rec.WriteCount() != 1 {
		t.Fatalf("backend WriteCount=%d want 1", rec.WriteCount())
	}
	if !bytes.Equal(rec.WriteAt(0).Data, payload) {
		t.Fatal("WRITE(16) payload mismatch at backend")
	}

	r = h.HandleCommand(ctx, read16CDB(0, 1), nil)
	if r.AsError() != nil {
		t.Fatalf("READ(16): %v", r.AsError())
	}
	if !bytes.Equal(r.Data, payload) {
		t.Fatalf("READ(16) data mismatch")
	}
}

func TestT2Batch10_5_Write16_LBAOutOfRange_Rejected(t *testing.T) {
	h := newHandlerForTest(t)
	// SLBA = volume block count → past end.
	r := h.HandleCommand(context.Background(),
		write16CDB(iscsi.DefaultVolumeBlocks, 1),
		make([]byte, iscsi.DefaultBlockSize))
	if r.Status == iscsi.StatusGood {
		t.Fatal("WRITE(16) LBA past end: Good (must fail)")
	}
	if r.ASC != iscsi.ASCLBAOutOfRange {
		t.Fatalf("ASC=0x%02x want LBAOutOfRange", r.ASC)
	}
}

func TestT2Batch10_5_Read16_LBAOutOfRange_Rejected(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(),
		read16CDB(iscsi.DefaultVolumeBlocks, 1), nil)
	if r.Status == iscsi.StatusGood {
		t.Fatal("READ(16) LBA past end: Good (must fail)")
	}
	if r.ASC != iscsi.ASCLBAOutOfRange {
		t.Fatalf("ASC=0x%02x want LBAOutOfRange", r.ASC)
	}
}

// ReadCapacity10 should still work alongside the new (16) variant.
// Regression guard that the ckpt 2 code path wasn't broken by the
// dispatch changes.
func TestT2Batch10_5_ReadCapacity10_StillGreen(t *testing.T) {
	h := newHandlerForTest(t)
	var cdb [16]byte
	cdb[0] = iscsi.ScsiReadCapacity10
	r := h.HandleCommand(context.Background(), cdb, nil)
	if r.AsError() != nil {
		t.Fatalf("READ_CAPACITY(10) regression: %v", r.AsError())
	}
	if len(r.Data) != 8 {
		t.Fatalf("len=%d want 8", len(r.Data))
	}
}
