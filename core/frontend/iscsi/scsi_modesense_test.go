// Ownership: sw port from V2 scsi_test.go (mode-sense subset).
// Batch 10.5 — port plan §4, locked 2026-04-22.
//
// Covers MODE_SENSE(6/10) + page 0x08 (Caching) + page 0x0A
// (Control) + page 0x3F (all) + unsupported page path.
// MODE_SELECT(6/10) and START_STOP_UNIT are trivial Good-stubs;
// one case each confirms the dispatch.
package iscsi_test

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// modeSense6CDB builds a MODE_SENSE(6) CDB for pageCode with
// allocation length 4 (small default) or the caller-supplied
// allocLen when non-zero.
func modeSense6CDB(pageCode uint8, allocLen uint8) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiModeSense6
	cdb[2] = pageCode // PC=0 + page code
	cdb[4] = allocLen
	return cdb
}

func modeSense10CDB(pageCode uint8, allocLen uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiModeSense10
	cdb[2] = pageCode
	binary.BigEndian.PutUint16(cdb[7:9], allocLen)
	return cdb
}

func newHandlerForTest(t *testing.T) *iscsi.SCSIHandler {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	return iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
}

func TestT2Batch10_5_ModeSense6_Page08Caching_ReturnsWCEOn(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), modeSense6CDB(0x08, 64), nil)
	if r.AsError() != nil {
		t.Fatalf("MODE_SENSE(6) page 0x08: %v", r.AsError())
	}
	// 4-byte header + 20-byte page.
	if len(r.Data) < 24 {
		t.Fatalf("len=%d want >=24", len(r.Data))
	}
	// Header byte 0 = mode data length.
	if r.Data[0] != 23 { // 3 + len(pages=20)
		t.Fatalf("mode data length=%d want 23", r.Data[0])
	}
	// Page 0x08 body starts at byte 4. Byte 2 of page = WCE|RCD flags.
	if r.Data[4] != 0x08 {
		t.Fatalf("page code=0x%02x want 0x08", r.Data[4])
	}
	if r.Data[6]&0x04 == 0 {
		t.Fatalf("WCE bit not set in page 0x08 (byte=%02x) — V2 parity lost", r.Data[6])
	}
}

func TestT2Batch10_5_ModeSense6_Page0AControl_MinimalBody(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), modeSense6CDB(0x0a, 64), nil)
	if r.AsError() != nil {
		t.Fatalf("MODE_SENSE(6) page 0x0A: %v", r.AsError())
	}
	// Expect header(4) + page(12) = 16.
	if len(r.Data) < 16 {
		t.Fatalf("len=%d want >=16", len(r.Data))
	}
	if r.Data[4] != 0x0a {
		t.Fatalf("page code=0x%02x want 0x0A", r.Data[4])
	}
}

func TestT2Batch10_5_ModeSense6_Page3FAll_ReturnsCachingAndControl(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), modeSense6CDB(0x3f, 255), nil)
	if r.AsError() != nil {
		t.Fatalf("MODE_SENSE(6) page 0x3F: %v", r.AsError())
	}
	// Header(4) + page08(20) + page0A(12) = 36.
	if len(r.Data) < 36 {
		t.Fatalf("len=%d want >=36", len(r.Data))
	}
	// page 0x08 first (at offset 4), then 0x0A at offset 24.
	if r.Data[4] != 0x08 {
		t.Fatalf("first page code=0x%02x want 0x08", r.Data[4])
	}
	if r.Data[24] != 0x0a {
		t.Fatalf("second page code=0x%02x want 0x0A", r.Data[24])
	}
}

func TestT2Batch10_5_ModeSense6_UnknownPage_ReturnsEmptyPages(t *testing.T) {
	h := newHandlerForTest(t)
	// Page 0x01 (not implemented) — V2 returns empty (just header).
	r := h.HandleCommand(context.Background(), modeSense6CDB(0x01, 64), nil)
	if r.AsError() != nil {
		t.Fatalf("MODE_SENSE(6) unknown page: %v", r.AsError())
	}
	if len(r.Data) != 4 {
		t.Fatalf("unknown page: len=%d want 4 (header only)", len(r.Data))
	}
}

func TestT2Batch10_5_ModeSense6_TruncatesToAllocLen(t *testing.T) {
	h := newHandlerForTest(t)
	// allocLen=8 — should truncate the page 0x08 response.
	r := h.HandleCommand(context.Background(), modeSense6CDB(0x08, 8), nil)
	if r.AsError() != nil {
		t.Fatalf("err: %v", r.AsError())
	}
	if len(r.Data) != 8 {
		t.Fatalf("len=%d want 8 (truncated)", len(r.Data))
	}
}

func TestT2Batch10_5_ModeSense10_Page08_10ByteHeader(t *testing.T) {
	h := newHandlerForTest(t)
	r := h.HandleCommand(context.Background(), modeSense10CDB(0x08, 64), nil)
	if r.AsError() != nil {
		t.Fatalf("MODE_SENSE(10) page 0x08: %v", r.AsError())
	}
	// Header(8) + page08(20) = 28.
	if len(r.Data) < 28 {
		t.Fatalf("len=%d want >=28", len(r.Data))
	}
	mdl := binary.BigEndian.Uint16(r.Data[0:2])
	if mdl != 26 { // 6 + len(pages=20)
		t.Fatalf("mode data length=%d want 26", mdl)
	}
	// Page 0x08 starts at offset 8 in MODE_SENSE(10).
	if r.Data[8] != 0x08 {
		t.Fatalf("page code=0x%02x want 0x08 at offset 8", r.Data[8])
	}
}

func TestT2Batch10_5_ModeSelect6_AcceptsAsNoopGood(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
	var cdb [16]byte
	cdb[0] = iscsi.ScsiModeSelect6
	r := h.HandleCommand(context.Background(), cdb, []byte{0, 0, 0, 0})
	if r.Status != iscsi.StatusGood {
		t.Fatalf("MODE_SELECT(6): status=0x%02x want Good", r.Status)
	}
	if rec.WriteCount() != 0 || rec.ReadCount() != 0 {
		t.Fatal("MODE_SELECT(6) reached backend; should be local no-op")
	}
}

func TestT2Batch10_5_ModeSelect10_AcceptsAsNoopGood(t *testing.T) {
	h := newHandlerForTest(t)
	var cdb [16]byte
	cdb[0] = iscsi.ScsiModeSelect10
	r := h.HandleCommand(context.Background(), cdb, []byte{0, 0, 0, 0, 0, 0, 0, 0})
	if r.Status != iscsi.StatusGood {
		t.Fatalf("MODE_SELECT(10): status=0x%02x want Good", r.Status)
	}
}

func TestT2Batch10_5_StartStopUnit_AcceptsAsNoopGood(t *testing.T) {
	h := newHandlerForTest(t)
	var cdb [16]byte
	cdb[0] = iscsi.ScsiStartStopUnit
	r := h.HandleCommand(context.Background(), cdb, nil)
	if r.Status != iscsi.StatusGood {
		t.Fatalf("START_STOP_UNIT: status=0x%02x want Good", r.Status)
	}
}
