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

type testALUAProvider struct {
	state iscsi.ALUAState
	tpgID uint16
	rtpID uint16
	naa   [8]byte
}

func (p *testALUAProvider) ALUAState() iscsi.ALUAState   { return p.state }
func (p *testALUAProvider) TargetPortGroupID() uint16    { return p.tpgID }
func (p *testALUAProvider) RelativeTargetPortID() uint16 { return p.rtpID }
func (p *testALUAProvider) DeviceNAA() [8]byte           { return p.naa }

func aluaTestProvider(state iscsi.ALUAState) *testALUAProvider {
	return &testALUAProvider{
		state: state,
		tpgID: 7,
		rtpID: 3,
		naa:   [8]byte{0x60, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0x11, 0x22},
	}
}

func inquiryCDB(alloc uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiInquiry
	binary.BigEndian.PutUint16(cdb[3:5], alloc)
	return cdb
}

func vpdCDB(page uint8, alloc uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiInquiry
	cdb[1] = 0x01
	cdb[2] = page
	binary.BigEndian.PutUint16(cdb[3:5], alloc)
	return cdb
}

func reportTPGCDB(alloc uint32) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiMaintenanceIn
	cdb[1] = iscsi.SaiReportTargetPortGroups
	binary.BigEndian.PutUint32(cdb[6:10], alloc)
	return cdb
}

func syncCache10CDB() [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiSyncCache10
	return cdb
}

func TestP6ALUA_NoProviderDoesNotAdvertiseALUA(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})

	inq := h.HandleCommand(context.Background(), inquiryCDB(96), nil)
	if inq.Status != iscsi.StatusGood {
		t.Fatalf("INQUIRY: %v", inq.AsError())
	}
	if got := inq.Data[5] & 0x30; got != 0 {
		t.Fatalf("TPGS bits with no ALUA provider = 0x%02x, want 0", got)
	}

	rtpg := h.HandleCommand(context.Background(), reportTPGCDB(64), nil)
	if rtpg.Status == iscsi.StatusGood {
		t.Fatal("REPORT TARGET PORT GROUPS succeeded without ALUA provider")
	}
	if rtpg.SenseKey != iscsi.SenseIllegalRequest {
		t.Fatalf("REPORT TPG no provider sense=0x%02x want IllegalRequest", rtpg.SenseKey)
	}

	vpd := h.HandleCommand(context.Background(), vpdCDB(0x83, 255), nil)
	if vpd.Status != iscsi.StatusGood {
		t.Fatalf("VPD83 no provider: %v", vpd.AsError())
	}
	if pageLen := binary.BigEndian.Uint16(vpd.Data[2:4]); pageLen != 12 {
		t.Fatalf("VPD83 no provider pageLen=%d want 12", pageLen)
	}
}

func TestP6ALUA_NonWritableStatesAllowReadButRejectWriteAndSync(t *testing.T) {
	for _, state := range []iscsi.ALUAState{
		iscsi.ALUAStandby,
		iscsi.ALUAUnavailable,
		iscsi.ALUATransitioning,
	} {
		t.Run(state.String(), func(t *testing.T) {
			rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
			payload := make([]byte, iscsi.DefaultBlockSize)
			if _, err := rec.Write(context.Background(), 0, payload); err != nil {
				t.Fatalf("seed Write: %v", err)
			}
			prov := aluaTestProvider(state)
			h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec, ALUA: prov})

			read := h.HandleCommand(context.Background(), readCDB(0, 1), nil)
			if read.Status != iscsi.StatusGood {
				t.Fatalf("READ should support path probing: %v", read.AsError())
			}

			write := h.HandleCommand(context.Background(), writeCDB(0, 1), payload)
			if write.Status != iscsi.StatusCheckCondition {
				t.Fatalf("WRITE status=0x%02x want CheckCondition", write.Status)
			}
			if write.SenseKey != iscsi.SenseNotReady || write.ASC != iscsi.ASCNotReady || write.ASCQ != iscsi.ASCQTargetPortStandby {
				t.Fatalf("WRITE sense=%02x/%02x/%02x want NotReady/04/0B",
					write.SenseKey, write.ASC, write.ASCQ)
			}

			sync := h.HandleCommand(context.Background(), syncCache10CDB(), nil)
			if sync.Status != iscsi.StatusCheckCondition {
				t.Fatalf("SYNC status=0x%02x want CheckCondition", sync.Status)
			}
			if rec.SyncCount() != 0 {
				t.Fatalf("SYNC reached backend %d times", rec.SyncCount())
			}
		})
	}
}

func TestP6ALUA_ActiveAdvertisesTPGSAndServesReportTPG(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	prov := aluaTestProvider(iscsi.ALUAActiveOptimized)
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec, ALUA: prov})

	inq := h.HandleCommand(context.Background(), inquiryCDB(96), nil)
	if inq.Status != iscsi.StatusGood {
		t.Fatalf("INQUIRY: %v", inq.AsError())
	}
	if got := inq.Data[5] & 0x30; got != 0x10 {
		t.Fatalf("TPGS bits = 0x%02x want 0x10 implicit ALUA", got)
	}

	rtpg := h.HandleCommand(context.Background(), reportTPGCDB(64), nil)
	if rtpg.Status != iscsi.StatusGood {
		t.Fatalf("REPORT TPG: %v", rtpg.AsError())
	}
	if len(rtpg.Data) != 16 {
		t.Fatalf("REPORT TPG len=%d want 16", len(rtpg.Data))
	}
	// Layout per SPC-4 §6.27 Table 175 (parameter list header + descriptor):
	//   [4]   AAS, [5] support flags, [6:8] Target Port Group,
	//   [9]   status code, [11] target port count,
	//   [14:16] Relative Target Port Identifier.
	if state := iscsi.ALUAState(rtpg.Data[4] & 0x0f); state != iscsi.ALUAActiveOptimized {
		t.Fatalf("REPORT TPG state=%02x want active optimized", state)
	}
	if tpg := binary.BigEndian.Uint16(rtpg.Data[6:8]); tpg != prov.tpgID {
		t.Fatalf("REPORT TPG id=%d want %d", tpg, prov.tpgID)
	}
	if status := rtpg.Data[9]; status != 0 {
		t.Fatalf("REPORT TPG status code=0x%02x want 0 (no transition)", status)
	}
	if rtp := binary.BigEndian.Uint16(rtpg.Data[14:16]); rtp != prov.rtpID {
		t.Fatalf("REPORT TPG relative target port=%d want %d", rtp, prov.rtpID)
	}
}

func TestP6ALUA_VPD83AddsPathDescriptorsAndTruncates(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	prov := aluaTestProvider(iscsi.ALUAActiveOptimized)
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec, ALUA: prov})

	vpd := h.HandleCommand(context.Background(), vpdCDB(0x83, 255), nil)
	if vpd.Status != iscsi.StatusGood {
		t.Fatalf("VPD83: %v", vpd.AsError())
	}
	if pageLen := binary.BigEndian.Uint16(vpd.Data[2:4]); pageLen != 28 {
		t.Fatalf("VPD83 pageLen=%d want 28", pageLen)
	}
	if got := vpd.Data[8:16]; !bytes.Equal(got, prov.naa[:]) {
		t.Fatalf("VPD83 NAA=%x want %x", got, prov.naa)
	}
	if typ := vpd.Data[17] & 0x0f; typ != 0x05 {
		t.Fatalf("VPD83 second descriptor type=0x%x want target port group", typ)
	}
	if typ := vpd.Data[25] & 0x0f; typ != 0x04 {
		t.Fatalf("VPD83 third descriptor type=0x%x want relative target port", typ)
	}

	short := h.HandleCommand(context.Background(), vpdCDB(0x83, 20), nil)
	if short.Status != iscsi.StatusGood {
		t.Fatalf("short VPD83: %v", short.AsError())
	}
	if len(short.Data) != 20 {
		t.Fatalf("short VPD83 len=%d want 20", len(short.Data))
	}
}

func TestP6ALUA_VPD00UnchangedWhenALUAEnabled(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	noALUA := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec})
	withALUA := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec, ALUA: aluaTestProvider(iscsi.ALUAActiveOptimized)})

	a := noALUA.HandleCommand(context.Background(), vpdCDB(0x00, 255), nil)
	b := withALUA.HandleCommand(context.Background(), vpdCDB(0x00, 255), nil)
	if a.Status != iscsi.StatusGood || b.Status != iscsi.StatusGood {
		t.Fatalf("VPD00 statuses noALUA=%v withALUA=%v", a.AsError(), b.AsError())
	}
	if !bytes.Equal(a.Data, b.Data) {
		t.Fatalf("VPD00 changed when ALUA enabled: noALUA=%x withALUA=%x", a.Data, b.Data)
	}
}

func TestP6ALUA_ReportTPGReflectsStateChanges(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	prov := aluaTestProvider(iscsi.ALUAActiveOptimized)
	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: rec, ALUA: prov})

	for _, state := range []iscsi.ALUAState{
		iscsi.ALUAActiveOptimized,
		iscsi.ALUAActiveNonOptimized,
		iscsi.ALUAStandby,
		iscsi.ALUAUnavailable,
		iscsi.ALUATransitioning,
	} {
		prov.state = state
		rtpg := h.HandleCommand(context.Background(), reportTPGCDB(64), nil)
		if rtpg.Status != iscsi.StatusGood {
			t.Fatalf("REPORT TPG state %02x: %v", state, rtpg.AsError())
		}
		if got := iscsi.ALUAState(rtpg.Data[4] & 0x0f); got != state {
			t.Fatalf("REPORT TPG state=%02x want %02x", got, state)
		}
	}
}
