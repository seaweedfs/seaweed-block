// Ownership: sw unit tests for Batch 11b Fabric Property
// Get/Set (CAP/VS/CC/CSTS). Port plan §4 Batch 11b.
//
// Pins:
//   - CAP is 8-byte, returns MQES=63 + CQR=1 + TO=2 + CSS bit 0.
//   - VS is 4-byte, equals NVMe 1.3 (0x00010300).
//   - CC.EN flip is deterministic — next PropertyGet(CSTS) sees
//     RDY=1. Pins QA constraint #3 (<1s upper bound).
//   - Read-only writes + size/offset mismatches return
//     InvalidParameters.
package nvme_test

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

const (
	regCAP  uint32 = 0x00
	regVS   uint32 = 0x08
	regCC   uint32 = 0x14
	regCSTS uint32 = 0x1C
)

func newPropertyHarness(t *testing.T) (*nvme.Target, *nvmeClient) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	prov := testback.NewStaticProvider(rec)
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cli := dialAndConnectOpts(t, addr, connectOptions{SkipIOQueue: true})
	return tg, cli
}

func TestT2Batch11b_PropertyGet_CAP(t *testing.T) {
	tg, cli := newPropertyHarness(t)
	defer tg.Close()
	defer cli.close()
	status, val := cli.adminPropertyGet(t, regCAP, true)
	expectStatusSuccess(t, status, "PropertyGet(CAP)")
	if mqes := val & 0xFFFF; mqes != 63 {
		t.Errorf("CAP.MQES=%d want 63", mqes)
	}
	if cqr := (val >> 16) & 0x1; cqr != 1 {
		t.Errorf("CAP.CQR=%d want 1 (required for NVMe/TCP)", cqr)
	}
	if to := (val >> 24) & 0xFF; to != 2 {
		t.Errorf("CAP.TO=%d want 2 (1 second)", to)
	}
	if css := (val >> 37) & 0x1; css != 1 {
		t.Errorf("CAP.CSS bit0=%d want 1 (NVM command set)", css)
	}
}

func TestT2Batch11b_PropertyGet_VS_Is13(t *testing.T) {
	tg, cli := newPropertyHarness(t)
	defer tg.Close()
	defer cli.close()
	status, val := cli.adminPropertyGet(t, regVS, false)
	expectStatusSuccess(t, status, "PropertyGet(VS)")
	if uint32(val) != 0x00010300 {
		t.Errorf("VS=0x%08x want 0x00010300 (NVMe 1.3)", val)
	}
}

func TestT2Batch11b_PropertyGet_CC_BootsAsZero(t *testing.T) {
	tg, cli := newPropertyHarness(t)
	defer tg.Close()
	defer cli.close()
	status, val := cli.adminPropertyGet(t, regCC, false)
	expectStatusSuccess(t, status, "PropertyGet(CC)")
	if val != 0 {
		t.Errorf("CC=0x%08x want 0 at boot", val)
	}
}

func TestT2Batch11b_PropertyGet_CSTS_BootsAsNotReady(t *testing.T) {
	tg, cli := newPropertyHarness(t)
	defer tg.Close()
	defer cli.close()
	status, val := cli.adminPropertyGet(t, regCSTS, false)
	expectStatusSuccess(t, status, "PropertyGet(CSTS)")
	if rdy := val & 0x1; rdy != 0 {
		t.Errorf("CSTS.RDY=%d at boot want 0", rdy)
	}
}

// TestT2Batch11b_CCEnableFlipsRDY pins QA constraint #3: after
// PropertySet(CC, EN=1) the very next PropertyGet(CSTS) sees
// RDY=1. No sleep, no retry — the flip is synchronous.
func TestT2Batch11b_CCEnableFlipsRDY(t *testing.T) {
	tg, cli := newPropertyHarness(t)
	defer tg.Close()
	defer cli.close()
	if status := cli.adminPropertySet(t, regCC, false, 0x1); status != 0 {
		t.Fatalf("PropertySet(CC, EN=1) status=0x%04x", status)
	}
	status, val := cli.adminPropertyGet(t, regCSTS, false)
	expectStatusSuccess(t, status, "PropertyGet(CSTS)")
	if rdy := val & 0x1; rdy != 1 {
		t.Errorf("CSTS.RDY=%d after CC.EN=1 — flip not synchronous (QA #3)", rdy)
	}

	// Flip back.
	if status := cli.adminPropertySet(t, regCC, false, 0); status != 0 {
		t.Fatalf("PropertySet(CC, EN=0) status=0x%04x", status)
	}
	_, val = cli.adminPropertyGet(t, regCSTS, false)
	if rdy := val & 0x1; rdy != 0 {
		t.Errorf("CSTS.RDY=%d after CC.EN=0 — RDY didn't clear", rdy)
	}
}

func TestT2Batch11b_PropertySet_ReadOnlyRejected(t *testing.T) {
	tg, cli := newPropertyHarness(t)
	defer tg.Close()
	defer cli.close()
	// CAP is read-only.
	status := cli.adminPropertySet(t, regCAP, true, 0xDEAD)
	if status == 0 {
		t.Error("PropertySet(CAP) succeeded — CAP is read-only")
	}
	// VS is read-only.
	status = cli.adminPropertySet(t, regVS, false, 0xBEEF)
	if status == 0 {
		t.Error("PropertySet(VS) succeeded — VS is read-only")
	}
	// CSTS is read-only.
	status = cli.adminPropertySet(t, regCSTS, false, 0x1)
	if status == 0 {
		t.Error("PropertySet(CSTS) succeeded — CSTS is read-only")
	}
}

func TestT2Batch11b_PropertyGet_SizeMismatchRejected(t *testing.T) {
	tg, cli := newPropertyHarness(t)
	defer tg.Close()
	defer cli.close()
	// CAP must be 8-byte; 4-byte read must fail.
	status, _ := cli.adminPropertyGet(t, regCAP, false)
	if status == 0 {
		t.Error("PropertyGet(CAP, size=4) succeeded — CAP is 8-byte only")
	}
	// VS must be 4-byte; 8-byte read must fail.
	status, _ = cli.adminPropertyGet(t, regVS, true)
	if status == 0 {
		t.Error("PropertyGet(VS, size=8) succeeded — VS is 4-byte only")
	}
}

func TestT2Batch11b_PropertyGet_UnknownOffsetRejected(t *testing.T) {
	tg, cli := newPropertyHarness(t)
	defer tg.Close()
	defer cli.close()
	status, _ := cli.adminPropertyGet(t, 0x40 /* not a defined register */, false)
	if status == 0 {
		t.Error("PropertyGet(unknown offset) succeeded")
	}
}
