// Ownership: sw unit tests for Batch 11b Set Features / Get
// Features / KeepAlive handlers. Port plan §4 Batch 11b.
//
// Scope pins:
//   - Feature 0x07 (Number of Queues) grants back the asked
//     value (up to the cap) in CapsuleResp DW0; Get Features
//     returns the same value.
//   - Feature 0x0F (Keep Alive Timer) is stored and round-
//     tripped. Per QA constraint #2 there is no timer — we
//     don't test fatal-timeout behavior because there is none.
//   - KeepAlive (opcode 0x18) always returns Success.
//   - Unknown FIDs return InvalidField.
package nvme_test

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

func newFeaturesHarness(t *testing.T) (*nvme.Target, *nvmeClient) {
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

func TestT2Batch11b_SetFeatures_NumberOfQueues_GrantsRequested(t *testing.T) {
	tg, cli := newFeaturesHarness(t)
	defer tg.Close()
	defer cli.close()
	// Request NSQR=0, NCQR=0 (Linux default — one IO queue pair).
	status, granted := cli.adminSetFeatures(t, 0x07, 0)
	expectStatusSuccess(t, status, "SetFeatures(NumberOfQueues)")
	if granted != 0 {
		t.Errorf("granted=0x%08x want 0", granted)
	}
	// Get Features round-trip.
	status, gv := cli.adminGetFeatures(t, 0x07, 0)
	expectStatusSuccess(t, status, "GetFeatures(NumberOfQueues)")
	if gv != 0 {
		t.Errorf("GetFeatures returned 0x%08x want 0", gv)
	}
}

func TestT2Batch11b_SetFeatures_NumberOfQueues_Capped(t *testing.T) {
	tg, cli := newFeaturesHarness(t)
	defer tg.Close()
	defer cli.close()
	// Request 65535 SQ + 65535 CQ — target caps both at 7.
	status, granted := cli.adminSetFeatures(t, 0x07, 0xFFFFFFFF)
	expectStatusSuccess(t, status, "SetFeatures")
	nsqr := granted & 0xFFFF
	ncqr := (granted >> 16) & 0xFFFF
	if nsqr != 7 || ncqr != 7 {
		t.Errorf("granted NSQR=%d NCQR=%d want 7/7 (cap)", nsqr, ncqr)
	}
}

func TestT2Batch11b_SetFeatures_KeepAliveTimer_RoundTrips(t *testing.T) {
	tg, cli := newFeaturesHarness(t)
	defer tg.Close()
	defer cli.close()
	// Set KATO = 120 (Linux default after Connect is typically
	// 120 * 100ms = 12s).
	status, _ := cli.adminSetFeatures(t, 0x0F, 120)
	expectStatusSuccess(t, status, "SetFeatures(KATO)")
	status, gv := cli.adminGetFeatures(t, 0x0F, 0)
	expectStatusSuccess(t, status, "GetFeatures(KATO)")
	if gv != 120 {
		t.Errorf("GetFeatures(KATO)=%d want 120 (round-trip)", gv)
	}
}

func TestT2Batch11b_SetFeatures_UnknownFIDRejected(t *testing.T) {
	tg, cli := newFeaturesHarness(t)
	defer tg.Close()
	defer cli.close()
	status, _ := cli.adminSetFeatures(t, 0xFE /* undefined */, 0)
	if status == 0 {
		t.Error("SetFeatures(unknown FID) succeeded — must reject")
	}
}

func TestT2Batch11b_GetFeatures_UnknownFIDRejected(t *testing.T) {
	tg, cli := newFeaturesHarness(t)
	defer tg.Close()
	defer cli.close()
	status, _ := cli.adminGetFeatures(t, 0xFE, 0)
	if status == 0 {
		t.Error("GetFeatures(unknown FID) succeeded — must reject")
	}
}

func TestT2Batch11b_KeepAlive_AlwaysSuccess(t *testing.T) {
	tg, cli := newFeaturesHarness(t)
	defer tg.Close()
	defer cli.close()
	// Multiple KeepAlives in a row — no rate limiting, no state
	// tracked (per QA constraint #2).
	for i := 0; i < 3; i++ {
		if s := cli.adminKeepAlive(t); s != 0 {
			t.Errorf("KeepAlive #%d status=0x%04x want 0", i, s)
		}
	}
}

// TestT2Batch11b_IdentifyController_KAS_One pins the "advertised
// ≡ implemented" flip: now that KeepAlive is real, Identify
// Controller advertises KAS=1 (100ms granularity).
func TestT2Batch11b_IdentifyController_KAS_One(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	if len(data) != 4096 {
		t.Fatalf("len=%d want 4096", len(data))
	}
	kas := uint16(data[320]) | uint16(data[321])<<8
	if kas != 1 {
		t.Errorf("Identify Controller KAS=%d want 1 (100ms) in 11b", kas)
	}
}
