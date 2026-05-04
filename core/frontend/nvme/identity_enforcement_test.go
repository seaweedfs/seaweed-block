// Ownership: sw regression tests (architect findings 2026-04-21
// round-7 review): pin two NVMe identity-enforcement gaps:
//
//  1. cmd/blockvolume --nvme-ns flag was dead. Test the wiring
//     by constructing a Target with HandlerConfig.NSID=N and
//     confirming an IO command at NSID != N is rejected.
//  2. cmd/blockvolume --nvme-subsysnqn was advertised but not
//     enforced. Test that a Connect with the WRONG SubNQN is
//     rejected with SCT=CommandSpecific SC=ConnectInvalidParameters.
package nvme_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// Finding 1 — handler-level: an IOHandler built with NSID=7
// must reject IO commands targeting any other NSID, including
// the previous default (1).
func TestT2NVMe_HandlerHonorsConfiguredNSID(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: rec, NSID: 7})
	if got := h.NSID(); got != 7 {
		t.Fatalf("handler NSID=%d want 7 (config not honored)", got)
	}
	r := h.Handle(context.Background(), nvme.IOCommand{
		Opcode: 0x01 /* Write */, NSID: 1, SLBA: 0, NLB: 1,
		Data: make([]byte, nvme.DefaultBlockSize),
	})
	if r.SCT != nvme.SCTGeneric || r.SC != nvme.SCInvalidField {
		t.Fatalf("NSID=1 against handler NSID=7: SCT=0x%x SC=0x%02x want Generic/InvalidField",
			r.SCT, r.SC)
	}
	if rec.WriteCount() != 0 {
		t.Fatal("backend invoked despite NSID mismatch")
	}
}

// Finding 1 — end-to-end via target: a Target built with
// HandlerConfig.NSID=7 must propagate through to the wire-
// level dispatcher. Today's Go client always uses NSID=1, so
// this test confirms the rejection path. (A complementary
// test using NSID=7 on the client confirms acceptance.)
func TestT2Process_NVMe_TargetHandlerNSIDConfigPropagates(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	prov := testback.NewStaticProvider(rec)

	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
		Handler:   nvme.HandlerConfig{NSID: 7}, // non-default
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tg.Close()

	// dialAndConnect uses the canonical SubNQN; our test client
	// always issues commands with NSID=1, so on a NSID=7 target
	// the Write must be rejected at the handler with InvalidField
	// (SCT=0 SC=0x02 = wire 0x8004 with DNR=1).
	cli := dialAndConnect(t, addr)
	defer cli.close()
	status := cli.writeCmd(t, 0, 1, make([]byte, nvme.DefaultBlockSize))
	if status == 0 {
		t.Fatal("Write at NSID=1 to NSID=7 target succeeded — config not enforced on wire")
	}
	gotSCT := uint8((status >> 9) & 0x07)
	gotSC := uint8((status >> 1) & 0xFF)
	if gotSCT != nvme.SCTGeneric || gotSC != nvme.SCInvalidField {
		t.Fatalf("NSID mismatch wire status=0x%04x SCT=%d SC=0x%02x want Generic/InvalidField",
			status, gotSCT, gotSC)
	}
	if rec.WriteCount() != 0 {
		t.Fatal("backend invoked despite NSID mismatch over wire")
	}
}

// Finding 2 — Fabric Connect with the wrong SubNQN must be
// rejected. Mismatched-SubNQN host should never be admitted.
//
// Uses a low-level capsule write to control the Connect's
// SubNQN field (the standard test client uses the canonical
// subsystem name).
func TestT2Process_NVMe_ConnectWithWrongSubNQN_Rejected(t *testing.T) {
	prov := testback.NewStaticProvider(
		testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"}))

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
	defer tg.Close()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	r := nvme.NewReader(conn)
	w := nvme.NewWriter(conn)

	// ICReq → ICResp.
	ic := nvme.ICRequest{PDUFormatVersion: 0, PDUMaxR2T: 1}
	if err := w.SendHeaderOnly(0x0 /* pduICReq */, &ic, 120); err != nil {
		t.Fatalf("send ICReq: %v", err)
	}
	if _, err := r.Dequeue(); err != nil {
		t.Fatalf("read ICResp hdr: %v", err)
	}
	var icResp nvme.ICResponse
	if err := r.Receive(&icResp); err != nil {
		t.Fatalf("read ICResp body: %v", err)
	}

	// Fabric Connect with an INTENTIONALLY wrong SubNQN.
	cmd := nvme.CapsuleCommand{
		OpCode: 0x7F, // adminFabric
		FCType: 0x01, // fcConnect
		CID:    1,
	}
	cd := nvme.ConnectData{
		HostID:  [16]byte{0x9, 0x9, 0x9},
		CNTLID:  0xFFFF,
		SubNQN:  "nqn.2026-04.example.v3:WRONG-subsys",
		HostNQN: "nqn.2026-04.example.host:bad",
	}
	cdBuf := make([]byte, 1024)
	cd.Marshal(cdBuf)
	if err := w.SendWithData(0x4 /* pduCapsuleCmd */, 0, &cmd, 64, cdBuf); err != nil {
		t.Fatalf("send Connect: %v", err)
	}

	if _, err := r.Dequeue(); err != nil {
		t.Fatalf("read CapsuleResp hdr: %v", err)
	}
	var resp nvme.CapsuleResponse
	if err := r.Receive(&resp); err != nil {
		t.Fatalf("read CapsuleResp body: %v", err)
	}
	if resp.Status == 0 {
		t.Fatal("Connect with wrong SubNQN returned Success — subsys identity NOT enforced")
	}
	gotSCT := uint8((resp.Status >> 9) & 0x07)
	gotSC := uint8((resp.Status >> 1) & 0xFF)
	if gotSCT != nvme.SCTCommandSpecific || gotSC != nvme.SCConnectInvalidParameters {
		t.Fatalf("wrong SubNQN status=0x%04x SCT=%d SC=0x%02x want CommandSpecific/ConnectInvalidParameters (1/0x80)",
			resp.Status, gotSCT, gotSC)
	}
}

// Confirms the in-process L1 path (target with no SubsysNQN
// configured) still accepts any SubNQN — preserves the
// "empty = no enforcement" contract documented on the
// expectedSubNQN field.
func TestT2NVMe_EmptySubsysNQN_AcceptsAnyConnect(t *testing.T) {
	prov := testback.NewStaticProvider(
		testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"}))

	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:   "127.0.0.1:0",
		VolumeID: "v1",
		Provider: prov,
		// SubsysNQN intentionally empty.
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tg.Close()
	cli := dialAndConnect(t, addr)
	defer cli.close()
	// dialAndConnect succeeded → Connect was admitted. Sanity-
	// check IO works.
	status := cli.writeCmd(t, 0, 1, make([]byte, nvme.DefaultBlockSize))
	expectStatusSuccess(t, status, "Write under empty-SubsysNQN target")
}
