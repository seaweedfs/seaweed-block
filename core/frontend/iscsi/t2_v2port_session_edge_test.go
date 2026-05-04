// Ownership: QA (from v3-phase-15-t2-v2-test-audit.md A-tier Phase 2 rewrite).
// sw may NOT modify this file without architect approval via §8B.4
// Discovery Bridge. See: sw-block/design/v3-phase-15-t2-v2-test-audit.md §5.
//
// V2 intent source:
//   - weed/storage/blockvol/iscsi/qa_test.go sub-tests:
//     * "discovery_then_scsi"       → TestT2V2Port_DiscoverySession_SCSICmd_ClosesConnection (A7)
//     * "rapid_fire_100"            → TestT2V2Port_RapidFire_100SessionsNoLeak (A8)
//     * "login_discovery_no_target" → TestT2V2Port_Discovery_NoMatchingTarget_ReturnsEmpty (A9)
//   - weed/storage/blockvol/iscsi/scsi_test.go adversarial:
//     * unknown SCSI opcode variant → TestT2V2Port_SCSI_UnknownOpcode_ReturnsCheckCondition (A10)
//
// V3 rewrite rationale: These are session-level adversarial tests that
// require a running target + Discovery / Normal login. V3 enforces:
//   - session.go:202 SCSI cmd in Discovery session → connection close
//   - scsi.go:164-165 unknown SCSI opcode → CHECK CONDITION + ASCInvalidOpcode
//   - HandleTextRequest: empty target list → empty Text-Response (no error)
//   - Rapid open/close lifecycle via in-process target listener
//
// Maps to ledger rows:
//   - PCDD-ISCSI-DISCOVERY-MISUSE-001 (Discovery session contract)
//   - PCDD-ISCSI-SCSI-UNKNOWN-OPCODE-001 (SCSI opcode validation)
//   - PCDD-ISCSI-LIFECYCLE-RAPID-001 (lifecycle stress)
//
// Test layer: Component (L1, in-process target + real PDU wire)

package iscsi_test

import (
	"runtime"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// startSessionEdgeTarget brings up a target backed by a recording
// backend at a fresh healthy lineage. Shared by the edge tests.
func startSessionEdgeTarget(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3,
	})
	prov := testback.NewStaticProvider(rec)
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:   "127.0.0.1:0",
		IQN:      "iqn.2026-04.example.v3:v1",
		VolumeID: "v1",
		Provider: prov,
	})
	a, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	return a, func() { tg.Close() }
}

// A7. V2 qa_test.go:"discovery_then_scsi"
//
// V3 fail-closed shape: session.go:202 refuses SCSI-Cmd when
// SessionType == Discovery and closes the connection.
func TestT2V2Port_DiscoverySession_SCSICmd_ClosesConnection(t *testing.T) {
	addr, cleanup := startSessionEdgeTarget(t)
	defer cleanup()

	// Login as Discovery session (no TargetName per RFC 7143 §12).
	cli := dialAndLoginOpts(t, addr, loginOptions{
		SessionType: iscsi.SessionTypeDiscovery,
	})
	defer func() {
		// Server will close on SCSI-Cmd; swallow logout errors.
		defer func() { _ = recover() }()
		cli.logout(t)
	}()

	// Send a SCSI-Cmd PDU. Discovery sessions must reject it.
	cmd := &iscsi.PDU{}
	cmd.SetOpcode(iscsi.OpSCSICmd)
	cmd.SetOpSpecific1(iscsi.FlagF | iscsi.FlagW)
	cmd.SetInitiatorTaskTag(0x4141)
	cmd.SetCmdSN(1)
	var cdb [16]byte
	cdb[0] = 0x2A // WRITE(10)
	cmd.SetCDB(cdb)

	if err := iscsi.WritePDU(cli.conn, cmd); err != nil {
		return // server dropped us mid-write — fail-closed
	}

	expectServerClosedConnection(t, cli.conn, "SCSI-Cmd in Discovery session")
}

// A8. V2 qa_test.go:"rapid_fire_100"
//
// V2 intent: opening and closing many sessions in rapid succession
// should not leak goroutines or listener state; target stays
// responsive throughout.
//
// V3 applicability: V3 target keeps its listener open; each accept
// spawns a session goroutine. Clean logout must reclaim both
// client-side and server-side resources.
func TestT2V2Port_RapidFire_100SessionsNoLeak(t *testing.T) {
	addr, cleanup := startSessionEdgeTarget(t)
	defer cleanup()

	// Baseline goroutine count after target ready.
	gBefore := runtime.NumGoroutine()

	const N = 100
	for i := range N {
		cli := dialAndLogin(t, addr)
		cli.logout(t)
		_ = i
	}

	// Give the target a moment to reap session goroutines.
	// Wait for goroutine count to settle within a bounded window.
	const maxWait = 20
	settled := false
	for range maxWait {
		runtime.GC()
		gAfter := runtime.NumGoroutine()
		// Allow up to a handful of extra goroutines (scheduler noise,
		// accept loop variants). The defining check is "bounded", not
		// "identical".
		if gAfter-gBefore < 20 {
			settled = true
			break
		}
	}
	if !settled {
		gAfter := runtime.NumGoroutine()
		t.Fatalf("goroutine count did not settle after %d sessions: before=%d after=%d (leak?)",
			N, gBefore, gAfter)
	}

	// Final sanity: target still accepts a fresh login.
	cli := dialAndLogin(t, addr)
	defer cli.logout(t)
}

// A9. V2 qa_test.go:"login_discovery_no_target"
//
// V2 intent: Discovery session with SendTargets=<name> filter that
// matches no target should return a clean empty Text-Response
// (not an error, not a connection close).
func TestT2V2Port_Discovery_NoMatchingTarget_ReturnsEmpty(t *testing.T) {
	addr, cleanup := startSessionEdgeTarget(t)
	defer cleanup()

	cli := dialAndLoginOpts(t, addr, loginOptions{
		SessionType: iscsi.SessionTypeDiscovery,
	})
	defer cli.logout(t)

	// Send Text request with SendTargets filter that matches no
	// target (target IQN is "iqn.2026-04.example.v3:v1" from the
	// harness; filter by an unrelated name).
	sendTargets := iscsi.NewParams()
	sendTargets.Set("SendTargets", "iqn.does-not-exist:nomatch")

	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpTextReq)
	req.SetOpSpecific1(iscsi.FlagF) // Final
	req.SetInitiatorTaskTag(0x5151)
	req.SetCmdSN(1)
	req.DataSegment = sendTargets.Encode()

	if err := iscsi.WritePDU(cli.conn, req); err != nil {
		t.Fatalf("WritePDU: %v", err)
	}

	resp, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("ReadPDU: %v", err)
	}
	if resp.Opcode() != iscsi.OpTextResp {
		t.Fatalf("opcode=%s want TextResp", iscsi.OpcodeName(resp.Opcode()))
	}

	// Response data segment should contain no TargetName entries.
	// An empty response is the RFC-correct answer: "no matching
	// targets". We accept either truly empty or a response with
	// no TargetName=... entries.
	if len(resp.DataSegment) > 0 {
		parsed, perr := iscsi.ParseParams(resp.DataSegment)
		if perr != nil {
			t.Fatalf("response params parse: %v", perr)
		}
		if _, has := parsed.Get("TargetName"); has {
			t.Fatalf("expected no TargetName in response; got one")
		}
	}
}

// A10. V2 scsi_test.go variant: unknown SCSI opcode
//
// V3 fail-closed shape: scsi.go:164 default case returns
// illegalRequest(ASCInvalidOpcode, ...) → SCSI CHECK CONDITION.
// Connection stays alive; initiator sees protocol-level failure.
func TestT2V2Port_SCSI_UnknownOpcode_ReturnsCheckCondition(t *testing.T) {
	addr, cleanup := startSessionEdgeTarget(t)
	defer cleanup()

	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	// CDB with opcode 0xE0 — not one of the opcodes V3 dispatches
	// (TUR, INQUIRY, READ_CAPACITY, READ_10, WRITE_10, etc.).
	var cdb [16]byte
	cdb[0] = 0xE0

	status, _ := cli.scsiCmd(t, cdb, nil, 0)
	expectCheckCondition(t, status, "unknown SCSI opcode 0xE0")
}
