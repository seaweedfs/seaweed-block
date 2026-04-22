// Ownership: QA (from v3-phase-15-t2-v2-test-audit.md B-tier rewrite).
// sw may NOT modify this file without architect approval via §8B.4
// Discovery Bridge. See: sw-block/design/v3-phase-15-t2-v2-test-audit.md §3.3.
//
// V2 intent source:
//   - weed/storage/blockvol/iscsi/qa_test.go sub-tests:
//     * "scsi_before_ffp"            → TestT2V2Port_SCSIBeforeFFP_Rejected (B9)
//     * "double_login_after_ffp"     → TestT2V2Port_DoubleLoginAfterFFP_Rejected (B10)
//     * "dataout_wrong_ttt"/"_offset"/"_exceed_burst"
//                                    → TestT2V2Port_UnexpectedDataOut_Rejected (B14-amalg)
//
// V3 rewrite rationale:
//   - V2 qa_test.go sub-tests use V2 TargetConfig + V2 session internals
//     (pending queue, specific SCSI status echoes). V3 architecture rejects
//     unsupported opcodes in FFP with a protocol error that closes the
//     connection (session.go line 171). That is a stricter fail-closed
//     shape than V2's reject-response; equally safe for the stale/phase-
//     violation invariants.
//   - Dataout (B14/B15/B16 in the audit) collapse to one V3-applicable
//     intent at this checkpoint because V3 session does not implement
//     R2T / multi-PDU Data-Out yet (session.go comment lines 11-14). The
//     three V2 dataout sub-tests all share the invariant "unsolicited
//     Data-Out must not be accepted". V3 enforces that at line 159 with
//     a protocol error. Audit row: "B14-amalg", scoped deferrals B1/B2
//     (R2T-specific bug regressions) remain D-ckpt10.
//
// Maps to ledger rows:
//   - PCDD-ISCSI-FFP-VIOLATION-001 (phase violation)
//   - PCDD-ISCSI-LOGIN-REPLAY-001 (login after FFP)
//   - PCDD-ISCSI-UNSOLICITED-DATAOUT-001 (Data-Out without R2T)
//
// Test layer: Component (L1, in-process target + raw TCP PDU)

package iscsi_test

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// startV2PortTarget brings up an in-process iSCSI target bound to a
// recording backend on a fresh healthy lineage. Returns the listen
// address and a cleanup func.
func startV2PortTarget(t *testing.T) (addr string, cleanup func()) {
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

// expectServerClosedConnection reads from conn with a bounded
// deadline and asserts that the server closed the connection. This
// is the V3 session's fail-closed signal for phase violations and
// unsupported opcodes in FFP (session.go returns an error from the
// serve loop, which closes the connection).
func expectServerClosedConnection(t *testing.T, conn net.Conn, why string) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	if err == io.EOF {
		return // server closed cleanly
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("%s: expected server to close connection, but read timed out (server may have ACKed the bad PDU)", why)
	}
	if err != nil {
		// Any non-nil error that isn't Timeout is acceptable (connection
		// reset, etc.) — the server dropped us, that's fail-closed.
		return
	}
	t.Fatalf("%s: expected server close, got %d bytes: %x", why, n, buf[:n])
}

// B9: SCSI command before FullFeature phase must be rejected.
//
// V3 fail-closed shape: session.go enters the login loop first; a
// SCSI-Cmd PDU in login state is processed as "unexpected Login-Req"
// or similar; the serve loop errors out and closes the connection.
func TestT2V2Port_SCSIBeforeFFP_Rejected(t *testing.T) {
	addr, cleanup := startV2PortTarget(t)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Do NOT send Login-Req. Immediately send a SCSI-Cmd PDU
	// (WRITE(10), LBA 0, 1 block). The session state is still
	// SessionLogin; processing a SCSI-Cmd in that state is a
	// phase violation.
	cmd := &iscsi.PDU{}
	cmd.SetOpcode(iscsi.OpSCSICmd)
	cmd.SetOpSpecific1(iscsi.FlagF | iscsi.FlagW)
	cmd.SetInitiatorTaskTag(0x1111)
	cmd.SetCmdSN(0)

	var cdb [16]byte
	cdb[0] = 0x2A // SCSI WRITE(10) opcode
	binary.BigEndian.PutUint32(cdb[2:6], 0) // LBA 0
	binary.BigEndian.PutUint16(cdb[7:9], 1) // 1 block
	cmd.SetCDB(cdb)

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := iscsi.WritePDU(conn, cmd); err != nil {
		// Server may have dropped us mid-write; that is also acceptable
		// fail-closed shape.
		return
	}

	expectServerClosedConnection(t, conn, "SCSI-Cmd before FullFeature")
}

// B10: Login PDU after reaching FullFeature must be rejected.
//
// V3 fail-closed shape: session.go:171 returns
// "unsupported opcode in FFP" when a Login-Req opcode arrives
// post-FFP; the serve loop exits and the connection closes.
func TestT2V2Port_DoubleLoginAfterFFP_Rejected(t *testing.T) {
	addr, cleanup := startV2PortTarget(t)
	defer cleanup()

	// Reach FullFeature via the shared test client.
	cli := dialAndLogin(t, addr)
	defer func() {
		// The server will close the connection from under us; logout
		// after a post-FFP Login-Req is expected to fail. Swallow.
		defer func() { _ = recover() }()
		cli.logout(t)
	}()

	// Send a second Login-Req after FFP. This is a protocol violation
	// (RFC 3720 §5.1 — Login only happens in Login phase).
	badLogin := &iscsi.PDU{}
	badLogin.SetOpcode(iscsi.OpLoginReq)
	badLogin.SetOpSpecific1(iscsi.FlagT) // Transit
	badLogin.SetInitiatorTaskTag(0x2222)
	badLogin.SetCmdSN(1)

	_ = cli.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := iscsi.WritePDU(cli.conn, badLogin); err != nil {
		return // server dropped us mid-write — fail-closed
	}

	expectServerClosedConnection(t, cli.conn, "Login-Req after FullFeature")
}

// B14-amalg: Unsolicited SCSI Data-Out (no preceding R2T) must be
// rejected. Covers the V3-applicable intent of V2 qa_test.go
// dataout_wrong_ttt / dataout_wrong_offset / dataout_exceed_burst.
//
// V3 does not yet implement R2T / multi-PDU Data-Out, so ANY
// Data-Out in FFP is unsolicited (session.go:159 returns
// "unexpected Data-Out without R2T").
func TestT2V2Port_UnexpectedDataOut_Rejected(t *testing.T) {
	addr, cleanup := startV2PortTarget(t)
	defer cleanup()

	cli := dialAndLogin(t, addr)
	defer func() {
		defer func() { _ = recover() }()
		cli.logout(t)
	}()

	// Send a Data-Out PDU with no preceding R2T.
	dout := &iscsi.PDU{}
	dout.SetOpcode(iscsi.OpSCSIDataOut)
	dout.SetOpSpecific1(iscsi.FlagF) // Final
	dout.SetInitiatorTaskTag(0x3333)

	_ = cli.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := iscsi.WritePDU(cli.conn, dout); err != nil {
		return // server dropped us — fail-closed
	}

	expectServerClosedConnection(t, cli.conn, "Unsolicited Data-Out in FFP")
}
