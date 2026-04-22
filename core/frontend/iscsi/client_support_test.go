// Ownership: sw test-support (per test-spec §9). A minimal Go
// iSCSI initiator used by the L1 in-process route test. NOT a
// general-purpose initiator — covers only the PDUs T2.L1 needs:
// login → SCSI-Cmd(WRITE/READ/TUR/INQUIRY/READ_CAPACITY) →
// logout. The OS-initiator compatibility surface (MaxBurst, R2T,
// DataPDUInOrder, etc.) is L2-OS territory.
package iscsi_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

// testClient drives one iSCSI connection against a target.
type testClient struct {
	conn   net.Conn
	cmdSN  uint32
	itt    uint32
	statSN uint32
	tsih   uint16
}

// loginOptions tunes the test client's login behavior. Defaults
// produce a single-round Normal-session login with the canonical
// test target name. Tests that need Discovery sessions or a
// custom TargetName override fields.
type loginOptions struct {
	SessionType   string // "" → "Normal"
	TargetName    string // "" → "iqn.2026-04.example.v3:v1"
	InitiatorName string // "" → "iqn.2026-04.example.host:test"
}

func dialAndLogin(t *testing.T, addr string) *testClient {
	t.Helper()
	return dialAndLoginOpts(t, addr, loginOptions{})
}

func dialAndLoginOpts(t *testing.T, addr string, opts loginOptions) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	c := &testClient{conn: conn, cmdSN: 0, itt: 1}

	if opts.SessionType == "" {
		opts.SessionType = iscsi.SessionTypeNormal
	}
	if opts.InitiatorName == "" {
		opts.InitiatorName = "iqn.2026-04.example.host:test"
	}
	if opts.TargetName == "" && opts.SessionType == iscsi.SessionTypeNormal {
		opts.TargetName = "iqn.2026-04.example.v3:v1"
	}

	// Single-round login: CSG=LoginOp, NSG=FullFeature, Transit=1.
	// Carries InitiatorName/SessionType/TargetName + minimal
	// operational params. The negotiator handles the direct
	// LoginOp jump (V2-compatible behavior).
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpLoginReq)
	req.SetImmediate(true)
	req.SetLoginStages(iscsi.StageLoginOp, iscsi.StageFullFeature)
	req.SetLoginTransit(true)
	req.SetISID([6]byte{0x02, 0x3d, 0x00, 0x00, 0x00, 0x01})
	req.SetInitiatorTaskTag(c.itt)
	c.itt++
	req.SetCmdSN(c.cmdSN)

	params := iscsi.NewParams()
	params.Set("InitiatorName", opts.InitiatorName)
	params.Set("SessionType", opts.SessionType)
	if opts.TargetName != "" {
		params.Set("TargetName", opts.TargetName)
	}
	params.Set("HeaderDigest", "None")
	params.Set("DataDigest", "None")
	params.Set("MaxRecvDataSegmentLength", "262144")
	params.Set("InitialR2T", "Yes")
	params.Set("ImmediateData", "Yes")
	req.DataSegment = params.Encode()

	if err := iscsi.WritePDU(conn, req); err != nil {
		t.Fatalf("write login: %v", err)
	}
	resp, err := iscsi.ReadPDU(conn)
	if err != nil {
		t.Fatalf("read login resp: %v", err)
	}
	if resp.Opcode() != iscsi.OpLoginResp {
		t.Fatalf("login resp opcode=%s want LoginResp", iscsi.OpcodeName(resp.Opcode()))
	}
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("login status class=0x%02x want Success", resp.LoginStatusClass())
	}
	if !resp.LoginTransit() {
		t.Fatalf("login resp did not grant transit")
	}
	c.tsih = resp.TSIH()
	c.cmdSN++
	c.statSN = resp.StatSN()
	return c
}

// scsiCmd builds a SCSI-Cmd PDU wrapping the given CDB, with
// optional immediate data (for WRITE in T2 minimal scope).
// Returns the SCSI-Response status + any Data-In payload.
func (c *testClient) scsiCmd(t *testing.T, cdb [16]byte, dataOut []byte, expectedDataIn int) (status uint8, dataIn []byte) {
	t.Helper()
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpSCSICmd)
	req.SetOpSpecific1(iscsi.FlagF) // final
	if expectedDataIn > 0 {
		req.BHS[1] |= iscsi.FlagR // read
	}
	if len(dataOut) > 0 {
		req.BHS[1] |= iscsi.FlagW // write
	}
	req.SetLUN(0) // LUN 0
	req.SetInitiatorTaskTag(c.itt)
	c.itt++
	req.SetExpectedDataTransferLength(uint32(len(dataOut) + expectedDataIn))
	req.SetCmdSN(c.cmdSN)
	c.cmdSN++
	req.SetExpStatSN(c.statSN + 1)
	req.SetCDB(cdb)
	if len(dataOut) > 0 {
		req.DataSegment = dataOut
	}
	if err := iscsi.WritePDU(c.conn, req); err != nil {
		t.Fatalf("write scsi-cmd: %v", err)
	}

	// Read responses until we see something with status. In the
	// minimal target:
	//   - commands returning data: ONE Data-In with S-bit set
	//   - commands without data: ONE SCSI-Response
	var collected bytes.Buffer
	for {
		resp, err := iscsi.ReadPDU(c.conn)
		if err != nil {
			t.Fatalf("read scsi resp: %v", err)
		}
		switch resp.Opcode() {
		case iscsi.OpSCSIDataIn:
			collected.Write(resp.DataSegment)
			if resp.OpSpecific1()&iscsi.FlagS != 0 {
				return resp.SCSIStatusByte(), collected.Bytes()
			}
		case iscsi.OpSCSIResp:
			return resp.SCSIStatusByte(), collected.Bytes()
		default:
			t.Fatalf("unexpected resp opcode: %s", iscsi.OpcodeName(resp.Opcode()))
		}
	}
}

// writeCDB10 builds a WRITE(10) CDB.
func writeCDB10(lba uint32, blocks uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], blocks)
	return cdb
}

// readCDB10 builds a READ(10) CDB.
func readCDB10(lba uint32, blocks uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], blocks)
	return cdb
}

// logout cleanly closes the session.
func (c *testClient) logout(t *testing.T) {
	t.Helper()
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpLogoutReq)
	req.SetOpSpecific1(iscsi.FlagF)
	req.SetInitiatorTaskTag(c.itt)
	c.itt++
	req.SetCmdSN(c.cmdSN)
	c.cmdSN++
	_ = iscsi.WritePDU(c.conn, req)
	// Best-effort read of response; ignore errors on shutdown.
	_, _ = iscsi.ReadPDU(c.conn)
	_ = c.conn.Close()
}

// expectGood fails the test if status is not StatusGood.
func expectGood(t *testing.T, status uint8, op string) {
	t.Helper()
	if status != iscsi.StatusGood {
		t.Fatalf("%s: status=0x%02x want Good (0x00)", op, status)
	}
}

// expectCheckCondition fails the test if status is Good.
func expectCheckCondition(t *testing.T, status uint8, op string) {
	t.Helper()
	if status != iscsi.StatusCheckCondition {
		t.Fatalf("%s: status=0x%02x want CheckCondition (0x02)", op, status)
	}
}

// silence unused-import noise if only some helpers are referenced
var _ = fmt.Sprintf
