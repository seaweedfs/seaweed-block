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

// scsiCmd builds a SCSI-Cmd PDU wrapping the given CDB. If
// `immediate` is non-empty it rides in the SCSI-Cmd's data
// segment (ImmediateData path). If the write's EDTL exceeds
// len(immediate), the target emits R2T and the client feeds
// the remainder from `solicited` via Data-Out PDUs.
//
// For simple small writes, pass the full payload as
// `immediate` and leave `solicited` nil — matches ckpt 4 flow.
//
// `expectedDataIn` is the read-side transfer length (0 for
// metadata/writes). Returns the SCSI-Response status + any
// collected Data-In payload.
func (c *testClient) scsiCmd(t *testing.T, cdb [16]byte, immediate []byte, expectedDataIn int) (status uint8, dataIn []byte) {
	t.Helper()
	return c.scsiCmdFull(t, cdb, immediate, nil, expectedDataIn)
}

// scsiCmdFull is the extended form: EDTL = len(immediate) +
// len(solicited). Used by the ckpt 10 large-write tests to
// drive the R2T → Data-Out path.
func (c *testClient) scsiCmdFull(t *testing.T, cdb [16]byte, immediate, solicited []byte, expectedDataIn int) (status uint8, dataIn []byte) {
	t.Helper()
	totalWrite := len(immediate) + len(solicited)

	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpSCSICmd)
	req.SetOpSpecific1(iscsi.FlagF)
	if expectedDataIn > 0 {
		req.BHS[1] |= iscsi.FlagR
	}
	if totalWrite > 0 {
		req.BHS[1] |= iscsi.FlagW
	}
	req.SetLUN(0)
	itt := c.itt
	req.SetInitiatorTaskTag(itt)
	c.itt++
	req.SetExpectedDataTransferLength(uint32(totalWrite + expectedDataIn))
	req.SetCmdSN(c.cmdSN)
	c.cmdSN++
	req.SetExpStatSN(c.statSN + 1)
	req.SetCDB(cdb)
	if len(immediate) > 0 {
		req.DataSegment = immediate
	}
	if err := iscsi.WritePDU(c.conn, req); err != nil {
		t.Fatalf("write scsi-cmd: %v", err)
	}

	// Read loop. For writes with solicited > 0 the target sends
	// R2T(s) first; we respond with Data-Out, then the target
	// emits SCSI-Response. For reads, Data-In(s) arrive until
	// the one with S-bit.
	var collected bytes.Buffer
	solicitedOffset := len(immediate) // where in the logical write stream Data-Out starts
	var dataSN uint32
	for {
		resp, err := iscsi.ReadPDU(c.conn)
		if err != nil {
			t.Fatalf("read scsi resp: %v", err)
		}
		switch resp.Opcode() {
		case iscsi.OpR2T:
			if len(solicited) == 0 {
				t.Fatalf("unexpected R2T (no solicited data)")
			}
			offset := resp.BufferOffset()
			desired := resp.DesiredDataLength()
			if int(offset)-solicitedOffset < 0 || int(offset)+int(desired)-solicitedOffset > len(solicited) {
				t.Fatalf("R2T range (offset=%d desired=%d) outside solicited payload [%d, %d)",
					offset, desired, solicitedOffset, solicitedOffset+len(solicited))
			}
			start := int(offset) - solicitedOffset
			chunk := solicited[start : start+int(desired)]

			// Send a single Data-Out PDU covering the R2T window.
			out := &iscsi.PDU{}
			out.SetOpcode(iscsi.OpSCSIDataOut)
			out.SetOpSpecific1(iscsi.FlagF) // final in this burst
			out.SetLUN(0)
			out.SetInitiatorTaskTag(itt)
			out.SetTargetTransferTag(resp.TargetTransferTag())
			out.SetDataSN(dataSN)
			dataSN++
			out.SetBufferOffset(offset)
			out.SetExpStatSN(c.statSN + 1)
			out.DataSegment = chunk
			if err := iscsi.WritePDU(c.conn, out); err != nil {
				t.Fatalf("write Data-Out: %v", err)
			}
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
