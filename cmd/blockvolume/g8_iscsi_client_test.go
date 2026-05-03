package main_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

type g8IscsiClient struct {
	conn   net.Conn
	cmdSN  uint32
	itt    uint32
	statSN uint32
}

func dialG8Iscsi(t *testing.T, addr, targetName string) *g8IscsiClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial iSCSI %s: %v", addr, err)
	}
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	c := &g8IscsiClient{conn: conn, itt: 1}

	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpLoginReq)
	req.SetImmediate(true)
	req.SetLoginStages(iscsi.StageLoginOp, iscsi.StageFullFeature)
	req.SetLoginTransit(true)
	req.SetISID([6]byte{0x02, 0x47, 0x08, 0x00, 0x00, 0x01})
	req.SetInitiatorTaskTag(c.itt)
	c.itt++
	req.SetCmdSN(c.cmdSN)

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.2026-05.example.host:g8")
	params.Set("SessionType", iscsi.SessionTypeNormal)
	params.Set("TargetName", targetName)
	params.Set("HeaderDigest", "None")
	params.Set("DataDigest", "None")
	params.Set("MaxRecvDataSegmentLength", "262144")
	params.Set("InitialR2T", "Yes")
	params.Set("ImmediateData", "Yes")
	req.DataSegment = params.Encode()

	if err := iscsi.WritePDU(conn, req); err != nil {
		t.Fatalf("write iSCSI login: %v", err)
	}
	resp, err := iscsi.ReadPDU(conn)
	if err != nil {
		t.Fatalf("read iSCSI login: %v", err)
	}
	if resp.Opcode() != iscsi.OpLoginResp || resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("iSCSI login failed: opcode=%s class=0x%02x detail=0x%02x",
			iscsi.OpcodeName(resp.Opcode()), resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
	c.cmdSN++
	c.statSN = resp.StatSN()
	return c
}

func (c *g8IscsiClient) close(t *testing.T) {
	t.Helper()
	if c == nil || c.conn == nil {
		return
	}
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpLogoutReq)
	req.SetOpSpecific1(iscsi.FlagF)
	req.SetInitiatorTaskTag(c.itt)
	c.itt++
	req.SetCmdSN(c.cmdSN)
	c.cmdSN++
	_ = iscsi.WritePDU(c.conn, req)
	_, _ = iscsi.ReadPDU(c.conn)
	_ = c.conn.Close()
}

func (c *g8IscsiClient) write10(t *testing.T, lba uint32, payload []byte) {
	t.Helper()
	status := c.write10Status(t, lba, payload)
	if status != iscsi.StatusGood {
		t.Fatalf("iSCSI WRITE(10) status=0x%02x", status)
	}
}

func (c *g8IscsiClient) write10Status(t *testing.T, lba uint32, payload []byte) uint8 {
	t.Helper()
	status, _ := c.scsi(t, g8WriteCDB10(lba, 1), payload, 0)
	return status
}

func (c *g8IscsiClient) read10(t *testing.T, lba uint32, blocks uint16, bytesPerBlock int) []byte {
	t.Helper()
	status, data := c.scsi(t, g8ReadCDB10(lba, blocks), nil, int(blocks)*bytesPerBlock)
	if status != iscsi.StatusGood {
		t.Fatalf("iSCSI READ(10) status=0x%02x", status)
	}
	return data
}

func (c *g8IscsiClient) scsi(t *testing.T, cdb [16]byte, writePayload []byte, expectedDataIn int) (uint8, []byte) {
	t.Helper()
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpSCSICmd)
	req.SetOpSpecific1(iscsi.FlagF)
	if len(writePayload) > 0 {
		req.BHS[1] |= iscsi.FlagW
	}
	if expectedDataIn > 0 {
		req.BHS[1] |= iscsi.FlagR
	}
	req.SetLUN(0)
	itt := c.itt
	req.SetInitiatorTaskTag(itt)
	c.itt++
	req.SetExpectedDataTransferLength(uint32(len(writePayload) + expectedDataIn))
	req.SetCmdSN(c.cmdSN)
	c.cmdSN++
	req.SetExpStatSN(c.statSN + 1)
	req.SetCDB(cdb)
	req.DataSegment = writePayload
	if err := iscsi.WritePDU(c.conn, req); err != nil {
		t.Fatalf("write iSCSI SCSI-Cmd: %v", err)
	}

	var collected bytes.Buffer
	for {
		resp, err := iscsi.ReadPDU(c.conn)
		if err != nil {
			t.Fatalf("read iSCSI response: %v", err)
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
			t.Fatalf("unexpected iSCSI opcode: %s", iscsi.OpcodeName(resp.Opcode()))
		}
	}
}

func g8WriteCDB10(lba uint32, blocks uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], blocks)
	return cdb
}

func g8ReadCDB10(lba uint32, blocks uint16) [16]byte {
	var cdb [16]byte
	cdb[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], blocks)
	return cdb
}
