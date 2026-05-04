package iscsi_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// TestP1_ISCSI_R2TDataOut_AllowsPipelinedCommand pins the Linux
// kernel initiator shape observed during mkfs.ext4: while the target
// is collecting R2T-solicited Data-Out for one WRITE, another
// SCSI-Command can arrive on the same connection. That command must be
// queued and processed after the write completes; it must not tear down
// the session with "expected Data-Out, got SCSI-Command".
func TestP1_ISCSI_R2TDataOut_AllowsPipelinedCommand(t *testing.T) {
	const (
		blocks    = 256 // 128 KiB at 512-byte SCSI blocks.
		maxBurst  = 64 * 1024
		blockSize = int(iscsi.DefaultBlockSize)
	)
	totalBytes := blocks * blockSize

	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3,
	})
	prov := testback.NewStaticProvider(rec)
	neg := iscsi.DefaultNegotiableConfig()
	neg.MaxBurstLength = maxBurst
	neg.FirstBurstLength = 0
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:      "127.0.0.1:0",
		IQN:         "iqn.2026-04.example.v3:v1",
		VolumeID:    "v1",
		Provider:    prov,
		Negotiation: neg,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	payload := make([]byte, totalBytes)
	for i := range payload {
		payload[i] = byte((i * 29) % 251)
	}

	writeITT := cli.itt
	writeCmd := &iscsi.PDU{}
	writeCmd.SetOpcode(iscsi.OpSCSICmd)
	writeCmd.SetOpSpecific1(iscsi.FlagF | iscsi.FlagW)
	writeCmd.SetLUN(0)
	writeCmd.SetInitiatorTaskTag(writeITT)
	cli.itt++
	writeCmd.SetExpectedDataTransferLength(uint32(totalBytes))
	writeCmd.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	writeCmd.SetExpStatSN(cli.statSN + 1)
	writeCmd.SetCDB(writeCDB10(0, uint16(blocks)))
	if err := iscsi.WritePDU(cli.conn, writeCmd); err != nil {
		t.Fatalf("write SCSI WRITE(10): %v", err)
	}

	r2t, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("read first R2T: %v", err)
	}
	if r2t.Opcode() != iscsi.OpR2T {
		t.Fatalf("first response opcode=%s want R2T", iscsi.OpcodeName(r2t.Opcode()))
	}

	turITT := cli.itt
	tur := &iscsi.PDU{}
	tur.SetOpcode(iscsi.OpSCSICmd)
	tur.SetOpSpecific1(iscsi.FlagF)
	tur.SetLUN(0)
	tur.SetInitiatorTaskTag(turITT)
	cli.itt++
	tur.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	tur.SetExpStatSN(cli.statSN + 1)
	tur.SetCDB([16]byte{iscsi.ScsiTestUnitReady})
	if err := iscsi.WritePDU(cli.conn, tur); err != nil {
		t.Fatalf("write pipelined TEST_UNIT_READY: %v", err)
	}

	sendDataOut := func(r2t *iscsi.PDU) {
		t.Helper()
		offset := r2t.BufferOffset()
		desired := r2t.DesiredDataLength()
		out := &iscsi.PDU{}
		out.SetOpcode(iscsi.OpSCSIDataOut)
		out.SetOpSpecific1(iscsi.FlagF)
		out.SetLUN(0)
		out.SetInitiatorTaskTag(writeITT)
		out.SetTargetTransferTag(r2t.TargetTransferTag())
		out.SetDataSN(0)
		out.SetBufferOffset(offset)
		out.SetExpStatSN(cli.statSN + 1)
		out.DataSegment = payload[offset : offset+desired]
		if err := iscsi.WritePDU(cli.conn, out); err != nil {
			t.Fatalf("write Data-Out offset=%d desired=%d: %v", offset, desired, err)
		}
	}
	sendDataOut(r2t)

	var sawWriteResp, sawTURResp bool
	for !(sawWriteResp && sawTURResp) {
		resp, err := iscsi.ReadPDU(cli.conn)
		if err != nil {
			t.Fatalf("read response after pipelined command: %v", err)
		}
		switch resp.Opcode() {
		case iscsi.OpR2T:
			sendDataOut(resp)
		case iscsi.OpSCSIResp:
			if resp.SCSIStatusByte() != iscsi.StatusGood {
				t.Fatalf("SCSI response ITT=0x%08x status=0x%02x want Good",
					resp.InitiatorTaskTag(), resp.SCSIStatusByte())
			}
			switch resp.InitiatorTaskTag() {
			case writeITT:
				sawWriteResp = true
			case turITT:
				sawTURResp = true
			default:
				t.Fatalf("unexpected SCSI response ITT=0x%08x", resp.InitiatorTaskTag())
			}
		default:
			t.Fatalf("unexpected opcode after Data-Out: %s", iscsi.OpcodeName(resp.Opcode()))
		}
	}

	if rec.WriteCount() != 1 {
		t.Fatalf("backend WriteCount=%d want 1", rec.WriteCount())
	}
	if got := rec.WriteAt(0).Data; !bytes.Equal(got, payload) {
		t.Fatalf("backend write payload mismatch: got %d bytes want %d", len(got), len(payload))
	}
}

func TestP1_ISCSI_R2TDataOut_PendingQueueBounded(t *testing.T) {
	const (
		blocks    = 256
		maxBurst  = 64 * 1024
		blockSize = int(iscsi.DefaultBlockSize)
	)
	totalBytes := blocks * blockSize

	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	prov := testback.NewStaticProvider(rec)
	neg := iscsi.DefaultNegotiableConfig()
	neg.MaxBurstLength = maxBurst
	neg.FirstBurstLength = 0
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:      "127.0.0.1:0",
		IQN:         "iqn.2026-04.example.v3:v1",
		VolumeID:    "v1",
		Provider:    prov,
		Negotiation: neg,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	writeCmd := &iscsi.PDU{}
	writeCmd.SetOpcode(iscsi.OpSCSICmd)
	writeCmd.SetOpSpecific1(iscsi.FlagF | iscsi.FlagW)
	writeCmd.SetLUN(0)
	writeCmd.SetInitiatorTaskTag(cli.itt)
	cli.itt++
	writeCmd.SetExpectedDataTransferLength(uint32(totalBytes))
	writeCmd.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	writeCmd.SetExpStatSN(cli.statSN + 1)
	writeCmd.SetCDB(writeCDB10(0, uint16(blocks)))
	if err := iscsi.WritePDU(cli.conn, writeCmd); err != nil {
		t.Fatalf("write SCSI WRITE(10): %v", err)
	}

	r2t, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("read R2T: %v", err)
	}
	if r2t.Opcode() != iscsi.OpR2T {
		t.Fatalf("first response opcode=%s want R2T", iscsi.OpcodeName(r2t.Opcode()))
	}

	for i := 0; i <= 64; i++ {
		tur := &iscsi.PDU{}
		tur.SetOpcode(iscsi.OpSCSICmd)
		tur.SetOpSpecific1(iscsi.FlagF)
		tur.SetLUN(0)
		tur.SetInitiatorTaskTag(cli.itt)
		cli.itt++
		tur.SetCmdSN(cli.cmdSN)
		cli.cmdSN++
		tur.SetExpStatSN(cli.statSN + 1)
		tur.SetCDB([16]byte{iscsi.ScsiTestUnitReady})
		if err := iscsi.WritePDU(cli.conn, tur); err != nil {
			t.Fatalf("write pipelined command %d: %v", i, err)
		}
	}

	_ = cli.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = iscsi.ReadPDU(cli.conn)
	if err == nil {
		t.Fatal("expected connection close after pending queue overflow")
	}
	if !strings.Contains(err.Error(), "connection") &&
		!strings.Contains(err.Error(), "forcibly closed") &&
		!strings.Contains(err.Error(), "reset") &&
		!strings.Contains(err.Error(), "EOF") {
		t.Fatalf("unexpected read error after overflow: %v", err)
	}
	if rec.WriteCount() != 0 {
		t.Fatalf("backend WriteCount=%d want 0 after overflow", rec.WriteCount())
	}
}

func TestP1_ISCSI_R2TDataOut_TimesOutWhenInitiatorStalls(t *testing.T) {
	const (
		blocks    = 256
		maxBurst  = 64 * 1024
		blockSize = int(iscsi.DefaultBlockSize)
	)
	totalBytes := blocks * blockSize

	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	prov := testback.NewStaticProvider(rec)
	neg := iscsi.DefaultNegotiableConfig()
	neg.MaxBurstLength = maxBurst
	neg.FirstBurstLength = 0
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:         "127.0.0.1:0",
		IQN:            "iqn.2026-04.example.v3:v1",
		VolumeID:       "v1",
		Provider:       prov,
		Negotiation:    neg,
		DataOutTimeout: 100 * time.Millisecond,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	writeCmd := &iscsi.PDU{}
	writeCmd.SetOpcode(iscsi.OpSCSICmd)
	writeCmd.SetOpSpecific1(iscsi.FlagF | iscsi.FlagW)
	writeCmd.SetLUN(0)
	writeCmd.SetInitiatorTaskTag(cli.itt)
	cli.itt++
	writeCmd.SetExpectedDataTransferLength(uint32(totalBytes))
	writeCmd.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	writeCmd.SetExpStatSN(cli.statSN + 1)
	writeCmd.SetCDB(writeCDB10(0, uint16(blocks)))
	if err := iscsi.WritePDU(cli.conn, writeCmd); err != nil {
		t.Fatalf("write SCSI WRITE(10): %v", err)
	}

	r2t, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("read R2T: %v", err)
	}
	if r2t.Opcode() != iscsi.OpR2T {
		t.Fatalf("first response opcode=%s want R2T", iscsi.OpcodeName(r2t.Opcode()))
	}

	start := time.Now()
	_ = cli.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = iscsi.ReadPDU(cli.conn)
	if err == nil {
		t.Fatal("expected connection close after Data-Out timeout")
	}
	elapsed := time.Since(start)
	if elapsed < 50*time.Millisecond {
		t.Fatalf("session closed too quickly: %s", elapsed)
	}
	if elapsed > time.Second {
		t.Fatalf("Data-Out timeout took too long: %s", elapsed)
	}
	if rec.WriteCount() != 0 {
		t.Fatalf("backend WriteCount=%d want 0 after timeout", rec.WriteCount())
	}
}
