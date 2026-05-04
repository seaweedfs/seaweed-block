package iscsi_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

func startP2Target(t *testing.T) (*iscsi.Target, string, *testback.RecordingBackend) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:   "127.0.0.1:0",
		IQN:      "iqn.2026-04.example.v3:v1",
		VolumeID: "v1",
		Provider: testback.NewStaticProvider(rec),
		Logger:   log.New(io.Discard, "", 0),
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	return tg, addr, rec
}

func TestP2_ISCSI_ConcurrentSessions50_WriteRead(t *testing.T) {
	tg, addr, _ := startP2Target(t)
	defer tg.Close()

	const sessions = 50
	errs := make(chan error, sessions)
	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cli := dialAndLogin(t, addr)
			defer cli.logout(t)

			payload := bytes.Repeat([]byte{byte(id)}, int(iscsi.DefaultBlockSize))
			status, _ := cli.scsiCmd(t, writeCDB10(uint32(id), 1), payload, 0)
			if status != iscsi.StatusGood {
				errs <- fmt.Errorf("session %d write status=0x%02x", id, status)
				return
			}
			status, got := cli.scsiCmd(t, readCDB10(uint32(id), 1), nil, int(iscsi.DefaultBlockSize))
			if status != iscsi.StatusGood {
				errs <- fmt.Errorf("session %d read status=0x%02x", id, status)
				return
			}
			if !bytes.Equal(got, payload) {
				errs <- fmt.Errorf("session %d read payload mismatch", id)
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestP2_ISCSI_TargetCloseWithActiveSessions_ExitsCleanly(t *testing.T) {
	tg, addr, _ := startP2Target(t)

	const sessions = 8
	clients := make([]*testClient, 0, sessions)
	for i := 0; i < sessions; i++ {
		clients = append(clients, dialAndLogin(t, addr))
	}

	closed := make(chan error, 1)
	go func() { closed <- tg.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("target Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("target Close timed out with active sessions")
	}

	for i, cli := range clients {
		_ = cli.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if _, err := iscsi.ReadPDU(cli.conn); err == nil {
			t.Fatalf("client %d: expected read error after target Close", i)
		}
		_ = cli.conn.Close()
	}
}

func TestP2_ISCSI_ErrorResponseAdvancesStatSN(t *testing.T) {
	tg, addr, _ := startP2Target(t)
	defer tg.Close()

	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	nop := &iscsi.PDU{}
	nop.SetOpcode(iscsi.OpNOPOut)
	nop.SetOpSpecific1(iscsi.FlagF)
	nop.SetImmediate(true)
	nop.SetInitiatorTaskTag(cli.itt)
	cli.itt++
	nop.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	if err := iscsi.WritePDU(cli.conn, nop); err != nil {
		t.Fatalf("write initial NOP-Out: %v", err)
	}
	nopResp, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("read initial NOP-In: %v", err)
	}
	if nopResp.Opcode() != iscsi.OpNOPIn {
		t.Fatalf("initial response opcode=%s want NOP-In", iscsi.OpcodeName(nopResp.Opcode()))
	}
	baseSN := nopResp.StatSN()

	read := &iscsi.PDU{}
	read.SetOpcode(iscsi.OpSCSICmd)
	read.SetOpSpecific1(iscsi.FlagF | iscsi.FlagR)
	read.SetLUN(0)
	read.SetInitiatorTaskTag(cli.itt)
	cli.itt++
	read.SetExpectedDataTransferLength(uint32(iscsi.DefaultBlockSize))
	read.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	read.SetExpStatSN(baseSN + 1)
	var cdb [16]byte
	cdb[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 0xFFFFFFF0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	read.SetCDB(cdb)
	if err := iscsi.WritePDU(cli.conn, read); err != nil {
		t.Fatalf("write out-of-range READ: %v", err)
	}
	errResp, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("read error response: %v", err)
	}
	if errResp.Opcode() != iscsi.OpSCSIResp {
		t.Fatalf("error response opcode=%s want SCSIResp", iscsi.OpcodeName(errResp.Opcode()))
	}
	if errResp.SCSIStatusByte() != iscsi.StatusCheckCondition {
		t.Fatalf("error response status=0x%02x want CheckCondition", errResp.SCSIStatusByte())
	}
	if errResp.StatSN() != baseSN+1 {
		t.Fatalf("error StatSN=%d want %d", errResp.StatSN(), baseSN+1)
	}

	nop2 := &iscsi.PDU{}
	nop2.SetOpcode(iscsi.OpNOPOut)
	nop2.SetOpSpecific1(iscsi.FlagF)
	nop2.SetImmediate(true)
	nop2.SetInitiatorTaskTag(cli.itt)
	cli.itt++
	nop2.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	nop2.SetExpStatSN(baseSN + 2)
	if err := iscsi.WritePDU(cli.conn, nop2); err != nil {
		t.Fatalf("write second NOP-Out: %v", err)
	}
	nop2Resp, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("read second NOP-In: %v", err)
	}
	if nop2Resp.StatSN() != baseSN+2 {
		t.Fatalf("post-error NOP StatSN=%d want %d", nop2Resp.StatSN(), baseSN+2)
	}
}
