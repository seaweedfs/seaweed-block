package nvme_test

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

func newStatsTarget(t *testing.T) (*nvme.Target, *nvmeClient) {
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
		t.Fatalf("Target.Start: %v", err)
	}
	t.Cleanup(func() {
		_ = tg.Close()
	})
	cli := dialAndConnect(t, addr)
	t.Cleanup(func() {
		cli.close()
	})
	return tg, cli
}

func TestTargetStats_RecordInlineAndR2TWrites(t *testing.T) {
	tg, cli := newStatsTarget(t)

	inlinePayload := make([]byte, nvme.DefaultBlockSize)
	status := writeInlineOnClient(t, cli, 0, 1, inlinePayload)
	expectStatusSuccess(t, status, "inline Write")

	r2tPayload := make([]byte, 2*nvme.DefaultBlockSize)
	status = writeChunkedOnClient(t, cli, 1, 2, r2tPayload, 2)
	expectStatusSuccess(t, status, "R2T Write")

	st := tg.Stats()
	if st.AdminConnects != 1 || st.IOConnects != 1 {
		t.Fatalf("connect stats admin=%d io=%d want 1/1", st.AdminConnects, st.IOConnects)
	}
	if st.WriteCommands != 2 {
		t.Fatalf("write_commands=%d want 2", st.WriteCommands)
	}
	if st.InlineWriteCommands != 1 || st.InlineWriteBytes != uint64(len(inlinePayload)) {
		t.Fatalf("inline stats commands=%d bytes=%d want 1/%d",
			st.InlineWriteCommands, st.InlineWriteBytes, len(inlinePayload))
	}
	if st.R2TWriteCommands != 1 || st.R2TWriteBytes != uint64(len(r2tPayload)) {
		t.Fatalf("r2t stats commands=%d bytes=%d want 1/%d",
			st.R2TWriteCommands, st.R2TWriteBytes, len(r2tPayload))
	}
	if st.H2CDataPDUs != 2 || st.H2CDataBytes != uint64(len(r2tPayload)) {
		t.Fatalf("h2c stats pdus=%d bytes=%d want 2/%d",
			st.H2CDataPDUs, st.H2CDataBytes, len(r2tPayload))
	}
}

func TestTargetStats_RecordReadAndFlush(t *testing.T) {
	tg, cli := newStatsTarget(t)

	status := cli.writeCmd(t, 0, 1, make([]byte, nvme.DefaultBlockSize))
	expectStatusSuccess(t, status, "seed Write")
	status, data := cli.readCmd(t, 0, 1, int(nvme.DefaultBlockSize))
	expectStatusSuccess(t, status, "Read")
	if len(data) != int(nvme.DefaultBlockSize) {
		t.Fatalf("read len=%d want %d", len(data), nvme.DefaultBlockSize)
	}
	status = flushOnClient(t, cli)
	expectStatusSuccess(t, status, "Flush")

	st := tg.Stats()
	if st.ReadCommands != 1 {
		t.Fatalf("read_commands=%d want 1", st.ReadCommands)
	}
	if st.FlushCommands != 1 {
		t.Fatalf("flush_commands=%d want 1", st.FlushCommands)
	}
	if st.C2HDataPDUs != 1 || st.C2HDataBytes != uint64(nvme.DefaultBlockSize) {
		t.Fatalf("c2h stats pdus=%d bytes=%d want 1/%d",
			st.C2HDataPDUs, st.C2HDataBytes, nvme.DefaultBlockSize)
	}
}

func writeInlineOnClient(t *testing.T, c *nvmeClient, slba uint64, nlb uint16, payload []byte) uint16 {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x01,
		CID:    cid,
		NSID:   1,
		D10:    uint32(slba & 0xFFFFFFFF),
		D11:    uint32(slba >> 32),
		D12:    uint32(nlb - 1),
	}
	if err := c.ioW.SendWithData(0x4, 0, &cmd, 64, payload); err != nil {
		t.Fatalf("send inline Write: %v", err)
	}
	resp := recvCapsuleResp(t, c.ioR)
	if resp.CID != cid {
		t.Fatalf("inline Write resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status
}

func flushOnClient(t *testing.T, c *nvmeClient) uint16 {
	t.Helper()
	cid := uint16(c.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x00,
		CID:    cid,
		NSID:   1,
	}
	if err := c.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send Flush: %v", err)
	}
	resp := recvCapsuleResp(t, c.ioR)
	if resp.CID != cid {
		t.Fatalf("Flush resp CID=%d want %d", resp.CID, cid)
	}
	return resp.Status
}

func TestTargetStats_ZeroForNilTarget(t *testing.T) {
	var tg *nvme.Target
	st := tg.Stats()
	if st != (nvme.Stats{}) {
		t.Fatalf("nil target stats = %+v, want zero", st)
	}
}
