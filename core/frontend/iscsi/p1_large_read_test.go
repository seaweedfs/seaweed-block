package iscsi_test

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

func TestP1_ISCSI_LargeRead_SplitsDataInByMaxRecvSegment(t *testing.T) {
	const (
		blocks    = 600 // 300 KiB at 512-byte SCSI blocks.
		maxRecv   = 64 * 1024
		blockSize = int(iscsi.DefaultBlockSize)
	)
	totalBytes := blocks * blockSize

	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	payload := make([]byte, totalBytes)
	for i := range payload {
		payload[i] = byte((i * 31) % 251)
	}
	if _, err := rec.Write(t.Context(), 0, payload); err != nil {
		t.Fatalf("seed backend: %v", err)
	}

	prov := testback.NewStaticProvider(rec)
	neg := iscsi.DefaultNegotiableConfig()
	neg.MaxRecvDataSegmentLength = maxRecv
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

	status, got, _, dataInTraces := cli.scsiCmdFullWithTrace(t, readCDB10(0, uint16(blocks)), nil, nil, totalBytes)
	expectGood(t, status, "large READ(10)")
	if !bytes.Equal(got, payload) {
		t.Fatalf("large READ payload mismatch: got=%d want=%d", len(got), len(payload))
	}
	if len(dataInTraces) < 2 {
		t.Fatalf("Data-In PDU count=%d want multiple", len(dataInTraces))
	}
	for i, tr := range dataInTraces {
		if tr.Length > maxRecv {
			t.Fatalf("Data-In[%d] length=%d exceeds MaxRecvDataSegmentLength=%d", i, tr.Length, maxRecv)
		}
		if i < len(dataInTraces)-1 && tr.Flags&(iscsi.FlagS|iscsi.FlagF) != 0 {
			t.Fatalf("intermediate Data-In[%d] flags=0x%02x must not carry S/F", i, tr.Flags)
		}
		if i == len(dataInTraces)-1 && tr.Flags&(iscsi.FlagS|iscsi.FlagF) != iscsi.FlagS|iscsi.FlagF {
			t.Fatalf("final Data-In flags=0x%02x want S|F", tr.Flags)
		}
		wantOffset := uint32(i * maxRecv)
		if tr.Offset != wantOffset {
			t.Fatalf("Data-In[%d] offset=%d want %d", i, tr.Offset, wantOffset)
		}
	}
}
