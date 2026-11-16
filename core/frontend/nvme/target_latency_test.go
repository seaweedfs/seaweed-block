package nvme_test

import (
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"path/filepath"
	"testing"
)

func TestNVMeTargetDurabilityLatency(t *testing.T) {
	_, backend, c := newSyncPairFixture(t, filepath.Join(t.TempDir(), "timing.bin"))
	cmd := nvme.CapsuleCommand{OpCode: 1, CID: 333, NSID: 1, D12: 1 << 30}
	if err := c.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatal(err)
	}
	ch, err := c.ioR.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if ch.Type != 0x9 {
		t.Fatalf("R2T type %x", ch.Type)
	}
	var r2t nvme.R2THeader
	if err := c.ioR.Receive(&r2t); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, nvme.DefaultBlockSize)
	h := nvme.H2CDataHeader{CCCID: 333, TAG: r2t.TAG, DATAL: uint32(len(data))}
	if err := c.ioW.SendWithData(0x6, 0, &h, 16, data); err != nil {
		t.Fatal(err)
	}
	if r := recvCapsuleResp(t, c.ioR); r.Status != 0 || r.CID != 333 {
		t.Fatal(r)
	}
	if status := syncPairFlush(t, c, 334); status != 0 {
		t.Fatal(status)
	}
	s := backend.WriteProfile().DurabilityLatency.Frontend
	if s.Writes != 1 || s.FUAWrites != 1 || s.Flushes != 1 || s.Failed != 0 || s.RequestTotal.Count != 2 || s.ReceiveToBackend.Count != 2 || s.AckDispatch.Count != 2 || s.FUATotal.Count != 1 || s.FlushTotal.Count != 1 {
		t.Fatalf("real TCP Target counters %+v", s)
	}
}
