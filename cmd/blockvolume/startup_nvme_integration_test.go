//go:build linux && subprocess && chunkstore_integration && systemd_integration

package main_test

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// This small initiator uses the same exported wire types and admin/IO queue
// handshake as core/frontend/nvme/client_support_test.go. It talks only to the
// real blockvolume in this harness; no kernel device or unrelated NQN is used.
type gap1NVMe struct {
	admin, io net.Conn
	r         *nvme.Reader
	w         *nvme.Writer
	cid       uint16
}

func gap1ConnectNVMe(t *testing.T, addr, nqn string) *gap1NVMe {
	t.Helper()
	c := &gap1NVMe{}
	t.Cleanup(c.close)
	var controller uint16 = 0xffff
	for q := uint16(0); q < 2; q++ {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if q == 0 {
			c.admin = conn
		} else {
			c.io = conn
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		r, w := nvme.NewReader(conn), nvme.NewWriter(conn)
		if err := w.SendHeaderOnly(0, &nvme.ICRequest{PDUFormatVersion: 0, PDUMaxR2T: 1}, 120); err != nil {
			t.Fatal(err)
		}
		h, err := r.Dequeue()
		if err != nil || h.Type != 1 {
			t.Fatalf("NVMe IC response: %+v %v", h, err)
		}
		var ic nvme.ICResponse
		if err := r.Receive(&ic); err != nil {
			t.Fatal(err)
		}
		c.cid++
		cmd := nvme.CapsuleCommand{OpCode: 0x7f, FCType: 1, CID: c.cid, D10: uint32(q) << 16}
		data := make([]byte, 1024)
		(&nvme.ConnectData{HostID: [16]byte{1, 2, 3, 4}, CNTLID: controller, SubNQN: nqn, HostNQN: "nqn.2026-11.test:gap1-host"}).Marshal(data)
		if err := w.SendWithData(4, 0, &cmd, 64, data); err != nil {
			t.Fatal(err)
		}
		reply := gap1NVMeReply(t, r, c.cid)
		if q == 0 {
			controller = uint16(reply.DW0)
			if controller == 0 {
				t.Fatal("no controller assigned")
			}
		}
		if q == 1 {
			c.r, c.w = r, w
		}
	}
	return c
}

func (c *gap1NVMe) close() {
	if c.io != nil {
		_ = c.io.Close()
	}
	if c.admin != nil {
		_ = c.admin.Close()
	}
}

func gap1NVMeReply(t *testing.T, r *nvme.Reader, cid uint16) nvme.CapsuleResponse {
	t.Helper()
	h, err := r.Dequeue()
	if err != nil || h.Type != 5 {
		t.Fatalf("NVMe capsule header: %+v %v", h, err)
	}
	var reply nvme.CapsuleResponse
	if err := r.Receive(&reply); err != nil {
		t.Fatal(err)
	}
	if reply.CID != cid || reply.Status != 0 {
		t.Fatalf("NVMe CID %d: %+v", cid, reply)
	}
	return reply
}

func (c *gap1NVMe) writeFlush(t *testing.T, lba uint64, payload []byte) {
	t.Helper()
	if len(payload) != 4096 {
		t.Fatal("harness writes exactly one block")
	}
	c.cid++
	cmd := nvme.CapsuleCommand{OpCode: 1, CID: c.cid, NSID: 1, D10: uint32(lba), D11: uint32(lba >> 32)}
	if err := c.w.SendHeaderOnly(4, &cmd, 64); err != nil {
		t.Fatal(err)
	}
	h, err := c.r.Dequeue()
	if err != nil || h.Type != 9 {
		t.Fatalf("NVMe R2T: %+v %v", h, err)
	}
	var r2t nvme.R2THeader
	if err := c.r.Receive(&r2t); err != nil {
		t.Fatal(err)
	}
	if r2t.CCCID != c.cid || r2t.DATAO != 0 || r2t.DATAL != uint32(len(payload)) {
		t.Fatalf("wrong R2T %+v", r2t)
	}
	data := nvme.H2CDataHeader{CCCID: c.cid, TAG: r2t.TAG, DATAO: 0, DATAL: uint32(len(payload))}
	if err := c.w.SendWithData(6, 0, &data, 16, payload); err != nil {
		t.Fatal(err)
	}
	gap1NVMeReply(t, c.r, c.cid)
	c.cid++
	flush := nvme.CapsuleCommand{OpCode: 0, CID: c.cid, NSID: 1}
	if err := c.w.SendHeaderOnly(4, &flush, 64); err != nil {
		t.Fatal(err)
	}
	gap1NVMeReply(t, c.r, c.cid) // only this completed Flush makes the record durable
}

func (c *gap1NVMe) readExact(t *testing.T, lba uint64, want []byte) {
	t.Helper()
	c.cid++
	cmd := nvme.CapsuleCommand{OpCode: 2, CID: c.cid, NSID: 1, D10: uint32(lba), D11: uint32(lba >> 32)}
	if err := c.w.SendHeaderOnly(4, &cmd, 64); err != nil {
		t.Fatal(err)
	}
	h, err := c.r.Dequeue()
	if err != nil || h.Type != 7 {
		t.Fatalf("NVMe read data: %+v %v", h, err)
	}
	var header nvme.C2HDataHeader
	if err := c.r.Receive(&header); err != nil {
		t.Fatal(err)
	}
	if header.CCCID != c.cid || header.DATAO != 0 || header.DATAL != uint32(len(want)) || c.r.Length() != len(want) {
		t.Fatalf("wrong read frame %+v length%d", header, c.r.Length())
	}
	got := make([]byte, c.r.Length())
	if err := c.r.ReceiveData(got); err != nil {
		t.Fatal(err)
	}
	gap1NVMeReply(t, c.r, c.cid)
	if !bytes.Equal(got, want) {
		t.Fatal("fsync-acknowledged NVMe record changed after supervised restart")
	}
}

// Calibration of the witness client only, not a systemd/re-grant proof.
func TestGAP1NVMeWitnessRoundTripsAcknowledgedBlock(t *testing.T) {
	backend := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1})
	nqn := "nqn.2026-11.test:gap1-witness-calibration"
	target := nvme.NewTarget(nvme.TargetConfig{Listen: "127.0.0.1:0", SubsysNQN: nqn, VolumeID: "v", Provider: testback.NewStaticProvider(backend), Handler: nvme.HandlerConfig{NSID: 1, BlockSize: 4096, VolumeSize: 64 * 4096}})
	addr, err := target.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	c := gap1ConnectNVMe(t, addr, nqn)
	want := bytes.Repeat([]byte{0x6d}, 4096)
	c.writeFlush(t, 8, want)
	c.readExact(t, 8, want)
	if backend.WriteCount() != 1 || backend.SyncCount() != 1 || backend.ReadCount() != 1 {
		t.Fatalf("witness bypassed backend write/flush/read: %d/%d/%d", backend.WriteCount(), backend.SyncCount(), backend.ReadCount())
	}
}
