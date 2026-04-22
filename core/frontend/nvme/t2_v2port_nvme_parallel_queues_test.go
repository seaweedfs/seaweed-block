// Ownership: QA (regression for m01 pre-11c BLOCKER-1 — parallel
// IO queue debug aid for sw).
// sw may NOT modify without architect approval via §8B.4 Discovery
// Bridge.
//
// Purpose: Linux kernel's `nvme connect` defaults to opening 8 IO
// queues (on m01: "mapped 8/0/0 default/read/poll queues"). mkfs
// issues concurrent Writes across those queues. None of the
// existing V3 unit tests drive parallel IO queues — every test
// uses one admin + one IO queue. If the m01 failure is a
// concurrency race inside Target or handleWrite, single-queue
// tests cannot surface it.
//
// This file exercises 8 IO queues concurrently against one
// admin controller + one target. If sw's handleWrite or CNTLID
// registry has a race, this test has the shape to expose it.
//
// Status at landing (2026-04-22): this test was GREEN on V3
// post-batch-11b. If sw breaks something during m01 debug, it
// must stay green after the fix.

package nvme_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// multiQueueClient opens an admin queue + N IO queues against one
// target, mirroring what the Linux kernel does on `nvme connect`.
type multiQueueClient struct {
	admin     net.Conn
	adminR    *nvme.Reader
	adminW    *nvme.Writer
	cntlID    uint16
	cidSeq    atomic.Uint32 // shared CID allocator

	// One IO queue per slot. queues[i] has QID = uint16(i+1).
	queues []*ioQueue
}

type ioQueue struct {
	conn net.Conn
	r    *nvme.Reader
	w    *nvme.Writer
	qid  uint16
}

// newMultiQueueClient builds one admin + numIOQueues IO queues.
func newMultiQueueClient(t *testing.T, addr string, subNQN, hostNQN string, numIOQueues int) *multiQueueClient {
	t.Helper()
	c := &multiQueueClient{}

	// Admin queue.
	c.admin = dialAndHandshake(t, addr)
	c.adminR = nvme.NewReader(c.admin)
	c.adminW = nvme.NewWriter(c.admin)
	adminCID := uint16(c.cidSeq.Add(1))
	adminCmd := nvme.CapsuleCommand{
		OpCode: 0x7F, FCType: 0x01, CID: adminCID,
		D10: uint32(0) << 16, // QID=0 admin
	}
	cd := nvme.ConnectData{
		HostID:  [16]byte{0xAA, 0xBB, 0xCC, 0xDD},
		CNTLID:  0xFFFF, SubNQN: subNQN, HostNQN: hostNQN,
	}
	cdBuf := make([]byte, 1024)
	cd.Marshal(cdBuf)
	if err := c.adminW.SendWithData(0x4, 0, &adminCmd, 64, cdBuf); err != nil {
		t.Fatalf("admin Connect send: %v", err)
	}
	adminResp := recvCapsuleResp(t, c.adminR)
	if adminResp.Status != 0 {
		t.Fatalf("admin Connect status=0x%04x", adminResp.Status)
	}
	c.cntlID = uint16(adminResp.DW0 & 0xFFFF)

	// N IO queues.
	c.queues = make([]*ioQueue, numIOQueues)
	for i := 0; i < numIOQueues; i++ {
		qid := uint16(i + 1)
		conn := dialAndHandshake(t, addr)
		q := &ioQueue{conn: conn, r: nvme.NewReader(conn), w: nvme.NewWriter(conn), qid: qid}

		ioCID := uint16(c.cidSeq.Add(1))
		ioCmd := nvme.CapsuleCommand{
			OpCode: 0x7F, FCType: 0x01, CID: ioCID,
			D10: uint32(qid) << 16,
		}
		ioCD := nvme.ConnectData{
			HostID: cd.HostID, CNTLID: c.cntlID,
			SubNQN: subNQN, HostNQN: hostNQN,
		}
		ioBuf := make([]byte, 1024)
		ioCD.Marshal(ioBuf)
		if err := q.w.SendWithData(0x4, 0, &ioCmd, 64, ioBuf); err != nil {
			t.Fatalf("IO Connect qid=%d send: %v", qid, err)
		}
		ioResp := recvCapsuleResp(t, q.r)
		if ioResp.Status != 0 {
			t.Fatalf("IO Connect qid=%d status=0x%04x", qid, ioResp.Status)
		}
		c.queues[i] = q
	}
	t.Cleanup(func() {
		for _, q := range c.queues {
			_ = q.conn.Close()
		}
		_ = c.admin.Close()
	})
	return c
}

// chunkedWriteOnQueue issues a chunked Write on the given IO queue.
// Safe to call from a goroutine (each queue owns its Reader/Writer).
func (c *multiQueueClient) chunkedWriteOnQueue(t *testing.T, q *ioQueue, slba uint64, nlb uint16, payload []byte, chunks int) (uint16, error) {
	t.Helper()
	if len(payload)%chunks != 0 {
		t.Fatalf("payload %d not divisible by chunks %d", len(payload), chunks)
	}
	chunkSize := len(payload) / chunks
	cid := uint16(c.cidSeq.Add(1))

	cmd := nvme.CapsuleCommand{
		OpCode: 0x01, CID: cid, NSID: 1,
		D10: uint32(slba & 0xFFFFFFFF),
		D11: uint32(slba >> 32),
		D12: uint32(nlb - 1),
	}
	if err := q.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		return 0, err
	}

	ch, err := q.r.Dequeue()
	if err != nil {
		return 0, err
	}
	if ch.Type == 0x5 {
		var resp nvme.CapsuleResponse
		_ = q.r.Receive(&resp)
		return resp.Status, nil
	}
	if ch.Type != 0x9 {
		t.Fatalf("qid=%d expected R2T, got 0x%x", q.qid, ch.Type)
	}
	var r2t nvme.R2THeader
	if err := q.r.Receive(&r2t); err != nil {
		return 0, err
	}

	for i := 0; i < chunks; i++ {
		off := i * chunkSize
		h := nvme.H2CDataHeader{
			CCCID: cid, TAG: r2t.TAG,
			DATAO: uint32(off), DATAL: uint32(chunkSize),
		}
		if err := q.w.SendWithData(0x6, 0, &h, 16, payload[off:off+chunkSize]); err != nil {
			return 0, err
		}
	}

	resp := recvCapsuleResp(t, q.r)
	if resp.CID != cid {
		t.Fatalf("qid=%d Write resp CID=%d want %d", q.qid, resp.CID, cid)
	}
	return resp.Status, nil
}

// parallelWriteTarget sets up a target + counting backend.
func parallelWriteTarget(t *testing.T) (*writeCountingBackend, string) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	backend := &writeCountingBackend{inner: rec}
	prov := testback.NewStaticProvider(backend)
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
	t.Cleanup(func() { _ = tg.Close() })
	return backend, addr
}

// --- QA: 8-queue concurrent 32 KiB chunked Writes (kernel mkfs pattern) ---

func TestT2V2Port_NVMe_IO_8Queues_Concurrent32KiBWrites(t *testing.T) {
	backend, addr := parallelWriteTarget(t)
	cli := newMultiQueueClient(t, addr, "nqn.2026-04.example.v3:subsys",
		"nqn.2026-04.example.host:1", 8)

	if len(cli.queues) != 8 {
		t.Fatalf("expected 8 IO queues, got %d", len(cli.queues))
	}

	const total = 32 * 1024
	const chunks = 8
	const nlb = uint16(total / 512)

	// Each queue writes a distinct 32 KiB payload at distinct SLBA
	// offsets so no overlap confuses integrity checks. SLBA unit
	// is blocks (512 B), so each queue's region starts at
	// (queueIdx * nlb).
	makePayload := func(idx int) []byte {
		p := make([]byte, total)
		for i := range p {
			p[i] = byte((i + idx*31) & 0xFF) // per-queue pattern
		}
		return p
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]uint16, len(cli.queues))
	errors := make([]error, len(cli.queues))
	for i, q := range cli.queues {
		i, q := i, q
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // synchronize launch for maximum contention
			payload := makePayload(i)
			st, err := cli.chunkedWriteOnQueue(t, q, uint64(i)*uint64(nlb), nlb, payload, chunks)
			results[i] = st
			errors[i] = err
		}()
	}

	// Deadline so a hung handleWrite doesn't wedge the whole suite.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	close(start)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent 8-queue Writes did not complete within 10s — " +
			"possible race in Target/Session (candidate for m01 BLOCKER-1 root cause)")
	}

	for i := range cli.queues {
		if errors[i] != nil {
			t.Errorf("queue %d: wire error %v", i, errors[i])
		}
		if results[i] != 0 {
			t.Errorf("queue %d: status=0x%04x SCT=%d SC=0x%02x",
				i, results[i], sctOf(results[i]), scOf(results[i]))
		}
	}

	// Integrity: 8 writes × 32 KiB = 256 KiB total.
	if got := backend.calls.Load(); got != 8 {
		t.Errorf("backend.calls=%d; want 8 (one per queue)", got)
	}
	if got := backend.bytes.Load(); got != int64(8*total) {
		t.Errorf("backend bytes=%d want %d (lost %d)", got, 8*total, int64(8*total)-got)
	}
}

// --- Admin + IO interleaving — probe CNTLID registry locking ---

// While 4 IO queues are mid-Write, admin queue concurrently issues
// Identify + GetFeatures commands. Exercises the ctrlMu lock path
// in target.go (lookupAdminController hit by IO sessions while
// admin is running other commands). If the mutex granularity is
// wrong, this would deadlock or return stale state.
func TestT2V2Port_NVMe_IO_AdminWhileIOInFlight(t *testing.T) {
	_, addr := parallelWriteTarget(t)
	cli := newMultiQueueClient(t, addr, "nqn.2026-04.example.v3:subsys",
		"nqn.2026-04.example.host:1", 4)

	// Shim to issue an admin Identify Controller using the admin
	// queue on the multiQueueClient (same shape as nvmeClient.adminIdentify).
	adminIdentify := func(cns uint8) (uint16, []byte) {
		cid := uint16(cli.cidSeq.Add(1))
		cmd := nvme.CapsuleCommand{
			OpCode: 0x06, CID: cid, NSID: 0, D10: uint32(cns),
		}
		if err := cli.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
			t.Fatalf("admin Identify send: %v", err)
		}
		ch, err := cli.adminR.Dequeue()
		if err != nil {
			t.Fatalf("admin Identify recv: %v", err)
		}
		if ch.Type == 0x5 {
			var resp nvme.CapsuleResponse
			_ = cli.adminR.Receive(&resp)
			return resp.Status, nil
		}
		var dh nvme.C2HDataHeader
		_ = cli.adminR.Receive(&dh)
		data := make([]byte, cli.adminR.Length())
		_ = cli.adminR.ReceiveData(data)
		resp := recvCapsuleResp(t, cli.adminR)
		return resp.Status, data
	}

	const total = 32 * 1024
	const chunks = 8
	const nlb = uint16(total / 512)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ioErrs := make(chan error, len(cli.queues))
	for i, q := range cli.queues {
		i, q := i, q
		go func() {
			payload := make([]byte, total)
			for k := range payload {
				payload[k] = byte((k + i*17) & 0xFF)
			}
			st, err := cli.chunkedWriteOnQueue(t, q, uint64(i)*uint64(nlb), nlb, payload, chunks)
			if err != nil {
				ioErrs <- err
				return
			}
			if st != 0 {
				ioErrs <- fmt.Errorf("queue %d status 0x%04x", i, st)
				return
			}
			ioErrs <- nil
		}()
	}

	// Fire admin commands in parallel with IO.
	for i := 0; i < 3; i++ {
		st, data := adminIdentify(0x01 /* Controller */)
		if st != 0 || len(data) != 4096 {
			t.Errorf("admin Identify during IO: status=0x%04x data=%d", st, len(data))
		}
	}

	for range cli.queues {
		select {
		case err := <-ioErrs:
			if err != nil {
				t.Errorf("IO queue error during admin-in-flight: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("IO queue hung during admin-in-flight test — possible ctrlMu contention")
		}
	}
}

