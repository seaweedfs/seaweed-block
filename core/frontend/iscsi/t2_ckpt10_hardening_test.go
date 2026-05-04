// Ownership: sw regression tests for architect review 2026-04-21
// ckpt 10 Medium findings:
//
//   #1  EDTL/CDB consistency check MUST run before the target
//       allocates memory or issues R2T (was: alloc edtl bytes
//       upfront, then discover mismatch only when SCSI handler
//       validated CDB).
//
//   #2  R2T-solicited Data-Out must reject TTT=0xFFFFFFFF
//       (unsolicited marker) — previously accepted, weakening
//       the InitialR2T=Yes negotiated discipline.
//
// Both tests drive a real in-process target via the existing
// Go test client; the second bypasses the normal test client's
// TTT echo to force the bad TTT over the wire.
package iscsi_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// countingBackend wraps a RecordingBackend to expose how many
// times Write was invoked. Used to confirm a pre-flight EDTL
// rejection NEVER reaches the backend.
type countingBackend struct {
	inner *testback.RecordingBackend
	writes atomic.Int32
}

func newCountingBackend() *countingBackend {
	return &countingBackend{
		inner: testback.NewRecordingBackend(frontend.Identity{
			VolumeID: "v1", ReplicaID: "r1",
		}),
	}
}

func (b *countingBackend) Identity() frontend.Identity { return b.inner.Identity() }
func (b *countingBackend) Close() error                { return b.inner.Close() }
func (b *countingBackend) Read(ctx context.Context, off int64, p []byte) (int, error) {
	return b.inner.Read(ctx, off, p)
}
func (b *countingBackend) Write(ctx context.Context, off int64, p []byte) (int, error) {
	b.writes.Add(1)
	return b.inner.Write(ctx, off, p)
}
func (b *countingBackend) Sync(ctx context.Context) error { return b.inner.Sync(ctx) }
func (b *countingBackend) SetOperational(ok bool, evidence string) {
	b.inner.SetOperational(ok, evidence)
}
func (b *countingBackend) WriteCallCount() int32 { return b.writes.Load() }

// Architect finding #1 (memory-amplification / protocol
// consistency): a SCSI-Cmd claiming EDTL wildly larger than the
// CDB's WRITE(10) transfer length MUST be rejected at CHECK
// CONDITION BEFORE any R2T or large buffer allocation. The
// backend MUST NOT be invoked at all.
func TestT2Route_ISCSI_LargeWrite_EDTLCDBMismatch_RejectsWithoutAllocating(t *testing.T) {
	rec := newCountingBackend()
	prov := testback.NewStaticProvider(rec)
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:   "127.0.0.1:0",
		IQN:      "iqn.2026-04.example.v3:v1",
		VolumeID: "v1",
		Provider: prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tg.Close()
	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	// Build a WRITE(10) CDB asking for 1 block. The CDB
	// authoritative transfer size is 1 * 512 = 512 bytes.
	cdb := writeCDB10(0, 1)

	// Construct a SCSI-Cmd with a GIANT EDTL of 128 MiB, not
	// reflecting the CDB. Carry 512 bytes of immediate data so
	// the target would need to solicit (128 MiB - 512) via R2T
	// if it trusted EDTL without pre-flight validation.
	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpSCSICmd)
	req.SetOpSpecific1(iscsi.FlagF | iscsi.FlagW)
	req.SetLUN(0)
	req.SetInitiatorTaskTag(cli.itt)
	cli.itt++
	const hostileEDTL uint32 = 128 * 1024 * 1024
	req.SetExpectedDataTransferLength(hostileEDTL)
	req.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	req.SetCDB(cdb)
	req.DataSegment = make([]byte, iscsi.DefaultBlockSize) // 1 block immediate

	before := time.Now()
	if err := iscsi.WritePDU(cli.conn, req); err != nil {
		t.Fatalf("WritePDU: %v", err)
	}
	// Target must respond quickly with CHECK CONDITION, NOT
	// issue R2T and wait for 128 MiB of Data-Out. Use a tight
	// deadline; if we fall off we know the target tried to
	// allocate/solicit.
	_ = cli.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("ReadPDU (target should have rejected fast): %v", err)
	}
	elapsed := time.Since(before)
	if elapsed > 2*time.Second {
		t.Fatalf("target took %v to reject EDTL/CDB mismatch — expected fast fail", elapsed)
	}
	if resp.Opcode() == iscsi.OpR2T {
		t.Fatal("target issued R2T on EDTL/CDB mismatch — memory-amplification vector open")
	}
	if resp.Opcode() != iscsi.OpSCSIResp {
		t.Fatalf("expected CHECK CONDITION SCSI-Response, got %s", iscsi.OpcodeName(resp.Opcode()))
	}
	if resp.SCSIStatusByte() != iscsi.StatusCheckCondition {
		t.Fatalf("status=0x%02x want CHECK CONDITION", resp.SCSIStatusByte())
	}
	// Sense data rides in the data segment as fixed-format.
	if len(resp.DataSegment) < 14 {
		t.Fatalf("response missing sense data: len=%d", len(resp.DataSegment))
	}
	senseKey := resp.DataSegment[2] & 0x0f
	asc := resp.DataSegment[12]
	if senseKey != iscsi.SenseIllegalRequest {
		t.Fatalf("sense key=0x%02x want IllegalRequest", senseKey)
	}
	if asc != iscsi.ASCInvalidFieldInCDB {
		t.Fatalf("ASC=0x%02x want InvalidFieldInCDB (0x24)", asc)
	}
	if rec.WriteCallCount() != 0 {
		t.Fatalf("backend Write invoked %d times on pre-flight reject (must be 0)",
			rec.WriteCallCount())
	}
}

// Architect finding #2: R2T-solicited Data-Out with
// TTT=0xFFFFFFFF MUST be rejected (session closes with error).
// The negotiated InitialR2T=Yes discipline requires the
// initiator to echo the R2T's TTT exactly.
func TestT2Route_ISCSI_LargeWrite_WrongTTT_OnDataOut_Rejected(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	prov := testback.NewStaticProvider(rec)
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:   "127.0.0.1:0",
		IQN:      "iqn.2026-04.example.v3:v1",
		VolumeID: "v1",
		Provider: prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tg.Close()
	cli := dialAndLogin(t, addr)

	// 4-block write; 1 block immediate, 3 blocks solicited.
	const blocks = 4
	const immediateBlocks = 1
	blockSize := int(iscsi.DefaultBlockSize)
	payload := make([]byte, blocks*blockSize)

	req := &iscsi.PDU{}
	req.SetOpcode(iscsi.OpSCSICmd)
	req.SetOpSpecific1(iscsi.FlagF | iscsi.FlagW)
	req.SetLUN(0)
	itt := cli.itt
	req.SetInitiatorTaskTag(itt)
	cli.itt++
	req.SetExpectedDataTransferLength(uint32(len(payload)))
	req.SetCmdSN(cli.cmdSN)
	cli.cmdSN++
	req.SetCDB(writeCDB10(0, blocks))
	req.DataSegment = payload[:immediateBlocks*blockSize]
	if err := iscsi.WritePDU(cli.conn, req); err != nil {
		t.Fatalf("WritePDU(SCSI-Cmd): %v", err)
	}

	// Expect R2T. Read it but DELIBERATELY ignore the TTT it
	// carries when we build the Data-Out.
	_ = cli.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	r2t, err := iscsi.ReadPDU(cli.conn)
	if err != nil {
		t.Fatalf("read R2T: %v", err)
	}
	if r2t.Opcode() != iscsi.OpR2T {
		t.Fatalf("expected R2T, got %s", iscsi.OpcodeName(r2t.Opcode()))
	}

	// Build a Data-Out with the UNSOLICITED marker TTT=0xFFFFFFFF
	// instead of echoing R2T.TargetTransferTag(). Target must
	// reject.
	out := &iscsi.PDU{}
	out.SetOpcode(iscsi.OpSCSIDataOut)
	out.SetOpSpecific1(iscsi.FlagF)
	out.SetLUN(0)
	out.SetInitiatorTaskTag(itt)
	out.SetTargetTransferTag(0xFFFFFFFF) // WRONG — unsolicited marker
	out.SetDataSN(0)
	out.SetBufferOffset(r2t.BufferOffset())
	out.DataSegment = payload[immediateBlocks*blockSize:]
	if err := iscsi.WritePDU(cli.conn, out); err != nil {
		t.Fatalf("WritePDU(Data-Out): %v", err)
	}

	// Target should close the connection with a session error.
	// Next read from our side surfaces EOF or a reset.
	_ = cli.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, readErr := iscsi.ReadPDU(cli.conn)
	if readErr == nil {
		t.Fatal("target accepted Data-Out with TTT=0xFFFFFFFF; R2T discipline weakened")
	}
	// Any connection-level error is acceptable: EOF, reset,
	// broken pipe. The important thing is the target refused
	// to commit the write.
	_ = net.IPv4zero // silence unused-import linter
	if rec.WriteCount() != 0 {
		t.Fatalf("backend Write invoked %d times under bad-TTT rejection; expected 0",
			rec.WriteCount())
	}
	_ = cli.conn.Close()
}
