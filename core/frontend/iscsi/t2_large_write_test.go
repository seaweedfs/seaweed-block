// Ownership: sw regression tests for T2 ckpt 10 — multi-PDU
// R2T large-write path. V2 iSCSI dataio.go intent ported
// as the smallest required R2T mechanism (assignment §4.3
// "Port smallest required R2T path; keep writes routed
// through frontend.Backend.Write").
//
// Scope:
//   - L1 component level, in-process target + Go initiator.
//   - Writes that exceed the SCSI-Cmd immediate-data slot
//     force the target to emit R2T; the client feeds Data-Out
//     PDU(s); target assembles the full payload and hands it
//     to frontend.Backend.Write as one call.
//   - Stale-lineage rejection still holds across the R2T path.
//     No stale-path weakening (stop-rule §4.3 preserved).
package iscsi_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// writeCDB10 from client_support_test is SCSI WRITE(10)
// (LBA + transferLen), which we reuse here.

// TestT2Route_ISCSI_LargeWrite_R2TPath_Component:
// 16-block write (8 KiB) where only 4 blocks of immediate data
// are carried in the SCSI-Cmd. The remaining 12 blocks arrive
// via an R2T-solicited Data-Out PDU.
//
// Verifies:
//   1. Target emits R2T for the remainder (no immediate-only
//      acceptance).
//   2. Backend.Write is called ONCE with the full 8 KiB
//      assembled in correct order (R2T offset honored).
//   3. SCSI Response Status=Good.
//   4. A subsequent READ(10) returns identical bytes — proves
//      R2T-assembled data lands at the right LBA.
func TestT2Route_ISCSI_LargeWrite_R2TPath_Component(t *testing.T) {
	const blocks = 16
	const immediateBlocks = 4
	const blockSize = uint32(iscsi.DefaultBlockSize)
	const totalBytes = int(blockSize) * blocks

	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3,
	})
	prov := testback.NewStaticProvider(rec)
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:   "127.0.0.1:0",
		IQN:      "iqn.2026-04.example.v3:v1",
		VolumeID: "v1",
		Provider: prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	// Build a deterministic payload: byte i = i % 251 (prime,
	// avoids aligning with 512-byte block boundaries).
	payload := make([]byte, totalBytes)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	immediate := payload[:int(blockSize)*immediateBlocks]
	solicited := payload[int(blockSize)*immediateBlocks:]

	// scsiCmdFull drives: SCSI-Cmd(EDTL=totalBytes, immediate) →
	// target R2T → client Data-Out(solicited) → SCSI-Response.
	status, _ := cli.scsiCmdFull(t, writeCDB10(0, uint16(blocks)), immediate, solicited, 0)
	expectGood(t, status, "large WRITE(10) via R2T")

	// Backend saw exactly ONE write. Target must assemble all
	// fragments before dispatching.
	if rec.WriteCount() != 1 {
		t.Fatalf("backend Write invoked %d times, want 1", rec.WriteCount())
	}
	w := rec.WriteAt(0)
	if w.Offset != 0 {
		t.Fatalf("backend Write offset=%d want 0", w.Offset)
	}
	if len(w.Data) != totalBytes {
		t.Fatalf("backend Write len=%d want %d", len(w.Data), totalBytes)
	}
	if !bytes.Equal(w.Data, payload) {
		// Find first mismatch for diagnostics.
		for i := range payload {
			if w.Data[i] != payload[i] {
				t.Fatalf("R2T-assembled payload mismatch at byte %d: got 0x%02x want 0x%02x",
					i, w.Data[i], payload[i])
			}
		}
	}

	// Round-trip: READ(10) 16 blocks back. Data-In currently fits
	// in one PDU (MaxRecvDataSegmentLength default 256 KiB).
	status, readBack := cli.scsiCmdFull(t, readCDB10(0, uint16(blocks)), nil, nil, totalBytes)
	expectGood(t, status, "READ(10) after large write")
	if !bytes.Equal(readBack, payload) {
		t.Fatalf("READ after large write: mismatch (len=%d want %d)", len(readBack), totalBytes)
	}
}

// TestT2Route_ISCSI_LargeWrite_ChunksR2TByMaxBurst:
// Linux initiators issue filesystem writes larger than one
// negotiated MaxBurstLength. The target must chunk the transfer
// across multiple R2Ts instead of rejecting the command or
// emitting one oversized R2T for the whole EDTL.
func TestT2Route_ISCSI_LargeWrite_ChunksR2TByMaxBurst(t *testing.T) {
	const blocks = 1024 // 512 KiB at the default 512-byte SCSI block size.
	const maxBurst = 64 * 1024
	const blockSize = uint32(iscsi.DefaultBlockSize)
	const totalBytes = int(blockSize) * blocks

	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3,
	})
	prov := testback.NewStaticProvider(rec)
	neg := iscsi.DefaultNegotiableConfig()
	neg.MaxBurstLength = maxBurst
	neg.FirstBurstLength = maxBurst
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
		payload[i] = byte((i * 17) % 251)
	}

	status, _, traces := cli.scsiCmdFullWithR2TTrace(t, writeCDB10(0, uint16(blocks)), nil, payload, 0)
	expectGood(t, status, "large WRITE(10) chunked by MaxBurst")
	if len(traces) < 2 {
		t.Fatalf("R2T count=%d want multiple chunks", len(traces))
	}
	for i, tr := range traces {
		if tr.Desired > maxBurst {
			t.Fatalf("R2T[%d] DesiredDataLength=%d exceeds MaxBurstLength=%d", i, tr.Desired, maxBurst)
		}
	}

	if rec.WriteCount() != 1 {
		t.Fatalf("backend Write invoked %d times, want 1", rec.WriteCount())
	}
	w := rec.WriteAt(0)
	if len(w.Data) != totalBytes {
		t.Fatalf("backend Write len=%d want %d", len(w.Data), totalBytes)
	}
	if !bytes.Equal(w.Data, payload) {
		t.Fatal("chunked R2T write payload mismatch")
	}
}

// TestT2Route_ISCSI_LargeWrite_OnStaleBackend_RejectsAfterR2T:
// stale-lineage invariant holds even when the write goes
// through the R2T path. The target MUST collect the full
// solicited payload (protocol correctness) THEN reject the
// SCSI command as CHECK CONDITION — not silently ACK because
// the R2T sequence completed.
//
// This pins the stop-rule (§4.3 "If OS initiator compatibility
// appears to require weakening stale read/write failure
// semantics, fail the checkpoint"): R2T completion is protocol
// state, NOT commit authorization.
func TestT2Route_ISCSI_LargeWrite_OnStaleBackend_RejectsAfterR2T(t *testing.T) {
	const blocks = 8
	const immediateBlocks = 2
	const blockSize = uint32(iscsi.DefaultBlockSize)
	const totalBytes = int(blockSize) * blocks

	stale := testback.NewStaleRejectingBackend()
	prov := testback.NewStaticProvider(stale)
	tg := iscsi.NewTarget(iscsi.TargetConfig{
		Listen:   "127.0.0.1:0",
		IQN:      "iqn.2026-04.example.v3:v1",
		VolumeID: "v1",
		Provider: prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer tg.Close()

	cli := dialAndLogin(t, addr)
	defer cli.logout(t)

	payload := make([]byte, totalBytes)
	immediate := payload[:int(blockSize)*immediateBlocks]
	solicited := payload[int(blockSize)*immediateBlocks:]
	status, _ := cli.scsiCmdFull(t, writeCDB10(0, uint16(blocks)), immediate, solicited, 0)

	expectCheckCondition(t, status, "large WRITE(10) on stale backend")
}

// TestT2Route_ISCSI_ImmediateOnlyWrite_StillWorks:
// regression guard — small writes that fit entirely in the
// SCSI-Cmd's immediate-data slot must continue to work without
// triggering R2T. (The handler decides based on EDTL vs
// len(immediate).)
func TestT2Route_ISCSI_ImmediateOnlyWrite_StillWorks(t *testing.T) {
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
	defer cli.logout(t)

	// One-block write entirely as immediate data.
	payload := make([]byte, iscsi.DefaultBlockSize)
	status, _ := cli.scsiCmdFull(t, writeCDB10(0, 1), payload, nil, 0)
	expectGood(t, status, "immediate-only WRITE")
	if rec.WriteCount() != 1 {
		t.Fatalf("WriteCount=%d want 1", rec.WriteCount())
	}
}

// Compile-time check: context is used elsewhere in the package;
// keep the import live if this file grows.
var _ = context.Background
