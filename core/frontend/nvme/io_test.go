// Ownership: QA (from sketch-approved test spec v3-phase-15-t2-test-spec.md for T2 First Real Frontends).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t2-test-spec.md
// Maps to ledger rows: INV-FRONTEND-NVME-001, INV-FRONTEND-PROTOCOL-001,
//                      INV-FRONTEND-PROTOCOL-002.WRITE, INV-FRONTEND-PROTOCOL-002.READ
//
// Test layer: Unit (L0)
// Protocol: NVMe/TCP
// Bounded fate (mirror of iSCSI L0 with NVMe protocol mapping):
//   - NVMe IO Read/Write route every byte through
//     frontend.Backend with correct offset/length.
//   - frontend.ErrStalePrimary maps to SCT=3 SC=3 (ANA Transition).
//     No success ACK on stale WRITE; no bytes returned on stale
//     READ.
//   - Invalid input (LBA out of range, NSID mismatch, short data)
//     maps to SCT=0 with a specific SC, without reaching the
//     backend.
//
// Spec adaptation note: test-spec §4 T2.L0.n* series is specified
// as the iSCSI series with names replaced (CheckCondition ->
// ProtocolFailure etc.). This file delivers the equivalents at
// the handler level — capsule framing + fabric login arrive with
// a later checkpoint, same as the iSCSI slice.
package nvme_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// The io.go constants are lowercase (package-private). Tests use
// the public re-exports defined below for readability.
const (
	opRead  = 0x02
	opWrite = 0x01
	opFlush = 0x00
)

// T2.L0.n1 — Read and Write route through frontend.Backend.
func TestT2NVMe_CommandReadWrite_UsesFrontendBackend(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 5, EndpointVersion: 3,
	})
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: rec})
	ctx := context.Background()

	payload := make([]byte, nvme.DefaultBlockSize)
	copy(payload, []byte("hello-nvme-t2"))
	r := h.Handle(ctx, nvme.IOCommand{
		Opcode: opWrite, NSID: 1, SLBA: 0, NLB: 1, Data: payload,
	})
	if e := r.AsError(); e != nil {
		t.Fatalf("Write: %v", e)
	}
	if rec.WriteCount() != 1 {
		t.Fatalf("backend Write invoked %d times, want 1", rec.WriteCount())
	}
	w := rec.WriteAt(0)
	if w.Offset != 0 || !bytes.Equal(w.Data, payload) {
		t.Fatalf("backend Write mismatch: offset=%d len=%d", w.Offset, len(w.Data))
	}

	rr := h.Handle(ctx, nvme.IOCommand{
		Opcode: opRead, NSID: 1, SLBA: 0, NLB: 1,
	})
	if e := rr.AsError(); e != nil {
		t.Fatalf("Read: %v", e)
	}
	if !bytes.Equal(rr.Data, payload) {
		t.Fatalf("Read data mismatch")
	}
	if rec.ReadCount() != 1 {
		t.Fatalf("backend Read invoked %d times, want 1", rec.ReadCount())
	}
}

// T2.L0.n2 — stale Write returns ANA Transition; MUST NOT ACK success.
func TestT2NVMe_StaleWrite_ReturnsProtocolFailure(t *testing.T) {
	stale := testback.NewStaleRejectingBackend()
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: stale})
	ctx := context.Background()

	payload := make([]byte, nvme.DefaultBlockSize)
	r := h.Handle(ctx, nvme.IOCommand{Opcode: opWrite, NSID: 1, SLBA: 0, NLB: 1, Data: payload})

	if r.SCT == nvme.SCTGeneric && r.SC == nvme.SCSuccess {
		t.Fatal("stale Write returned Success — silent stale-ACK is a correctness violation")
	}
	e := r.AsError()
	if e == nil {
		t.Fatal("AsError nil for non-Success status")
	}
	if e.SCT != nvme.SCTPathRelated {
		t.Fatalf("SCT=0x%x want PathRelated (0x3)", e.SCT)
	}
	if e.SC != nvme.SCPathAsymAccessTransition {
		t.Fatalf("SC=0x%02x want ANATransition (0x03)", e.SC)
	}
}

// T2.L0.n3 — stale Read returns ANA Transition; MUST NOT leak bytes.
func TestT2NVMe_StaleRead_ReturnsProtocolFailure(t *testing.T) {
	stale := testback.NewStaleRejectingBackend()
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: stale})
	ctx := context.Background()

	r := h.Handle(ctx, nvme.IOCommand{Opcode: opRead, NSID: 1, SLBA: 0, NLB: 1})

	if r.SCT == nvme.SCTGeneric && r.SC == nvme.SCSuccess {
		t.Fatal("stale Read returned Success — stale-read data leak is a correctness violation")
	}
	if len(r.Data) != 0 {
		t.Fatalf("stale Read returned %d bytes — must not leak stale bytes to initiator", len(r.Data))
	}
	e := r.AsError()
	if e.SCT != nvme.SCTPathRelated || e.SC != nvme.SCPathAsymAccessTransition {
		t.Fatalf("stale Read status (SCT=0x%x SC=0x%02x) want PathRelated/ANATransition",
			e.SCT, e.SC)
	}
}

// T2.L0.n4 — invalid input fails closed without reaching backend.
func TestT2NVMe_InvalidInput_FailsClosed(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: rec})
	ctx := context.Background()

	cases := []struct {
		name   string
		cmd    nvme.IOCommand
		wantSC uint8
	}{
		{
			name:   "read_lba_past_end",
			cmd:    nvme.IOCommand{Opcode: opRead, NSID: 1, SLBA: nvme.DefaultVolumeBlocks + 1, NLB: 1},
			wantSC: nvme.SCLBAOutOfRange,
		},
		{
			name:   "write_overflow_range",
			cmd:    nvme.IOCommand{Opcode: opWrite, NSID: 1, SLBA: nvme.DefaultVolumeBlocks - 1, NLB: 4, Data: make([]byte, 4*nvme.DefaultBlockSize)},
			wantSC: nvme.SCLBAOutOfRange,
		},
		{
			name:   "write_short_data",
			cmd:    nvme.IOCommand{Opcode: opWrite, NSID: 1, SLBA: 0, NLB: 4, Data: make([]byte, nvme.DefaultBlockSize)},
			wantSC: nvme.SCInvalidField,
		},
		{
			name:   "nlb_zero",
			cmd:    nvme.IOCommand{Opcode: opWrite, NSID: 1, SLBA: 0, NLB: 0, Data: nil},
			wantSC: nvme.SCInvalidField,
		},
		{
			name:   "unknown_nsid",
			cmd:    nvme.IOCommand{Opcode: opRead, NSID: 99, SLBA: 0, NLB: 1},
			wantSC: nvme.SCInvalidField,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			beforeW := rec.WriteCount()
			beforeR := rec.ReadCount()
			r := h.Handle(ctx, tc.cmd)
			if r.SCT == nvme.SCTGeneric && r.SC == nvme.SCSuccess {
				t.Fatalf("%s: Success — must fail closed", tc.name)
			}
			if r.SCT != nvme.SCTGeneric {
				t.Fatalf("%s: SCT=0x%x want Generic (0x0)", tc.name, r.SCT)
			}
			if r.SC != tc.wantSC {
				t.Fatalf("%s: SC=0x%02x want 0x%02x", tc.name, r.SC, tc.wantSC)
			}
			if rec.WriteCount() != beforeW || rec.ReadCount() != beforeR {
				t.Fatalf("%s: backend invoked for invalid input", tc.name)
			}
		})
	}
}

// Distinction regression: backend-closed must produce a different
// wire tuple than stale-lineage (same SCT, different SC), matching
// the iSCSI slice's closed-vs-stale invariant.
func TestT2NVMe_BackendClosed_DistinctFromStale(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	_ = rec.Close()
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: rec})
	ctx := context.Background()
	r := h.Handle(ctx, nvme.IOCommand{
		Opcode: opWrite, NSID: 1, SLBA: 0, NLB: 1,
		Data: make([]byte, nvme.DefaultBlockSize),
	})
	if r.SCT != nvme.SCTPathRelated {
		t.Fatalf("SCT=0x%x want PathRelated", r.SCT)
	}
	if r.SC == nvme.SCPathAsymAccessTransition {
		t.Fatalf("closed-backend SC must NOT equal ANATransition (conflates fail modes)")
	}
	if r.SC != nvme.SCPathAsymAccessInaccessible {
		t.Fatalf("closed-backend: SC=0x%02x want ANAInaccessible (0x02)", r.SC)
	}
}

// Unknown opcode rejected without reaching backend.
func TestT2NVMe_UnknownOpcode_ReturnsInvalidOpcode(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: rec})
	r := h.Handle(context.Background(), nvme.IOCommand{
		Opcode: 0x7f, NSID: 1, SLBA: 0, NLB: 1,
	})
	if r.SCT != nvme.SCTGeneric || r.SC != nvme.SCInvalidOpcode {
		t.Fatalf("unknown opcode status (SCT=0x%x SC=0x%02x) want Generic/InvalidOpcode",
			r.SCT, r.SC)
	}
	if rec.ReadCount()+rec.WriteCount() != 0 {
		t.Fatal("unknown opcode reached backend")
	}
}

// Flush is accepted as a no-op Success in T2 scope.
func TestT2NVMe_Flush_SuccessInT2Scope(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: rec})
	r := h.Handle(context.Background(), nvme.IOCommand{Opcode: opFlush, NSID: 1})
	if r.AsError() != nil {
		t.Fatalf("Flush: %v", r.AsError())
	}
}

// AsError on a success IOResult is nil.
func TestIOResult_AsError_NilOnSuccess(t *testing.T) {
	var r nvme.IOResult
	if r.AsError() != nil {
		t.Fatal("AsError non-nil on zero (Success) result")
	}
}

// errors.As chains through IOResult.AsError().
func TestIOResult_AsError_SupportsErrorsAs(t *testing.T) {
	r := nvme.IOResult{SCT: nvme.SCTPathRelated, SC: nvme.SCPathAsymAccessTransition}
	var err error = r.AsError()
	var se *nvme.StatusError
	if !errors.As(err, &se) {
		t.Fatal("errors.As failed")
	}
	if se.SC != nvme.SCPathAsymAccessTransition {
		t.Fatalf("unwrapped SC=0x%02x", se.SC)
	}
}
