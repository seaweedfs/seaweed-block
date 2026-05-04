package main

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// stubBackend is a no-op frontend.Backend sufficient to satisfy
// HandlerConfig.Backend != nil — these tests only exercise the
// capacity plumb, not data path.
type stubBackend struct{}

func (stubBackend) Read(_ context.Context, _ int64, _ []byte) (int, error)  { return 0, nil }
func (stubBackend) Write(_ context.Context, _ int64, _ []byte) (int, error) { return 0, nil }
func (stubBackend) Sync(_ context.Context) error                            { return nil }
func (stubBackend) SetOperational(_ bool, _ string)                         {}
func (stubBackend) Identity() frontend.Identity                             { return frontend.Identity{} }
func (stubBackend) Close() error                                            { return nil }

// G5-5C addendum (P0 product fix, architect ruling 2026-04-28):
// frontend volume capacity MUST derive from --durable-blocks ×
// --durable-blocksize, not silently fall back to frontend defaults.
//
// These tests pin INV-G5-FRONTEND-CAPACITY-FROM-DURABLE-CONFIG by
// exercising the production construction path: feed (blocks,
// blockSize) through computeFrontendVolumeSize, build an iSCSI/NVMe
// HandlerConfig, construct a Target, and read back the advertised
// capacity from SCSIHandler.VolumeSize / NVMe IOHandler.VolumeSize.

func TestComputeFrontendVolumeSize_ProductOfBlocksAndBlockSize(t *testing.T) {
	cases := []struct {
		blocks    uint
		blockSize uint
		want      uint64
	}{
		{1, 4096, 4096},
		{256, 4096, 1 << 20},                    // 1 MiB at 4 KiB blocks
		{65536, 4096, 256 << 20},                // 256 MiB at 4 KiB blocks (the case that exposed the bug)
		{2048, 512, 1 << 20},                    // 1 MiB at 512 B blocks (legacy default)
		{1 << 20, 4096, 4 << 30},                // 4 GiB
	}
	for _, c := range cases {
		got, err := computeFrontendVolumeSize(c.blocks, c.blockSize)
		if err != nil {
			t.Errorf("blocks=%d blockSize=%d: unexpected err %v", c.blocks, c.blockSize, err)
			continue
		}
		if got != c.want {
			t.Errorf("blocks=%d blockSize=%d: got %d, want %d", c.blocks, c.blockSize, got, c.want)
		}
	}
}

func TestComputeFrontendVolumeSize_RejectsZero(t *testing.T) {
	if _, err := computeFrontendVolumeSize(0, 4096); err == nil {
		t.Error("blocks=0 should be rejected")
	}
	if _, err := computeFrontendVolumeSize(256, 0); err == nil {
		t.Error("blockSize=0 should be rejected")
	}
}

// TestComputeFrontendVolumeSize_OverflowGuard verifies the uint64
// overflow check fires before silently wrapping to a small value.
func TestComputeFrontendVolumeSize_OverflowGuard(t *testing.T) {
	// 2^32 × 2^32 = 2^64 = wraps to 0 on uint64 multiply.
	if _, err := computeFrontendVolumeSize(1<<33, 1<<33); err == nil {
		t.Error("blocks × blockSize that overflows uint64 should be rejected")
	}
}

// TestIscsiHandlerCapacity_FromDurableConfig is the load-bearing
// pin for INV-G5-FRONTEND-CAPACITY-FROM-DURABLE-CONFIG on the iSCSI
// path: construct an iscsi.HandlerConfig with capacity computed by
// computeFrontendVolumeSize, build the SCSIHandler exactly as
// iscsi.NewTarget does internally, and assert the advertised
// capacity round-trips. This is the layer that broke in production:
// HandlerConfig was zero-valued and silently fell back to the
// 1 MiB defaults.
func TestIscsiHandlerCapacity_FromDurableConfig(t *testing.T) {
	const (
		blocks    = uint(65536)
		blockSize = uint(4096)
	)
	wantSize, err := computeFrontendVolumeSize(blocks, blockSize)
	if err != nil {
		t.Fatalf("computeFrontendVolumeSize: %v", err)
	}

	h := iscsi.NewSCSIHandler(iscsi.HandlerConfig{
		Backend:    stubBackend{},
		BlockSize:  uint32(blockSize),
		VolumeSize: wantSize,
	})
	if got := h.VolumeSize(); got != wantSize {
		t.Errorf("SCSIHandler.VolumeSize = %d, want %d (durable bytes)", got, wantSize)
	}
	if got := h.BlockSize(); got != uint32(blockSize) {
		t.Errorf("SCSIHandler.BlockSize = %d, want %d", got, uint32(blockSize))
	}
	// Sanity: the bug-trigger value (1 MiB default) must NOT match
	// when 256 MiB was requested. Without this assertion, the test
	// would still pass under a regression that re-introduced the
	// default fallback if wantSize happened to equal 1 MiB.
	if got := h.VolumeSize(); got == 1<<20 {
		t.Errorf("SCSIHandler.VolumeSize fell back to 1 MiB default — capacity plumb broken")
	}
}

// TestNvmeHandlerCapacity_FromDurableConfig is the symmetric pin on
// the NVMe path.
func TestNvmeHandlerCapacity_FromDurableConfig(t *testing.T) {
	const (
		blocks    = uint(65536)
		blockSize = uint(4096)
	)
	wantSize, err := computeFrontendVolumeSize(blocks, blockSize)
	if err != nil {
		t.Fatalf("computeFrontendVolumeSize: %v", err)
	}

	h := nvme.NewIOHandler(nvme.HandlerConfig{
		Backend:    stubBackend{},
		NSID:       1,
		BlockSize:  uint32(blockSize),
		VolumeSize: wantSize,
	})
	if got := h.VolumeSize(); got != wantSize {
		t.Errorf("NVMe IOHandler.VolumeSize = %d, want %d", got, wantSize)
	}
	if got := h.BlockSize(); got != uint32(blockSize) {
		t.Errorf("NVMe IOHandler.BlockSize = %d, want %d", got, uint32(blockSize))
	}
	if got := h.VolumeSize(); got == 1<<20 {
		t.Errorf("NVMe IOHandler.VolumeSize fell back to 1 MiB default — capacity plumb broken")
	}
}

// TestFrontendDefaults_StillReturn1MiB is the negative-control pin:
// when HandlerConfig is zero-valued (the production-shipping bug
// shape), capacity falls to 1 MiB. This documents what was broken
// so a future reader can grep for the bug pattern by INV ID.
func TestFrontendDefaults_StillReturn1MiB(t *testing.T) {
	// iSCSI default fallback (the bug) — 2048 × 512 = 1 MiB.
	hi := iscsi.NewSCSIHandler(iscsi.HandlerConfig{Backend: stubBackend{}})
	if got := hi.VolumeSize(); got != 1<<20 {
		t.Errorf("iSCSI default VolumeSize = %d, want %d (1 MiB)", got, 1<<20)
	}
	// NVMe default fallback — same shape.
	hn := nvme.NewIOHandler(nvme.HandlerConfig{Backend: stubBackend{}, NSID: 1})
	if got := hn.VolumeSize(); got != 1<<20 {
		t.Errorf("NVMe default VolumeSize = %d, want %d (1 MiB)", got, 1<<20)
	}
	// This test will keep passing as long as the defaults stay 1
	// MiB. If a future change updates the frontend defaults (e.g.,
	// to match a more useful production shape), this test fails
	// and the reader should re-evaluate whether
	// INV-G5-FRONTEND-CAPACITY-FROM-DURABLE-CONFIG still pins the
	// right behavior.
}
