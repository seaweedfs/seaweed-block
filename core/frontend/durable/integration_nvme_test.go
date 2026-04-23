// T3b integration — NVMe handler against DurableProvider-sourced
// StorageBackend. Matrix-parameterized over walstore + smartwal.
// Complements TestT3b_NVMe_Flush_DispatchesToBackend with real
// round-trip via LogicalStorage.

package durable_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// TestT3b_Integration_NVMe_WriteReadFlush_Matrix exercises a full
// NVMe Write → Flush → Read round-trip through the NVMe IOHandler
// against both walstore and smartwal impls.
func TestT3b_Integration_NVMe_WriteReadFlush_Matrix(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			const blockSize = 4096
			const numBlocks = 16
			backend := openDurableBackend(t, impl, numBlocks, blockSize)
			h := nvme.NewIOHandler(nvme.HandlerConfig{
				Backend:    backend,
				BlockSize:  blockSize,
				VolumeSize: uint64(numBlocks) * blockSize,
				NSID:       1,
			})
			ctx := context.Background()

			// Write 1 block at LBA 3.
			payload := make([]byte, blockSize)
			for i := range payload {
				payload[i] = byte((i*5 + 19) & 0xFF)
			}
			wRes := h.Handle(ctx, nvme.IOCommand{
				Opcode: 0x01, NSID: 1, SLBA: 3, NLB: 1, Data: payload,
			})
			if wRes.AsError() != nil {
				t.Fatalf("NVMe Write: %v", wRes.AsError())
			}

			// Flush (opcode 0x00) — must succeed.
			fRes := h.Handle(ctx, nvme.IOCommand{
				Opcode: 0x00, NSID: 1,
			})
			if fRes.AsError() != nil {
				t.Fatalf("NVMe Flush: %v", fRes.AsError())
			}

			// Read back.
			rRes := h.Handle(ctx, nvme.IOCommand{
				Opcode: 0x02, NSID: 1, SLBA: 3, NLB: 1,
			})
			if rRes.AsError() != nil {
				t.Fatalf("NVMe Read: %v", rRes.AsError())
			}
			if !bytes.Equal(rRes.Data, payload) {
				t.Fatalf("Read payload mismatch (impl=%s)", impl)
			}

			// Read unwritten LBA — zeros.
			rZero := h.Handle(ctx, nvme.IOCommand{
				Opcode: 0x02, NSID: 1, SLBA: 7, NLB: 1,
			})
			if rZero.AsError() != nil {
				t.Fatalf("NVMe Read unwritten: %v", rZero.AsError())
			}
			if !isAllZeros(rZero.Data) {
				t.Fatalf("unwritten LBA non-zero (impl=%s)", impl)
			}
		})
	}
}

// TestT3b_Integration_NVMe_MultiBlockWrite pins multi-block write
// translation through the adapter for the NVMe side.
func TestT3b_Integration_NVMe_MultiBlockWrite(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			const blockSize = 4096
			const numBlocks = 16
			backend := openDurableBackend(t, impl, numBlocks, blockSize)
			h := nvme.NewIOHandler(nvme.HandlerConfig{
				Backend:    backend,
				BlockSize:  blockSize,
				VolumeSize: uint64(numBlocks) * blockSize,
				NSID:       1,
			})
			ctx := context.Background()

			// 4 blocks at LBA 2.
			const nlb = uint32(4)
			payload := make([]byte, int(nlb)*blockSize)
			for i := range payload {
				payload[i] = byte((i*11 + 7) & 0xFF)
			}
			wRes := h.Handle(ctx, nvme.IOCommand{
				Opcode: 0x01, NSID: 1, SLBA: 2, NLB: nlb, Data: payload,
			})
			if wRes.AsError() != nil {
				t.Fatalf("NVMe multi-block Write: %v", wRes.AsError())
			}
			if r := h.Handle(ctx, nvme.IOCommand{Opcode: 0x00, NSID: 1}); r.AsError() != nil {
				t.Fatalf("NVMe Flush: %v", r.AsError())
			}
			rRes := h.Handle(ctx, nvme.IOCommand{
				Opcode: 0x02, NSID: 1, SLBA: 2, NLB: nlb,
			})
			if rRes.AsError() != nil {
				t.Fatalf("NVMe multi-block Read: %v", rRes.AsError())
			}
			if !bytes.Equal(rRes.Data, payload) {
				for i, b := range rRes.Data {
					if b != payload[i] {
						t.Fatalf("NVMe multi-block diff at byte %d: got 0x%02x want 0x%02x (impl=%s)",
							i, b, payload[i], impl)
					}
				}
			}
		})
	}
}
