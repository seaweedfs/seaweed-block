// T3c scenario 4 of 4 — disk fill / graceful-error.
//
// Volume geometry is fixed at Create time (NumBlocks × BlockSize).
// Writing past the last LBA must return a clean error (either
// from adapter boundary check or storage LBA-out-of-range), not
// silent success and not corruption of in-range blocks.
//
// This is the V3 equivalent of V2's "disk-fill" scenario — we
// can't actually exhaust a real disk in unit tests, but the
// per-volume fixed geometry means "writing beyond volume end"
// exercises the same fail-hard property.
//
// Pins ledger row PCDD-DURABLE-DISK-FULL-001.

package durable_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

func TestT3c_Scenario_DiskFill_OutOfRange_FailsCleanly(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			view := newStubView(healthyProj(id))

			const numBlocks = uint32(4)
			const blockSize = 4096
			cfg := durable.ProviderConfig{
				Impl:        impl,
				StorageRoot: root,
				BlockSize:   blockSize,
				NumBlocks:   numBlocks,
			}

			p, err := durable.NewDurableProvider(cfg, view)
			if err != nil {
				t.Fatalf("NewDurableProvider: %v", err)
			}
			defer p.Close()
			ctx := context.Background()
			b, err := p.Open(ctx, "v1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatalf("RecoverVolume: %v", err)
			}

			// Step 1: fill the volume with a known pattern.
			filler := make([]byte, blockSize)
			for i := range filler {
				filler[i] = byte((i + 5) & 0xFF)
			}
			for lba := uint32(0); lba < numBlocks; lba++ {
				if _, err := b.Write(ctx, int64(lba)*blockSize, filler); err != nil {
					t.Fatalf("fill Write lba=%d: %v", lba, err)
				}
			}
			if err := b.Sync(ctx); err != nil {
				t.Fatalf("Sync: %v", err)
			}

			// Step 2: write past end — MUST error, not silently
			// succeed. Offset = numBlocks * blockSize (the first
			// byte BEYOND volume).
			outOfRange := int64(numBlocks) * blockSize
			_, err = b.Write(ctx, outOfRange, filler)
			if err == nil {
				t.Errorf("Write past EOV succeeded silently (impl=%s) — expected error", impl)
			}

			// Step 3: in-range blocks must still be intact after
			// the failed out-of-range write — fail-hard should not
			// corrupt prior state.
			for lba := uint32(0); lba < numBlocks; lba++ {
				got := make([]byte, blockSize)
				if _, err := b.Read(ctx, int64(lba)*blockSize, got); err != nil {
					t.Fatalf("post-fail Read lba=%d: %v", lba, err)
				}
				if !bytes.Equal(got, filler) {
					t.Fatalf("LBA %d corrupted by out-of-range write (impl=%s): first diff @ byte %d",
						lba, impl, firstDiffIdx(got, filler))
				}
			}
		})
	}
}
