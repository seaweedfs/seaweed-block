package durable_test

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// INV-FE-PRIMARY-READY-OPERATIONAL-001: a non-chunk durable volume admits I/O
// right after open and recovery (the role, primary or supporting, does not
// enter), so its first-serving channel is already closed and its frontends
// start at once, as before the gate. A later loss of readiness does not
// reopen the channel (the process exit owns that window).
func TestDurableProvider_NonChunkVolumeServesRightAfterOpen(t *testing.T) {
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 64,
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	b := p.Backend("v1")
	if b == nil {
		t.Fatal("no backend after EnsureStorage")
	}
	select {
	case <-b.FirstServing():
		t.Fatal("first-serving closed before recovery")
	default:
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.FirstServing():
	default:
		t.Fatal("a recovered non-chunk volume has not admitted I/O: its frontends would wait")
	}
	b.SetOperational(false, "test: readiness lost")
	select {
	case <-b.FirstServing():
	default:
		t.Fatal("a later loss of readiness reopened the first-serving channel")
	}
}
