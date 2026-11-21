package durable_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

func TestDurableProvider_DataSyncStatus(t *testing.T) {
	t.Setenv("SWBLOCK_SYNC", "fsync")
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 64}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	be, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	// Neither status nor Sync may re-read the environment during this open.
	t.Setenv("SWBLOCK_SYNC", "invalid-after-open")
	if _, err := be.Write(context.Background(), 0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := be.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(p.DurableStatuses())
	if err != nil {
		t.Fatal(err)
	}
	var wire []struct {
		DataSync *struct{ Mode, Reason string }
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire) != 1 || wire[0].DataSync == nil || wire[0].DataSync.Mode != "fsync" || !strings.Contains(wire[0].DataSync.Reason, "override") {
		t.Fatalf("open-time data sync absent or changed: %s", raw)
	}
}
