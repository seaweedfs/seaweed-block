package durable_test

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// A volume is not created on a durable root without room for its full size
// and its WAL (operator guide, Size a host; issue 484): a full local disk
// fails guest writes, so the cure comes before the first write.
func TestCreateRefusesWithoutLocalHeadroom(t *testing.T) {
	defer durable.SetRootFreeForTest(8 << 20)()
	root := t.TempDir()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: root, BlockSize: 4096, NumBlocks: 65536}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	_, err = p.EnsureStorage("v1")
	if err == nil || !strings.Contains(err.Error(), "8388608 bytes free") || !strings.Contains(err.Error(), "needs 335544320") {
		t.Fatalf("create on a root with 8 MiB free: %v", err)
	}
	if left, _ := filepath.Glob(filepath.Join(root, "*", "*.bin")); len(left) != 0 {
		t.Fatalf("a refused create left %v", left)
	}
}

// An existing volume still opens on a root that is short of headroom, with a
// loud warning: refusing it would leave the acknowledged data unserved.
func TestOpenWarnsWithoutLocalHeadroom(t *testing.T) {
	root := t.TempDir()
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: root, BlockSize: 4096, NumBlocks: 64}
	p, err := durable.NewDurableProvider(cfg, v1View())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	p.Close()
	var out bytes.Buffer
	log.SetOutput(&out)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	defer durable.SetRootFreeForTest(4096)()
	p, err = durable.NewDurableProvider(cfg, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatalf("an existing volume did not open on a short root: %v", err)
	}
	if !strings.Contains(out.String(), "WARNING") || !strings.Contains(out.String(), "4096 bytes free") {
		t.Fatalf("no headroom warning:\n%s", out.String())
	}
}
