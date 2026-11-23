package durable_test

import (
	"bytes"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A volume is not created on a durable root without room for its full size
// and its WAL (operator guide, Size a host; issue 484), and a refused create
// leaves nothing behind: no store, no chunk fresh marker or other sidecar.
func TestCreateRefusesWithoutLocalHeadroom(t *testing.T) {
	defer durable.SetRootFreeForTest(8 << 20)()
	for _, impl := range []durable.ImplName{durable.ImplWALStore, durable.ImplWALStoreChunk} {
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			cfg := durable.ProviderConfig{Impl: impl, StorageRoot: root, BlockSize: 4096, NumBlocks: 65536}
			if impl == durable.ImplWALStoreChunk {
				remote := storage.NewMemChunkRemote()
				cfg.ChunkRemoteFor = func(string) (storage.ChunkRemote, error) { return remote, nil }
			}
			p, err := durable.NewDurableProvider(cfg, v1View())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.Close() })
			_, err = p.EnsureStorage("v1")
			if err == nil || !strings.Contains(err.Error(), "8388608 bytes free") || !strings.Contains(err.Error(), "needs 335544320") {
				t.Fatalf("create on a root with 8 MiB free: %v", err)
			}
			var left []string
			_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					left = append(left, path)
				}
				return nil
			})
			if len(left) != 0 {
				t.Fatalf("a refused create left %v", left)
			}
		})
	}
}

// An existing volume still opens on a root short of headroom, with a loud
// warning when its remaining data growth plus the WAL exceed the free space.
// A fresh 256 MiB volume holds only its WAL: 256 MiB free is short of the
// 256 MiB of data still to grow plus the 64 MiB WAL; 330 MiB free is not.
func TestOpenWarnsWithoutLocalHeadroom(t *testing.T) {
	root := t.TempDir()
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStore, StorageRoot: root, BlockSize: 4096, NumBlocks: 65536}
	p, err := durable.NewDurableProvider(cfg, v1View())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	p.Close()
	for _, c := range []struct {
		free int64
		warn bool
	}{{256 << 20, true}, {330 << 20, false}} {
		var out bytes.Buffer
		log.SetOutput(&out)
		restore := durable.SetRootFreeForTest(c.free)
		p, err := durable.NewDurableProvider(cfg, v1View())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.EnsureStorage("v1"); err != nil {
			t.Fatalf("an existing volume did not open with %d bytes free: %v", c.free, err)
		}
		p.Close()
		restore()
		log.SetOutput(os.Stderr)
		if got := strings.Contains(out.String(), "WARNING volume v1"); got != c.warn {
			t.Fatalf("free %d: warning=%v, want %v:\n%s", c.free, got, c.warn, out.String())
		}
	}
}
