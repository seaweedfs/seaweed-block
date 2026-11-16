package durable_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// The empty selector (ImplAuto) creates new volumes as walstore and opens an
// existing volume with the implementation its on-disk superblock names, so
// volumes created under the old smartwal default keep opening. An explicit
// selector still refuses a different on-disk format
// (TestT3b_DurableProvider_Open_ImplKindMismatch_FailsFast).

func autoProvider(t *testing.T, root string, impl durable.ImplName) *durable.DurableProvider {
	t.Helper()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:        impl,
		StorageRoot: root,
		BlockSize:   4096,
		NumBlocks:   16,
	}, newStubView(healthyProj(id)))
	if err != nil {
		t.Fatalf("NewDurableProvider(%q): %v", impl, err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func onDiskMagic(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	magic := make([]byte, 4)
	if _, err := io.ReadFull(f, magic); err != nil {
		t.Fatal(err)
	}
	return string(magic)
}

func statusImpl(t *testing.T, p *durable.DurableProvider) string {
	t.Helper()
	st := p.DurableStatuses()
	if len(st) != 1 {
		t.Fatalf("DurableStatuses: %d entries, want 1", len(st))
	}
	return st[0].Impl
}

func TestDurableProvider_AutoCreatesWALStore(t *testing.T) {
	if durable.ImplAuto != "" {
		t.Fatalf("ImplAuto=%q; the zero value must be auto", durable.ImplAuto)
	}
	root := t.TempDir()
	p := autoProvider(t, root, durable.ImplAuto)
	defer p.Close()
	if _, err := p.Open(context.Background(), "v1"); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := statusImpl(t, p); got != string(durable.ImplWALStore) {
		t.Fatalf("status impl=%q, want walstore", got)
	}
	if got := onDiskMagic(t, filepath.Join(root, "v1.bin")); got != "SWBK" {
		t.Fatalf("on-disk magic=%q, want SWBK (walstore)", got)
	}
}

func TestDurableProvider_AutoOpensExistingVolumeByOnDiskFormat(t *testing.T) {
	for _, impl := range []durable.ImplName{durable.ImplSmartWAL, durable.ImplWALStore, durable.ImplParallelWALStore} {
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			created := autoProvider(t, root, impl)
			if _, err := created.Open(context.Background(), "v1"); err != nil {
				t.Fatalf("create %s: %v", impl, err)
			}
			if err := created.Close(); err != nil {
				t.Fatalf("close %s: %v", impl, err)
			}

			p := autoProvider(t, root, durable.ImplAuto)
			defer p.Close()
			if _, err := p.Open(context.Background(), "v1"); err != nil {
				if errors.Is(err, durable.ErrImplKindMismatch) {
					t.Fatalf("auto refused an existing %s volume: %v", impl, err)
				}
				t.Fatalf("auto Open of an existing %s volume: %v", impl, err)
			}
			if got := statusImpl(t, p); got != string(impl) {
				t.Fatalf("status impl=%q, want the on-disk %q", got, impl)
			}
		})
	}
}

func TestDurableProvider_AutoRefusesUnknownOnDiskFormat(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "v1.bin"), make([]byte, 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	p := autoProvider(t, root, durable.ImplAuto)
	defer p.Close()
	if _, err := p.Open(context.Background(), "v1"); !errors.Is(err, durable.ErrImplKindMismatch) {
		t.Fatalf("auto Open of an unrecognized file: %v, want ErrImplKindMismatch", err)
	}
}
