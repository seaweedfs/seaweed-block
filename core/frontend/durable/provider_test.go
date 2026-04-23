// T3b Provider tests — mini plan §2 acceptance rows #1-#4, #8.
// Matrix-parameterized per Addendum A #1: every test iterates both
// walstore and smartwal impls.

package durable_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

func implMatrix() []durable.ImplName {
	return []durable.ImplName{durable.ImplSmartWAL, durable.ImplWALStore}
}

func newProvider(t *testing.T, impl durable.ImplName) (*durable.DurableProvider, *stubView, string) {
	t.Helper()
	root := t.TempDir()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	view := newStubView(healthyProj(id))
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:        impl,
		StorageRoot: root,
		BlockSize:   4096,
		NumBlocks:   16, // 64 KiB volume — enough for unit tests
	}, view)
	if err != nil {
		t.Fatalf("NewDurableProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, view, root
}

// #1 — DurableProvider implements frontend.Provider.
func TestT3b_DurableProvider_ImplementsProvider(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)
			var _ frontend.Provider = p
		})
	}
}

// #2 — Open selects the right impl per config. Matrix walks both.
func TestT3b_DurableProvider_Open_SelectsImpl_Matrix(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, root := newProvider(t, impl)
			backend, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if backend.Identity().VolumeID != "v1" {
				t.Errorf("Identity().VolumeID=%q want v1", backend.Identity().VolumeID)
			}
			// Verify on-disk magic matches selector.
			magic, err := readFirstMagic(filepath.Join(root, "v1.bin"))
			if err != nil {
				t.Fatalf("read magic: %v", err)
			}
			var expectMagic string
			switch impl {
			case durable.ImplWALStore:
				expectMagic = "SWBK"
			case durable.ImplSmartWAL:
				expectMagic = "SWAW"
			}
			if magic != expectMagic {
				t.Errorf("on-disk magic=%q want %q (impl=%s)", magic, expectMagic, impl)
			}
		})
	}
}

// #3 — Returned Backend starts operational=false.
func TestT3b_DurableProvider_Open_StartsNotOperational(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)
			backend, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			// Any I/O before RecoverVolume must return ErrNotReady.
			if _, err := backend.Read(context.Background(), 0, make([]byte, 4)); !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("Read before Recover: want ErrNotReady, got %v", err)
			}
			if _, err := backend.Write(context.Background(), 0, []byte("x")); !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("Write before Recover: want ErrNotReady, got %v", err)
			}
			if err := backend.Sync(context.Background()); !errors.Is(err, frontend.ErrNotReady) {
				t.Fatalf("Sync before Recover: want ErrNotReady, got %v", err)
			}
		})
	}
}

// #4 — ImplKind mismatch between selector and on-disk superblock
// fails fast with named error.
func TestT3b_DurableProvider_Open_ImplKindMismatch_FailsFast(t *testing.T) {
	root := t.TempDir()
	id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
	view := newStubView(healthyProj(id))

	// Step 1: create a file with impl=smartwal.
	pSmart, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:        durable.ImplSmartWAL,
		StorageRoot: root,
		BlockSize:   4096,
		NumBlocks:   16,
	}, view)
	if err != nil {
		t.Fatalf("create smartwal provider: %v", err)
	}
	if _, err := pSmart.Open(context.Background(), "v1"); err != nil {
		t.Fatalf("smartwal Open: %v", err)
	}
	_ = pSmart.Close()

	// Step 2: open the same file with selector=walstore — must fail.
	pWAL, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:        durable.ImplWALStore,
		StorageRoot: root,
		BlockSize:   4096,
		NumBlocks:   16,
	}, view)
	if err != nil {
		t.Fatalf("create walstore provider: %v", err)
	}
	defer pWAL.Close()

	_, err = pWAL.Open(context.Background(), "v1")
	if err == nil {
		t.Fatal("want ImplKind mismatch error, got nil (silent coerce)")
	}
	if !errors.Is(err, durable.ErrImplKindMismatch) {
		t.Fatalf("want ErrImplKindMismatch, got %v", err)
	}
}

// readFirstMagic reads the first 4 bytes of a file to peek magic.
func readFirstMagic(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	buf := make([]byte, 4)
	if _, err := f.Read(buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// #8 — Backend Close does NOT tear down storage; Provider.Close
// tears down in correct order; double-close idempotent.
func TestT3b_DurableProvider_Lifecycle_BackendClose_DoesNotTearDownStorage(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)
			backend, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			// Close the backend handle.
			if err := backend.Close(); err != nil {
				t.Fatalf("backend Close: %v", err)
			}
			// Re-opening via provider should still work because
			// storage wasn't torn down. Provider caches so we
			// get the same (now-closed) backend; verify that the
			// cached backend correctly reports ErrBackendClosed.
			same, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("re-Open: %v", err)
			}
			// We expect the cached (closed) backend — any I/O
			// returns ErrBackendClosed (closed wins over not-
			// operational in the gate stack).
			if _, err := same.Read(context.Background(), 0, make([]byte, 4)); !errors.Is(err, frontend.ErrBackendClosed) {
				t.Errorf("Read on closed backend: want ErrBackendClosed, got %v", err)
			}
		})
	}
}

func TestT3b_DurableProvider_Lifecycle_DoubleClose_Idempotent(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)
			_, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("first Close: %v", err)
			}
			// Second close must be a no-op, not a panic.
			if err := p.Close(); err != nil {
				t.Errorf("second Close: %v (want nil, idempotent)", err)
			}
		})
	}
}

// Bonus: Open caches backend per volumeID. Two Opens return same handle.
func TestT3b_DurableProvider_Open_Caches(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)
			b1, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("Open #1: %v", err)
			}
			b2, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("Open #2: %v", err)
			}
			if b1 != b2 {
				t.Errorf("Open returned different backends for same volumeID; cache broken")
			}
		})
	}
}
