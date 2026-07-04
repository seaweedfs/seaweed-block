// T3b Provider tests — mini plan §2 acceptance rows #1-#4, #8.
// Matrix-parameterized per Addendum A #1: every test iterates both
// walstore and smartwal impls.

package durable_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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

func TestDurableProvider_EnsureStorage_ConcurrentSameVolumeReturnsSingleHandle(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)

			const goroutines = 16
			var wg sync.WaitGroup
			wg.Add(goroutines)
			storages := make([]any, goroutines)
			errs := make([]error, goroutines)
			for i := 0; i < goroutines; i++ {
				i := i
				go func() {
					defer wg.Done()
					storages[i], errs[i] = p.EnsureStorage("v1")
				}()
			}
			wg.Wait()

			for i, err := range errs {
				if err != nil {
					t.Fatalf("EnsureStorage[%d]: %v", i, err)
				}
			}
			first := storages[0]
			if first == nil {
				t.Fatal("EnsureStorage[0] returned nil storage")
			}
			for i, got := range storages[1:] {
				if got != first {
					t.Fatalf("EnsureStorage[%d] returned different storage handle", i+1)
				}
			}
		})
	}
}

func TestDurableProvider_DurableStatuses_ReportLineageAndOperationalState(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, root := newProvider(t, impl)
			if _, err := p.Open(context.Background(), "v1"); err != nil {
				t.Fatalf("Open: %v", err)
			}

			before := p.DurableStatuses()
			if len(before) != 1 {
				t.Fatalf("status count before recover=%d want 1: %+v", len(before), before)
			}
			if before[0].VolumeID != "v1" || before[0].ReplicaID != "r1" || before[0].Epoch != 1 || before[0].EndpointVersion != 1 {
				t.Fatalf("unexpected lineage before recover: %+v", before[0])
			}
			if !before[0].Latched {
				t.Fatalf("status should report latched lineage after Open: %+v", before[0])
			}
			if before[0].Operational {
				t.Fatalf("status should not be operational before RecoverVolume: %+v", before[0])
			}
			if before[0].Impl != string(impl) {
				t.Fatalf("impl=%q want %q", before[0].Impl, impl)
			}
			if before[0].Path != filepath.Join(root, "v1.bin") {
				t.Fatalf("path=%q want %q", before[0].Path, filepath.Join(root, "v1.bin"))
			}

			if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatalf("RecoverVolume: %v", err)
			}
			after := p.DurableStatuses()
			if len(after) != 1 {
				t.Fatalf("status count after recover=%d want 1: %+v", len(after), after)
			}
			if !after[0].Operational || !after[0].Latched {
				t.Fatalf("status should report recovered durable lineage: %+v", after[0])
			}
			if after[0].Evidence == "" {
				t.Fatalf("status should include recovery evidence: %+v", after[0])
			}
			if !after[0].FrontierKnown {
				t.Fatalf("status should expose storage frontier evidence: %+v", after[0])
			}
		})
	}
}

func TestDurableProvider_DurableStatuses_ReportWriteProfile(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)
			backend, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatalf("RecoverVolume: %v", err)
			}
			sb, ok := backend.(*durable.StorageBackend)
			if !ok {
				t.Fatalf("backend type=%T, want *durable.StorageBackend", backend)
			}

			if _, err := backend.Write(context.Background(), 0, make([]byte, 4096)); err != nil {
				t.Fatalf("Write: %v", err)
			}
			sb.RecordTargetWrite(4096, time.Millisecond)
			if err := backend.Sync(context.Background()); err != nil {
				t.Fatalf("Sync: %v", err)
			}

			statuses := p.DurableStatuses()
			if len(statuses) != 1 {
				t.Fatalf("status count=%d want 1: %+v", len(statuses), statuses)
			}
			prof := statuses[0].WriteProfile
			if prof.TargetWriteOps != 1 || prof.TargetWriteBytes != 4096 {
				t.Fatalf("target write profile mismatch: %+v", prof)
			}
			if prof.TargetWriteDurationNanos == 0 {
				t.Fatalf("target duration was not recorded: %+v", prof)
			}
			if prof.BackendWriteOps != 1 || prof.BackendWriteBytes != 4096 {
				t.Fatalf("backend write profile mismatch: %+v", prof)
			}
			if prof.BackendWriteDurationNanos == 0 {
				t.Fatalf("backend write duration was not recorded: %+v", prof)
			}
			if prof.BackendStorageWriteCalls != 1 || prof.BackendStorageWriteBlocks != 1 {
				t.Fatalf("backend storage write profile mismatch: %+v", prof)
			}
			if prof.BackendStorageBatchCalls != 0 || prof.BackendStorageBatchBlocks != 0 {
				t.Fatalf("single-block write must not report batch profile: %+v", prof)
			}
			if prof.BackendSyncOps != 1 || prof.BackendSyncDurationNanos == 0 {
				t.Fatalf("backend sync profile mismatch: %+v", prof)
			}
		})
	}
}

func TestDurableProvider_DurableStatuses_ReportWriteBatchProfile(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			p, _, _ := newProvider(t, impl)
			backend, err := p.Open(context.Background(), "v1")
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatalf("RecoverVolume: %v", err)
			}

			payload := make([]byte, 8192)
			for i := range payload {
				payload[i] = byte(i)
			}
			if _, err := backend.Write(context.Background(), 0, payload); err != nil {
				t.Fatalf("Write: %v", err)
			}
			got := make([]byte, len(payload))
			if _, err := backend.Read(context.Background(), 0, got); err != nil {
				t.Fatalf("Read: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("readback mismatch at %d", firstDiffIdx(got, payload))
			}

			statuses := p.DurableStatuses()
			if len(statuses) != 1 {
				t.Fatalf("status count=%d want 1: %+v", len(statuses), statuses)
			}
			prof := statuses[0].WriteProfile
			if prof.BackendWriteOps != 1 || prof.BackendWriteBytes != uint64(len(payload)) {
				t.Fatalf("backend write profile mismatch: %+v", prof)
			}
			if prof.BackendStorageWriteCalls != 1 || prof.BackendStorageWriteBlocks != 2 {
				t.Fatalf("backend storage write profile mismatch: %+v", prof)
			}
			if prof.BackendStorageBatchCalls != 1 || prof.BackendStorageBatchBlocks != 2 {
				t.Fatalf("backend batch profile mismatch: %+v", prof)
			}
			if impl == durable.ImplWALStore {
				if prof.WALCopyOps == 0 || prof.WALCopyBytes == 0 || prof.WALCopyDurationNanos == 0 {
					t.Fatalf("WAL copy profile missing: %+v", prof)
				}
				if prof.WALEncodeOps == 0 || prof.WALEncodeBytes == 0 || prof.WALEncodeDurationNanos == 0 {
					t.Fatalf("WAL encode profile missing: %+v", prof)
				}
				if prof.WALChecksumOps == 0 || prof.WALChecksumBytes == 0 || prof.WALChecksumDurationNanos == 0 {
					t.Fatalf("WAL checksum profile missing: %+v", prof)
				}
				if prof.WALAppendOps == 0 || prof.WALAppendBytes == 0 || prof.WALAppendDurationNanos == 0 {
					t.Fatalf("WAL append profile missing: %+v", prof)
				}
				if prof.DirtyMapUpdateOps == 0 || prof.DirtyMapUpdateDurationNanos == 0 {
					t.Fatalf("dirty-map profile missing: %+v", prof)
				}
			}
		})
	}
}

func TestDurableProvider_LatchVolumeIdentity_AfterEnsureStorageWithoutFrontendHealthy(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			view := newStubView(frontend.Projection{
				VolumeID:  "v1",
				ReplicaID: "r2",
				Healthy:   false,
			})
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl:        impl,
				StorageRoot: root,
				BlockSize:   4096,
				NumBlocks:   16,
			}, view)
			if err != nil {
				t.Fatalf("NewDurableProvider: %v", err)
			}
			t.Cleanup(func() { _ = p.Close() })

			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatalf("EnsureStorage: %v", err)
			}
			if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatalf("RecoverVolume: %v", err)
			}
			before := p.DurableStatuses()
			if len(before) != 1 || before[0].Latched || !before[0].Operational {
				t.Fatalf("pre-latch status should be operational but unlatched: %+v", before)
			}

			view.set(frontend.Projection{
				VolumeID:        "v1",
				ReplicaID:       "r2",
				Epoch:           3,
				EndpointVersion: 2,
				Healthy:         false, // supporting replica remains frontend-gated
			})
			latched, err := p.LatchVolumeIdentity("v1")
			if err != nil {
				t.Fatalf("LatchVolumeIdentity: %v", err)
			}
			if !latched {
				t.Fatal("LatchVolumeIdentity should report a changed identity")
			}

			after := p.DurableStatuses()
			if len(after) != 1 {
				t.Fatalf("status count=%d want 1: %+v", len(after), after)
			}
			if !after[0].Latched || !after[0].Operational {
				t.Fatalf("post-latch status should be latched and operational: %+v", after[0])
			}
			if after[0].ReplicaID != "r2" || after[0].Epoch != 3 || after[0].EndpointVersion != 2 {
				t.Fatalf("post-latch lineage mismatch: %+v", after[0])
			}
		})
	}
}

func TestDurableProvider_LatchVolumeIdentity_AllowsSameReplicaAuthorityAdvance(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			view := newStubView(frontend.Projection{
				VolumeID:  "v1",
				ReplicaID: "r2",
				Healthy:   false,
			})
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl:        impl,
				StorageRoot: root,
				BlockSize:   4096,
				NumBlocks:   16,
			}, view)
			if err != nil {
				t.Fatalf("NewDurableProvider: %v", err)
			}
			t.Cleanup(func() { _ = p.Close() })

			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatalf("EnsureStorage: %v", err)
			}
			view.set(frontend.Projection{
				VolumeID:        "v1",
				ReplicaID:       "r2",
				Epoch:           3,
				EndpointVersion: 2,
				Healthy:         false,
			})
			latched, err := p.LatchVolumeIdentity("v1")
			if err != nil {
				t.Fatalf("LatchVolumeIdentity initial: %v", err)
			}
			if !latched {
				t.Fatal("initial latch should change identity")
			}

			view.set(frontend.Projection{
				VolumeID:        "v1",
				ReplicaID:       "r2",
				Epoch:           4,
				EndpointVersion: 3,
				Healthy:         false,
			})
			latched, err = p.LatchVolumeIdentity("v1")
			if err != nil {
				t.Fatalf("LatchVolumeIdentity advance: %v", err)
			}
			if !latched {
				t.Fatal("authority advance should report changed identity")
			}

			statuses := p.DurableStatuses()
			if len(statuses) != 1 {
				t.Fatalf("status count=%d want 1: %+v", len(statuses), statuses)
			}
			if statuses[0].Epoch != 4 || statuses[0].EndpointVersion != 3 {
				t.Fatalf("authority advance should update lineage: %+v", statuses[0])
			}
		})
	}
}

func TestDurableProvider_LatchVolumeIdentity_RejectsDifferentReplicaLineage(t *testing.T) {
	for _, impl := range implMatrix() {
		impl := impl
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			view := newStubView(frontend.Projection{
				VolumeID:  "v1",
				ReplicaID: "r2",
				Healthy:   false,
			})
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl:        impl,
				StorageRoot: root,
				BlockSize:   4096,
				NumBlocks:   16,
			}, view)
			if err != nil {
				t.Fatalf("NewDurableProvider: %v", err)
			}
			t.Cleanup(func() { _ = p.Close() })

			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatalf("EnsureStorage: %v", err)
			}
			view.set(frontend.Projection{
				VolumeID:        "v1",
				ReplicaID:       "r2",
				Epoch:           3,
				EndpointVersion: 2,
				Healthy:         false,
			})
			latched, err := p.LatchVolumeIdentity("v1")
			if err != nil {
				t.Fatalf("LatchVolumeIdentity initial: %v", err)
			}
			if !latched {
				t.Fatal("initial latch should change identity")
			}

			view.set(frontend.Projection{
				VolumeID:        "v1",
				ReplicaID:       "r3",
				Epoch:           4,
				EndpointVersion: 1,
				Healthy:         false,
			})
			latched, err = p.LatchVolumeIdentity("v1")
			if err == nil {
				t.Fatal("LatchVolumeIdentity should reject different replica lineage")
			}
			if latched {
				t.Fatal("failed latch must not report changed identity")
			}

			statuses := p.DurableStatuses()
			if len(statuses) != 1 {
				t.Fatalf("status count=%d want 1: %+v", len(statuses), statuses)
			}
			if statuses[0].ReplicaID != "r2" || statuses[0].Epoch != 3 || statuses[0].EndpointVersion != 2 {
				t.Fatalf("rejected latch must preserve original lineage: %+v", statuses[0])
			}
		})
	}
}
