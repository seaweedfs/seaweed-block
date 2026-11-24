package durable_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A walstore-chunk volume has walstore's on-disk format, and on a fresh host
// its published bytes may exist only in the remote HEAD. Auto must not open
// such a root as plain walstore (no read-miss or fencing hooks: it would
// serve the local, absent extent). It refuses, and the explicit
// walstore-chunk selector with the remote still reads the exact bytes.
func TestDurableProvider_AutoRefusesLazyChunkRoot(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	view := v1View()

	// Host A publishes a nonzero block.
	hostA := chunkProvider(t, t.TempDir(), remote, view, 0)
	sA := openRecovered(t, hostA)
	if _, err := sA.Write(33, block(0x33)); err != nil {
		t.Fatal(err)
	}
	if err := hostA.MakeVolumeRemoteDurable(context.Background(), "v1", 1); err != nil {
		t.Fatalf("MakeVolumeRemoteDurable: %v", err)
	}

	// Host B attaches lazily from HEAD and closes before LBA 33 is fetched.
	rootB := t.TempDir()
	if err := hostA.Close(); err != nil {
		t.Fatal(err)
	}
	targetView := viewAs("r1", 4)
	hostB := chunkProvider(t, rootB, remote, targetView, 0)
	openRecovered(t, hostB)
	if err := hostB.Close(); err != nil {
		t.Fatalf("close host B: %v", err)
	}

	// Restart host B with no selector: auto must refuse before any I/O.
	auto := autoProvider(t, rootB, durable.ImplAuto)
	if _, err := auto.EnsureStorage("v1"); err == nil {
		got := []byte("not read")
		if _, rerr := auto.RecoverVolume(context.Background(), "v1"); rerr == nil {
			if s, serr := auto.EnsureStorage("v1"); serr == nil {
				got, _ = s.Read(33)
			}
		}
		t.Fatalf("auto opened a chunk-managed root as plain walstore; LBA 33 reads %x... (want HEAD byte 33)", got[:4])
	} else if !errors.Is(err, durable.ErrImplKindMismatch) {
		t.Fatalf("auto on a chunk root: %v, want ErrImplKindMismatch", err)
	}
	_ = auto.Close()

	// The explicit selector with the remote reopens and reads HEAD's bytes.
	explicit := chunkProvider(t, rootB, remote, targetView, 0)
	s := openRecovered(t, explicit)
	if got, err := s.Read(33); err != nil || !bytes.Equal(got, block(0x33)) {
		t.Fatalf("explicit walstore-chunk reopen read: %v", err)
	}
}

// Any root with chunk metadata is refused under auto, not only a lazy one:
// a chunk volume created and attached on this host.
func TestDurableProvider_AutoRefusesAnyChunkRoot(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	root := t.TempDir()
	p := chunkProvider(t, root, remote, v1View(), 0)
	openRecovered(t, p)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	auto := autoProvider(t, root, durable.ImplAuto)
	if _, err := auto.EnsureStorage("v1"); !errors.Is(err, durable.ErrImplKindMismatch) {
		t.Fatalf("auto on a root with chunk metadata: %v, want ErrImplKindMismatch", err)
	}
}

// A chunk fresh marker without a volume file (a chunk create interrupted
// before the file existed) is refused rather than turned into a new plain
// walstore volume, and the marker is left in place.
func TestDurableProvider_AutoRefusesStrayChunkFreshMarker(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "v1.bin.chunkfresh")
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	auto := autoProvider(t, root, durable.ImplAuto)
	if _, err := auto.EnsureStorage("v1"); !errors.Is(err, durable.ErrImplKindMismatch) {
		t.Fatalf("auto with a stray chunk fresh marker: %v, want ErrImplKindMismatch", err)
	}
	if _, err := os.Stat(filepath.Join(root, "v1.bin")); !os.IsNotExist(err) {
		t.Fatalf("auto created a volume file beside a chunk marker: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("chunk fresh marker removed: %v", err)
	}
}
