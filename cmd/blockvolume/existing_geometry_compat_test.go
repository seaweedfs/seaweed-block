package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestExistingDurableVolumeIgnoresCreateGeometry(t *testing.T) {
	root := t.TempDir()
	s, err := storage.CreateWALStore(filepath.Join(root, "v1.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{0x73}, 4096)
	if _, err = s.Write(0, want); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	args := append(phase150RequiredBlockvolumeArgs(), "--volume-id=v1", "--durable-root="+root, "--durable-impl=walstore", "--durable-blocks=4294967295", "--durable-blocksize=8192")
	f, err := parseFlags(args)
	if err != nil {
		t.Fatalf("existing storage must ignore create-only geometry: %v", err)
	}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplName(f.durableImpl), StorageRoot: f.durableRoot, NumBlocks: uint32(f.durableBlocks), BlockSize: int(f.durableBlockSize)}, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	result, _, err := openAndRecoverDurable(p, &spyHost{}, "v1", 5*time.Second)
	if err != nil || result != durableStartupReady {
		t.Fatalf("existing recovery: %v %v", result, err)
	}
	st, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if st.NumBlocks() != 16 || st.BlockSize() != 4096 {
		t.Fatalf("existing geometry changed: %d x %d", st.NumBlocks(), st.BlockSize())
	}
	got := make([]byte, 4096)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Read(context.Background(), 0, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("existing bytes changed")
	}
}
