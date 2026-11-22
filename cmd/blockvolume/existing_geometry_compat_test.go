package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
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
	blocks, blockSize, err := durableCreateGeometry(f)
	if err != nil {
		t.Fatal(err)
	}
	if blocks != 0 {
		t.Fatal("existing file must select open-only mode")
	}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplName(f.durableImpl), StorageRoot: f.durableRoot, NumBlocks: blocks, BlockSize: blockSize}, healthyView{})
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

func TestExistingDurableVolumeDisappearsDoesNotCreateFromIgnoredFlags(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("64-bit CLI uint overflow case")
	}
	for _, impl := range []durable.ImplName{durable.ImplWALStore, durable.ImplWALStoreChunk} {
		t.Run(string(impl), func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "v1.bin")
			s, err := storage.CreateWALStore(path, 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			args := append(phase150RequiredBlockvolumeArgs(), "--volume-id=v1", "--durable-root="+root, "--durable-impl="+string(impl), "--durable-blocks=4294967297")
			if impl == durable.ImplWALStoreChunk {
				args = append(args, "--chunk-filer=127.0.0.1:18888")
			}
			f, err := parseFlags(args)
			if err != nil {
				t.Fatal(err)
			}
			blocks, bs, err := durableCreateGeometry(f)
			if err != nil {
				t.Fatal(err)
			}
			if blocks != 0 {
				t.Fatal("not open-only")
			}
			if err = os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if _, _, err = durableCreateGeometry(f); err == nil {
				t.Fatal("fresh recheck accepted original overflowing count")
			}
			p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: impl, ChunkHolder: "s1/r1", StorageRoot: root, NumBlocks: blocks, BlockSize: bs, ChunkRemoteFor: func(string) (storage.ChunkRemote, error) {
				return nil, errors.New("unexpected remote access for missing open-only volume")
			}}, healthyView{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.Close() })
			if _, err = p.EnsureStorage("v1"); err == nil {
				t.Fatal("created replacement from ignored flags")
			}
			if _, err = os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("replacement file exists: %v", err)
			}
			if _, err = os.Stat(path + ".chunkfresh"); !os.IsNotExist(err) {
				t.Fatalf("open-only refusal left a fresh marker: %v", err)
			}
		})
	}
}
