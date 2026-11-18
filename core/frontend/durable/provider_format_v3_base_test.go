//go:build swblock_format_v3_test

package durable

import (
	"bytes"
	"context"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
	"time"
)

func TestDurableProviderV3_AttachedChunkBASEInstallAndCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	p := mappedProviderV3(t, t.TempDir(), remote)
	st, e := p.EnsureStorage("v1")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.RecoverVolume(ctx, "v1"); e != nil {
		t.Fatal(e)
	}
	var id [16]byte
	id[0] = 1
	h, e := storage.BeginInstall(st, 0, id)
	if e != nil {
		t.Fatalf("real provider attached BASE begin refused: %v", e)
	}
	want := bytes.Repeat([]byte{0x69}, 4096)
	if e = h.WriteBase(5, want); e != nil {
		t.Fatal(e)
	}
	if _, e = p.CaptureVolume(ctx, "v1"); e == nil {
		t.Fatal("partial BASE was captured")
	}
	if e = h.MarkBaseComplete(); e != nil {
		t.Fatal(e)
	}
	if _, e = p.CaptureVolume(ctx, "v1"); e != nil {
		t.Fatalf("completed BASE capture refused: %v", e)
	}
	head, e := remote.GetHead(ctx)
	if e != nil {
		t.Fatal(e)
	}
	ref, ok := head.Chunks[1]
	if !ok {
		t.Fatal("completed BASE chunk omitted")
	}
	chunk, e := remote.Get(ctx, ref.Key)
	if e != nil || len(chunk) < 8192 || !bytes.Equal(chunk[4096:8192], want) {
		t.Fatalf("captured BASE mismatch: %v", e)
	}
}

func TestDurableProviderV3_AttachesCompletedChunkBASE(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := mappedProviderV3(t, t.TempDir(), storage.NewMemChunkRemote())
	st, e := p.EnsureStorage("v1")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = st.Recover(); e != nil {
		t.Fatal(e)
	}
	var id [16]byte
	id[0] = 2
	h, e := storage.BeginInstall(st, 0, id)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.WriteBase(5, bytes.Repeat([]byte{0x37}, 4096)); e != nil {
		t.Fatal(e)
	}
	if e = h.MarkBaseComplete(); e != nil {
		t.Fatal(e)
	}
	if _, e = p.RecoverVolume(ctx, "v1"); e != nil {
		t.Fatalf("real provider completed BASE attach refused: %v", e)
	}
	got := make([]byte, 4096)
	if n, e := p.Backend("v1").Read(ctx, 5*4096, got); e != nil || n != 4096 || !bytes.Equal(got, bytes.Repeat([]byte{0x37}, 4096)) {
		t.Fatalf("completed BASE read n=%d err=%v", n, e)
	}
}
