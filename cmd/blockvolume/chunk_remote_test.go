package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

func TestWALStoreChunkFlags(t *testing.T) {
	base := func(extra ...string) []string {
		return append(phase150RequiredBlockvolumeArgs(), extra...)
	}
	f, err := parseFlags(base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk",
		"--chunk-filer=127.0.0.1:18888", "--chunk-capture-interval=30s"))
	if err != nil {
		t.Fatalf("walstore-chunk with a filer: %v", err)
	}
	if f.chunk.filer != "127.0.0.1:18888" || f.chunk.path != defaultChunkPath || f.chunk.captureInterval != 30*time.Second || f.chunk.controlEndpoint {
		t.Fatalf("parsed chunk flags = %+v", f.chunk)
	}
	f, err = parseFlags(base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1",
		"--status-addr=127.0.0.1:0", "--chunk-control-endpoint"))
	if err != nil || !f.chunk.controlEndpoint {
		t.Fatalf("walstore-chunk with the control endpoint: %+v %v", f.chunk, err)
	}
	refused := map[string][]string{
		"no filer":               base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk"),
		"no durable root":        base("--durable-impl=walstore-chunk", "--chunk-filer=127.0.0.1:18888"),
		"relative path":          base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-path=swblock"),
		"root path":              base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-path=/"),
		"negative interval":      base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-capture-interval=-1s"),
		"chunk flag on walstore": base("--durable-root=/tmp/sw-block", "--durable-impl=walstore", "--chunk-filer=x:1"),
		"chunk path on walstore": base("--durable-root=/tmp/sw-block", "--durable-impl=walstore", "--chunk-path=/other"),
		"control endpoint on walstore": base("--durable-root=/tmp/sw-block", "--durable-impl=walstore", "--status-addr=127.0.0.1:0",
			"--chunk-control-endpoint"),
		"control endpoint without a status listener": base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk",
			"--chunk-filer=x:1", "--chunk-control-endpoint"),
	}
	for name, args := range refused {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("%s: parseFlags succeeded; want an error", name)
		}
	}
}

func TestChunkVolumeRootStaysInsideBase(t *testing.T) {
	root, err := chunkVolumeRoot(defaultChunkPath, "v1")
	if err != nil || root != defaultChunkPath+"/v1" {
		t.Fatalf("chunkVolumeRoot(v1) = %q, %v", root, err)
	}
	for _, id := range []string{"", ".", "..", "a/b", "../v2", `a\b`} {
		if r, err := chunkVolumeRoot(defaultChunkPath, id); err == nil {
			t.Errorf("volume id %q mapped to %q; want refusal", id, r)
		} else if !strings.Contains(err.Error(), "plain path element") {
			t.Errorf("volume id %q: unexpected error %v", id, err)
		}
	}
}

// The attach check is the VOLUME entry's attach rules (B9 W2 §4.2): the first
// attach creates VOLUME from the local geometry, a later attach with the same
// geometry passes, and another geometry is refused.
func TestChunkAttachCheckAppliesVolumeRules(t *testing.T) {
	ctx := context.Background()
	cat, err := chunkstore.NewCatalog(chunkstore.NewMemFiler(), defaultChunkPath)
	if err != nil {
		t.Fatal(err)
	}
	check := chunkAttachCheck(cat)
	g := durable.ChunkGeometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}
	if err := check(ctx, "v1", g); err != nil {
		t.Fatalf("first attach: %v", err)
	}
	v, ok, err := cat.ReadVolume(ctx, "v1")
	if err != nil || !ok || v.Origin != chunkstore.OriginCreate || v.Geometry != (chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}) {
		t.Fatalf("VOLUME after the first attach: %+v ok=%v %v", v, ok, err)
	}
	if err := check(ctx, "v1", g); err != nil {
		t.Fatalf("attach with the same geometry: %v", err)
	}
	other := g
	other.NumBlocks = 128
	if err := check(ctx, "v1", other); chunkstore.CodeOf(err) != chunkstore.CodeGeometryMismatch {
		t.Fatalf("attach with another geometry: %v, want geometry_mismatch", err)
	}
}

// HEAD names a host as <server-id>/<replica-id>: two servers serving the same
// replica id are different holders (B9 W2 appendix, 2026-11-15 14:30Z).
func TestChunkHolderIsServerAndReplica(t *testing.T) {
	f, err := parseFlags(append(phase150RequiredBlockvolumeArgs(), "--durable-root=/tmp/sw-block",
		"--durable-impl=walstore-chunk", "--chunk-filer=x:1"))
	if err != nil {
		t.Fatal(err)
	}
	if got := chunkHolder(f); got != "m02/r1" {
		t.Fatalf("chunk holder %q, want m02/r1", got)
	}
}
