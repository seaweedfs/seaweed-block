package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

func TestWALStoreChunkFlags(t *testing.T) {
	base := func(extra ...string) []string {
		return append(phase150RequiredBlockvolumeArgs(), extra...)
	}
	f, err := parseFlags(base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk",
		"--chunk-filer=127.0.0.1:18888", "--chunk-capture-interval=5m"))
	if err != nil {
		t.Fatalf("walstore-chunk with a filer: %v", err)
	}
	if f.chunk.filer != "127.0.0.1:18888" || f.chunk.path != defaultChunkPath || f.chunk.captureInterval != 5*time.Minute || f.chunk.controlEndpoint {
		t.Fatalf("parsed chunk flags = %+v", f.chunk)
	}
	f, err = parseFlags(base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1",
		"--status-addr=127.0.0.1:0", "--chunk-control-endpoint"))
	if err != nil || !f.chunk.controlEndpoint {
		t.Fatalf("walstore-chunk with the control endpoint: %+v %v", f.chunk, err)
	}
	d, err := parseFlags(base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1"))
	if err != nil {
		t.Fatal(err)
	}
	if d.chunk.captureInterval != 0 || d.chunk.maintenanceInterval != time.Hour || d.chunk.automaticGC {
		t.Fatalf("scheduler defaults interval %s, want capture off (0), maintenance 1h and GC off", d.chunk.captureInterval)
	}
	on, err := parseFlags(base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-capture-interval=15m"))
	if err != nil || on.chunk.captureInterval != 15*time.Minute {
		t.Fatalf("opting in to a 15m periodic cut: %+v %v", on.chunk, err)
	}
	off, err := parseFlags(base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1",
		"--chunk-capture-interval=0"))
	if err != nil || off.chunk.captureInterval != 0 {
		t.Fatalf("disabling the timer: %+v %v", off.chunk, err)
	}
	if _, err := parseFlags(base("--durable-root=/tmp/sw-block", "--durable-impl=walstore")); err != nil {
		t.Fatalf("walstore with the chunk defaults untouched: %v", err)
	}
	refused := map[string][]string{
		"no filer":                base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk"),
		"no durable root":         base("--durable-impl=walstore-chunk", "--chunk-filer=127.0.0.1:18888"),
		"relative path":           base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-path=swblock"),
		"root path":               base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-path=/"),
		"below OSS capture floor": base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-capture-interval=4m59s"),
		"60 s in an OSS build":    base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-capture-interval=60s"),
		"negative maintenance":    base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-maintenance-interval=-1s"),
		"negative interval":       base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-capture-interval=-1s"),
		"chunk flag on walstore":  base("--durable-root=/tmp/sw-block", "--durable-impl=walstore", "--chunk-filer=x:1"),
		"chunk path on walstore":  base("--durable-root=/tmp/sw-block", "--durable-impl=walstore", "--chunk-path=/other"),
		"control endpoint on walstore": base("--durable-root=/tmp/sw-block", "--durable-impl=walstore", "--status-addr=127.0.0.1:0",
			"--chunk-control-endpoint"),
		"control endpoint without a status listener": base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk",
			"--chunk-filer=x:1", "--chunk-control-endpoint"),
		"zero upload concurrency": base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-upload-concurrency=0"),
		"too many uploads":        base("--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-upload-concurrency=65"),
		"interval on walstore":    base("--durable-root=/tmp/sw-block", "--durable-impl=walstore", "--chunk-capture-interval=5s"),
	}
	for name, args := range refused {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("%s: parseFlags succeeded; want an error", name)
		}
	}
}

// --chunk-detach-precopy stays defined but is refused at flag validation, so
// a process started with it fails with a clear message instead of running the
// pre-copy detach; without it the same flags parse as before.
func TestChunkDetachPrecopyFlagRefused(t *testing.T) {
	base := func(extra ...string) []string {
		return append(phase150RequiredBlockvolumeArgs(), extra...)
	}
	valid := []string{"--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1",
		"--status-addr=127.0.0.1:0", "--chunk-control-endpoint"}
	f, err := parseFlags(base(valid...))
	if err != nil || f.chunk.detachPrecopy {
		t.Fatalf("valid chunk flags without --chunk-detach-precopy: %+v %v", f.chunk, err)
	}
	const want = "--chunk-detach-precopy is not supported in this release"
	for name, args := range map[string][]string{
		"walstore-chunk":  base(append(valid, "--chunk-detach-precopy")...),
		"explicit true":   base(append(valid, "--chunk-detach-precopy=true")...),
		"without a chunk": base("--durable-root=/tmp/sw-block", "--durable-impl=walstore", "--chunk-detach-precopy"),
	} {
		if _, err := parseFlags(args); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: parseFlags error = %v, want %q", name, err, want)
		}
	}
	if _, err := parseFlags(base(append(valid, "--chunk-detach-precopy=false")...)); err != nil {
		t.Errorf("explicit false: %v", err)
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

// Before any local store is created, a geometry flag that disagrees with the
// volume record is refused with a line naming both; a matching record, a
// missing record (the attach check creates it) and an unreadable one (the
// attach check still applies) do not refuse.
func TestGeometryFlagsRefusalComparesTheRecord(t *testing.T) {
	ctx := context.Background()
	cat, err := chunkstore.NewCatalog(chunkstore.NewMemFiler(), defaultChunkPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := geometryFlagsRefusal(ctx, cat, "v1", 32768, 4096); got != "" {
		t.Fatalf("no record yet: %q", got)
	}
	if _, err := cat.EnsureVolume(ctx, "v1", chunkstore.Geometry{NumBlocks: 65536, BlockSize: 4096, ChunkBlocks: 1024}); err != nil {
		t.Fatal(err)
	}
	if got := geometryFlagsRefusal(ctx, cat, "v1", 65536, 4096); got != "" {
		t.Fatalf("matching flags refused: %q", got)
	}
	for _, c := range []struct {
		blocks uint64
		size   uint32
	}{{32768, 4096}, {65536, 512}} {
		got := geometryFlagsRefusal(ctx, cat, "v1", c.blocks, c.size)
		for _, want := range []string{"geometry_mismatch", "--durable-blocks", "65536 blocks of 4096 bytes"} {
			if !strings.Contains(got, want) {
				t.Fatalf("flags %d x %d: refusal %q lacks %q", c.blocks, c.size, got, want)
			}
		}
	}
	if got := geometryFlagsRefusal(ctx, cat, "v1", 0, 0); got != "" {
		t.Fatalf("an existing store (no create geometry) must not be refused: %q", got)
	}
	// An unreadable record: the same valid volume over a filer whose
	// lookups fail. The record is read (and fails), and nothing is refused.
	failing := &lookupFailFiler{MemFiler: chunkstore.NewMemFiler()}
	failCat, err := chunkstore.NewCatalog(failing, defaultChunkPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failCat.EnsureVolume(ctx, "v1", chunkstore.Geometry{NumBlocks: 65536, BlockSize: 4096, ChunkBlocks: 1024}); err != nil {
		t.Fatal(err)
	}
	failing.fail, failing.reads = true, 0
	if got := geometryFlagsRefusal(ctx, failCat, "v1", 32768, 4096); got != "" {
		t.Fatalf("an unreadable record must not refuse: %q", got)
	}
	if failing.reads == 0 {
		t.Fatal("the unreadable-record case never read the record")
	}
}

// lookupFailFiler is a MemFiler whose LookupEntry fails once fail is set; it
// counts the lookups.
type lookupFailFiler struct {
	*chunkstore.MemFiler
	fail  bool
	reads int
}

func (f *lookupFailFiler) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	f.reads++
	if f.fail {
		return nil, errors.New("filer unavailable")
	}
	return f.MemFiler.LookupEntry(ctx, dir, name)
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
// replica id are different holders (B9 W2 appendix).
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

// --chunk-dirty-threshold is gone: a cut stages by the
// copy-on-write budget (M5 Stage), so the flag never had an effect. A unit
// that still passes it fails at start.
func TestChunkDirtyThresholdFlagIsGone(t *testing.T) {
	args := append(phase150RequiredBlockvolumeArgs(), "--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1", "--chunk-dirty-threshold=512")
	if _, err := parseFlags(args); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("--chunk-dirty-threshold: %v; want flag provided but not defined", err)
	}
}

// --chunk-snapshot-schedule: daily:7 by default; daily:N, weekly:N and off
// are accepted; anything else is refused; setting it without walstore-chunk
// is refused like the other chunk flags.
func TestChunkSnapshotScheduleFlag(t *testing.T) {
	base := func(extra ...string) []string {
		return append(phase150RequiredBlockvolumeArgs(), extra...)
	}
	chunk := func(extra ...string) []string {
		return base(append([]string{"--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1"}, extra...)...)
	}
	d, err := parseFlags(chunk())
	if err != nil || d.chunk.snapshotSchedule != (snapshotSchedule{period: 24 * time.Hour, keep: 7}) {
		t.Fatalf("default schedule %+v %v; want daily, keep 7", d.chunk.snapshotSchedule, err)
	}
	for v, want := range map[string]snapshotSchedule{
		"daily:3":  {period: 24 * time.Hour, keep: 3},
		"weekly:4": {period: 7 * 24 * time.Hour, keep: 4},
		"off":      {},
	} {
		f, err := parseFlags(chunk("--chunk-snapshot-schedule=" + v))
		if err != nil || f.chunk.snapshotSchedule != want {
			t.Fatalf("%s: %+v %v; want %+v", v, f.chunk.snapshotSchedule, err, want)
		}
		if got := f.chunk.snapshotSchedule.String(); got != v {
			t.Fatalf("%s prints as %q", v, got)
		}
	}
	for _, v := range []string{"hourly:7", "daily", "daily:0", "daily:x", "weekly:-1", "daily:1001", ""} {
		if _, err := parseFlags(chunk("--chunk-snapshot-schedule=" + v)); err == nil {
			t.Fatalf("%q accepted", v)
		}
	}
	if _, err := parseFlags(base("--durable-root=/tmp/sw-block", "--durable-impl=walstore", "--chunk-snapshot-schedule=weekly:4")); err == nil {
		t.Fatal("--chunk-snapshot-schedule accepted without walstore-chunk")
	}
}
