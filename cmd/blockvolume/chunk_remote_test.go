package main

import (
	"context"
	"errors"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
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
// volume record is refused with a line naming both (78); a record the filer
// could not deliver is refused as retryable (75), never taken as "absent";
// a record that is there but does not decode is refused for good (78); a matching
// record, a missing record (the attach check creates it) and an existing
// store (no create geometry) are not refused.
func TestGeometryFlagsRefusalComparesTheRecord(t *testing.T) {
	ctx := context.Background()

	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	mem := chunkstore.NewMemFiler()
	blobs := &maintenanceBlobs{d: map[string][]byte{}}
	cat, err := chunkstore.NewBCatalog(mem, blobs, defaultChunkPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, code := geometryFlagsRefusal(ctx, cat, ref, 32768, 4096, 1024); got != "" || code != 0 {
		t.Fatal(got, code)
	}
	g := layout.Geometry{SizeBytes: 65536 * 4096, BlockSize: 4096, ChunkBytes: 1024 * 4096}
	if _, err := cat.CreateVolume(ctx, ref, g); err != nil {
		t.Fatal(err)
	}
	if got, code := geometryFlagsRefusal(ctx, cat, ref, 65536, 4096, 1024); got != "" || code != 0 {
		t.Fatal(got, code)
	}
	for _, g := range []struct {
		blocks      uint64
		size, chunk uint32
	}{{32768, 4096, 1024}, {65536, 512, 1024}, {65536, 4096, 512}} {
		got, code := geometryFlagsRefusal(ctx, cat, ref, g.blocks, g.size, g.chunk)
		if code != exitGeometryRefused || !strings.Contains(got, "geometry_mismatch") || !strings.Contains(got, "65536 blocks of 4096 bytes") {
			t.Fatalf("mismatch: %q %d", got, code)
		}
	}
	if got, code := geometryFlagsRefusal(ctx, cat, ref, 0, 0, 0); got != "" || code != 0 {
		t.Fatal(got, code)
	}
	failing := &lookupFailFiler{MemFiler: mem, fail: true}
	failCat, err := chunkstore.NewBCatalog(failing, blobs, defaultChunkPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, code := geometryFlagsRefusal(ctx, failCat, ref, 65536, 4096, 1024); code != exitWorkspaceRetry || !strings.Contains(got, "could not be read") || failing.reads == 0 {
		t.Fatal(got, code, failing.reads)
	}
	dir := path.Join(defaultChunkPath, ref.Dir())
	e, err := mem.LookupEntry(ctx, dir, "VOLUME")
	if err != nil {
		t.Fatal(err)
	}
	e.Content = []byte("{")
	if err := mem.UpdateEntry(ctx, dir, e, nil); err != nil {
		t.Fatal(err)
	}
	if got, code := geometryFlagsRefusal(ctx, cat, ref, 65536, 4096, 1024); code != exitGeometryRefused || !strings.Contains(got, "unreadable (corrupt:") {
		t.Fatal(got, code)
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

// Attach creates an unclaimed HEAD only in the master's identified directory.
func TestChunkAttachCheckAppliesVolumeRules(t *testing.T) {
	ctx := context.Background()
	f := &layoutAttachFiler{MemFiler: chunkstore.NewMemFiler()}
	cat, err := chunkstore.NewBCatalog(f, f, defaultChunkPath)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := layout.ParseVolumeID("00112233445566778899aabbccddeeff")
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	check := chunkAttachCheck(cat, ref)
	g := durable.ChunkGeometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}
	for i := 0; i < 2; i++ {
		if err := check(ctx, "v1", g); err != nil {
			t.Fatal(err)
		}
	}
	h, found, err := cat.ReadHead(ctx, ref)
	if err != nil || !found || h.VolumeID != id || h.Epoch != 0 || h.Holder != "" || h.Current != nil {
		t.Fatalf("unclaimed HEAD: %+v %v", h, err)
	}
	if err := check(ctx, "other", g); !errors.Is(err, layout.ErrIdentity) {
		t.Fatalf("wrong name: %v", err)
	}
	if err := chunkAttachCheck(cat, chunkstore.VolumeRef{Name: "v1"})(ctx, "v1", g); !errors.Is(err, layout.ErrIdentity) {
		t.Fatalf("missing master id: %v", err)
	}
	if found, err := f.LookupEntry(ctx, defaultChunkPath+"/v1", "VOLUME"); err == nil || found != nil {
		t.Fatalf("legacy directory created: %v", err)
	}
}

type layoutAttachFiler struct{ *chunkstore.MemFiler }

func (*layoutAttachFiler) Put(context.Context, []byte) (string, error) {
	return "", errors.New("attach must not upload data")
}
func (*layoutAttachFiler) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("attach must not read data")
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
