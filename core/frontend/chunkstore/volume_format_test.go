package chunkstore

import (
	"bytes"
	"context"
	"path"
	"strings"
	"testing"
)

// A store written by a format 1 binary (its VOLUME says format 1) is refused
// at the VOLUME read, by attach and by every catalog operation, never read
// with the new record and binding forms.
func TestVolumeFormat_OldStoreRefused(t *testing.T) {
	f, b := newFakeFiler(), newFakeBlobs()
	c := newCat(t, f, b)
	ctx := context.Background()
	if _, err := c.EnsureVolume(ctx, "v1", catGeo); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := c.ReadVolume(ctx, "v1"); err != nil || !ok || v.Format != 2 {
		t.Fatalf("new VOLUME: %+v %t %v; want format 2", v, ok, err)
	}
	p := catBase + "/v1/VOLUME"
	f.mu.Lock()
	e := f.entries[p]
	e.Content = bytes.Replace(e.Content, []byte(`"format":2`), []byte(`"format":1`), 1)
	f.mu.Unlock()
	if _, _, err := c.ReadVolume(ctx, "v1"); err == nil || !strings.Contains(err.Error(), "format 1 not supported") {
		t.Fatalf("format 1 VOLUME read: %v", err)
	}
	if _, err := c.EnsureVolume(ctx, "v1", catGeo); err == nil {
		t.Fatal("attach accepted a format 1 store")
	}
}

// A real pre-F4 store (frozen from a format 1 binary) is refused by every
// catalog operation at its VOLUME, and nothing is written to it.
func TestVolumeFormat_PreF4StoreRefused(t *testing.T) {
	for _, segmented := range []bool{false, true} {
		f, b := loadPreF4(t, segmented)
		c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		written := len(f.allWritten())
		if _, _, err := c.ReadVolume(ctx, "v1"); err == nil || !strings.Contains(err.Error(), "format 1 not supported") {
			t.Fatalf("segmented=%t: read: %v", segmented, err)
		}
		if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v3", RequestID: "r"}); err == nil {
			t.Fatalf("segmented=%t: fork of a format 1 store accepted", segmented)
		}
		if _, err := c.EnsureVolume(ctx, "v1", catGeo); err == nil {
			t.Fatalf("segmented=%t: attach to a format 1 store accepted", segmented)
		}
		if n := len(f.allWritten()) - written; n != 0 {
			t.Fatalf("segmented=%t: %d entries written to a format 1 store", segmented, n)
		}
	}
}

// The frozen pre-F4 stores without their VOLUME entries (a store from
// before VOLUME entries: HEAD present, no VOLUME) are refused by attach,
// never stamped format 2, and nothing is written.
func TestVolumeFormat_PreF4StoreWithoutVolumeRefused(t *testing.T) {
	for _, segmented := range []bool{false, true} {
		f, b := loadPreF4(t, segmented)
		for p := range f.entries {
			if strings.HasSuffix(p, "/"+volumeName) {
				delete(f.entries, p)
			}
		}
		c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase)
		if err != nil {
			t.Fatal(err)
		}
		written := len(f.allWritten())
		if _, err := c.EnsureVolume(context.Background(), "v1", catGeo); CodeOf(err) != CodeNotChunkVolume {
			t.Fatalf("segmented=%t: attach to a pre-VOLUME store: %v", segmented, err)
		}
		if n := len(f.allWritten()) - written; n != 0 {
			t.Fatalf("segmented=%t: %d entries written", segmented, n)
		}
	}
}

// A legacy store with HEAD and keys but no VOLUME is refused by every record
// operation, and nothing is written to it.
func TestVolumeFormat_VolumelessLegacyRefusedByRecordOperations(t *testing.T) {
	f, b := loadPreF4(t, false)
	for p := range f.entries {
		if strings.HasSuffix(p, "/"+volumeName) {
			delete(f.entries, p)
		}
	}
	bindPreF4Objects(t, f, b)
	c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	written := len(f.allWritten())
	for name, run := range map[string]func() error{
		"snapshot delete": func() error { _, err := c.DeleteSnapshot(ctx, "v1", "s1"); return err },
		"snapshot list":   func() error { _, _, err := c.ListSnapshots(ctx, "v1", ListOptions{}); return err },
		"abort snapshot":  func() error { _, err := c.AbortSnapshot(ctx, "v1", "s1", "1-0000000000000001"); return err },
		"snapshot replay": func() error {
			_, _, err := c.SnapshotReplay(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"})
			return err
		},
		"fork source": func() error {
			_, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v7", RequestID: "f"})
			return err
		},
		"independence": func() error { _, _, err := c.ReadIndependence(ctx, "v1"); return err },
	} {
		if err := run(); CodeOf(err) != CodeNotChunkVolume {
			t.Errorf("%s on a volume-less legacy store: %v; want not_chunk_volume", name, err)
		}
	}
	if n := len(f.allWritten()) - written; n != 0 {
		t.Fatalf("%d entries written to a volume-less legacy store", n)
	}
}

// bindPreF4Objects binds the frozen store's objects as keys (as bindPreF4
// does), so the store holds keys as well as HEAD.
func bindPreF4Objects(t *testing.T, f *fakeFiler, b *fakeBlobs) {
	t.Helper()
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Put(context.Background(), "legacy-data", bytes.Repeat([]byte{0x41}, 4096)); err != nil {
		t.Fatal(err)
	}
}
