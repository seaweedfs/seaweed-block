package chunkstore

import (
	"bytes"
	"context"
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
