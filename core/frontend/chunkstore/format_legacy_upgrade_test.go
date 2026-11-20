package chunkstore

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Frozen output of the pre-format-3 CreateSnapshot/Fork producers at 3401cc60.
func preF4ForkFixture(t *testing.T, segmented bool) (*fakeFiler, *Catalog, *ChunkRemote) {
	t.Helper()
	suffix := "false"
	if segmented {
		suffix = "true"
	}
	data, err := os.ReadFile("testdata/pre_f4_fork_" + suffix + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var frozen struct {
		Entries map[string]*filerpb.Entry
		Objects map[string][]byte
	}
	if err := json.Unmarshal(data, &frozen); err != nil {
		t.Fatal(err)
	}
	f, b := newFakeFiler(), newFakeBlobs()
	f.entries = frozen.Entries
	c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return time.Unix(1000, 0) }
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Put(context.Background(), "legacy-data", bytes.Repeat([]byte{0x41}, 8*4096)); err != nil {
		t.Fatal(err)
	}
	for key, body := range frozen.Objects {
		if err := r.Put(context.Background(), key, body); err != nil {
			t.Fatal(err)
		}
	}
	return f, c, r
}
func assertLegacyImage(t *testing.T, r *ChunkRemote) {
	t.Helper()
	h, err := r.GetHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h.RemoteLSN != 7 || len(h.Chunks) != 1 {
		t.Fatalf("legacy image/frontier lost: %+v", h)
	}
	data, err := r.Get(context.Background(), h.Chunks[0].Key)
	if err != nil || !bytes.Equal(data, bytes.Repeat([]byte{0x41}, 8*4096)) {
		t.Fatalf("legacy bytes changed: %v", err)
	}
}
func TestFormatUpgradeLegacyIncompleteFork(t *testing.T) {
	for _, segmented := range []bool{false, true} {
		name := "flat"
		if segmented {
			name = "segmented"
		}
		t.Run(name, func(t *testing.T) {
			f, c, r := preF4ForkFixture(t, segmented)
			_, err := c.Fork(context.Background(), ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "old-fork"})
			if err != nil {
				t.Fatalf("old incomplete fork retry: %v", err)
			}
			dst, err := NewChunkRemote(f, r.b, path.Join(catBase, "v2"))
			if err != nil {
				t.Fatal(err)
			}
			assertLegacyImage(t, dst)
			e, err := f.LookupEntry(context.Background(), path.Join(catBase, "v2"), headName)
			if err != nil {
				t.Fatal(err)
			}
			var doc struct{ Format int }
			if json.Unmarshal(e.Content, &doc) != nil || doc.Format != 3 {
				t.Fatal("fork data must be format3")
			}
		})
	}
}
func TestFormatUpgradeLegacyForkIdentityStillRefusesChangedImage(t *testing.T) {
	for _, segmented := range []bool{false, true} {
		t.Run(map[bool]string{false: "flat", true: "segmented"}[segmented], func(t *testing.T) {
			f, c, _ := preF4ForkFixture(t, segmented)
			p := snapPath(t, f, "v1", "s1")
			var s snapDoc
			if err := json.Unmarshal(f.entries[p].Content, &s); err != nil {
				t.Fatal(err)
			}
			var h map[string]any
			if err := json.Unmarshal(s.Head, &h); err != nil {
				t.Fatal(err)
			}
			// Preserve generation/frontier: only immutable content identity changes.
			if segmented {
				ref := h["segments"].(map[string]any)["0"].(map[string]any)
				sum := ref["sha256"].([]any)
				sum[0] = float64((int(sum[0].(float64)) + 1) % 256)
			} else {
				ref := h["chunks"].(map[string]any)["0"].(map[string]any)
				ref["sha256"] = strings.Repeat("0", 64)
			}
			s.Head, _ = json.Marshal(h)
			f.entries[p].Content, _ = json.Marshal(s)
			if _, err := c.Fork(context.Background(), ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "old-fork"}); CodeOf(err) != CodeTargetExists {
				t.Fatalf("changed image admitted or identity not reached: %v", err)
			}
		})
	}
}
func TestFormatUpgradeLegacySnapshotAndDelete(t *testing.T) {
	t.Run("snapshot", func(t *testing.T) {
		f, c, r := preF4ForkFixture(t, false)
		h, err := r.GetHead(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.CreateSnapshot(context.Background(), "v1", SnapshotRequest{Name: "upgraded", RequestID: "new-snapshot"}, h); err != nil {
			t.Fatalf("legacy snapshot: %v", err)
		}
		e := f.entries[snapPath(t, f, "v1", "upgraded")]
		var s snapDoc
		if err := json.Unmarshal(e.Content, &s); err != nil {
			t.Fatal(err)
		}
		var doc struct{ Format int }
		json.Unmarshal(s.Head, &doc)
		if doc.Format != 3 {
			t.Fatal("snapshot contains flat data")
		}
		assertLegacyImage(t, r)
	})
	t.Run("delete", func(t *testing.T) {
		f, c, r := preF4ForkFixture(t, false)
		for p := range f.entries {
			if p != path.Join(catBase, "v1", headName) && p != path.Join(catBase, "v1", volumeName) && !bytes.Contains([]byte(p), []byte("/keys/")) {
				delete(f.entries, p)
			}
		}
		h, err := r.GetHead(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		h.Holder = ""
		h.Epoch = 0
		f.entries[path.Join(catBase, "v1", headName)] = legacyHeadFixture(t, h)
		c.retainHook = func(stage, _ string) {
			if stage == hookHeadMarked {
				assertLegacyImage(t, r)
				e := f.entries[path.Join(catBase, "v1", headName)]
				var d struct{ Format int }
				json.Unmarshal(e.Content, &d)
				if d.Format != 3 {
					t.Fatal("delete mark contains flat data")
				}
			}
		}
		res, err := c.DeleteVolume(context.Background(), "v1", nil)
		if err != nil || !res.Completed {
			t.Fatalf("legacy delete: %+v %v", res, err)
		}
	})
}

type legacyUpgradeView struct{}

func (legacyUpgradeView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 1, Healthy: true}
}
func TestFormatUpgradeLegacyProviderTakeover(t *testing.T) {
	f, _, r := preF4ForkFixture(t, false)
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8, ChunkHolder: "node-new/r1", ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return r, nil }}, legacyUpgradeView{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("legacy provider takeover: %v", err)
	}
	h, err := r.GetHead(ctx)
	if err != nil || h.Holder != "node-new/r1" || h.Epoch != 2 {
		t.Fatalf("takeover not installed: %+v %v", h, err)
	}
	e := f.entries[path.Join(catBase, "v1", headName)]
	var d struct{ Format int }
	json.Unmarshal(e.Content, &d)
	if d.Format != 3 {
		t.Fatal("takeover wrote flat data")
	}
	for i := uint32(0); i < 8; i++ {
		got, err := s.Read(i)
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0x41}, 4096)) {
			t.Fatalf("takeover bytes %d: %v", i, err)
		}
	}
	assertLegacyImage(t, r)
}
