package transport

import (
	"bytes"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Replacing a replica on the product's store (storage.CreateWALStore: the
// mapped v3 store, with a chunk layer attached as the provider attaches one):
// the primary's rebuild must bring the replica to the primary's image, block
// for block, for (a) a brand-new replica added to a volume that holds data
// (a lost node replaced) and (b) a replica removed and added back (its store
// already holds an older image). A failure here says where v1 stops.

const prodBlocks, prodBlockSize, prodChunkBlocks = 64, 4096, 16

func productStore(t *testing.T, name string, layer bool) *storage.WALStore {
	t.Helper()
	s, err := storage.CreateWALStoreWithOptions(filepath.Join(t.TempDir(), name), prodBlocks, prodBlockSize, storage.WALStoreOptions{ChunkBlocks: prodChunkBlocks})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if layer {
		if _, err := storage.AttachChunkLayer(s, storage.NewMemChunkRemote(), storage.ChunkLayerConfig{ChunkBlocks: prodChunkBlocks, Epoch: 1, Holder: name, FreshFromHead: true}); err != nil {
			t.Fatalf("attach chunk layer to %s: %v", name, err)
		}
	}
	return s
}

func prodBlk(b byte) []byte { return bytes.Repeat([]byte{b}, prodBlockSize) }

func writeAndSync(t *testing.T, s *storage.WALStore, fill byte) {
	t.Helper()
	for lba := uint32(0); lba < 8; lba++ {
		if _, err := s.Write(lba, prodBlk(fill+byte(lba))); err != nil {
			t.Fatalf("write lba %d: %v", lba, err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func imageSum(t *testing.T, s *storage.WALStore) [32]byte {
	t.Helper()
	h := sha256.New()
	for lba := uint32(0); lba < prodBlocks; lba++ {
		b, err := s.Read(lba)
		if err != nil {
			t.Fatalf("read lba %d: %v", lba, err)
		}
		h.Write(b)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// rebuildInto runs the primary's rebuild of replica r into store s (the
// executor the primary uses when it escalates a replica to rebuild).
func rebuildInto(t *testing.T, primary *storage.WALStore, s *storage.WALStore, session uint64) adapter.SessionCloseResult {
	t.Helper()
	l, err := NewReplicaListener("127.0.0.1:0", s)
	if err != nil {
		t.Fatal(err)
	}
	l.Serve()
	defer l.Stop()
	_, _, pH := primary.Boundaries()
	exec := NewBlockExecutor(primary, l.Addr())
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
	if err := exec.StartRebuild("r2", session, 1, 1, pH); err != nil {
		return adapter.SessionCloseResult{Success: false, FailReason: "StartRebuild: " + err.Error()}
	}
	select {
	case r := <-closeCh:
		return r
	case <-time.After(30 * time.Second):
		t.Fatal("rebuild did not close")
		return adapter.SessionCloseResult{}
	}
}

func TestProductStoreReplicaReplacementReachesThePrimarysImage(t *testing.T) {
	for _, c := range []struct {
		name                       string
		primaryLayer, replicaLayer bool
	}{
		{"no chunk layer", false, false},
		{"primary with chunk layer", true, false},
		{"both with chunk layer", true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Run("brand-new replica", func(t *testing.T) {
				primary := productStore(t, "primary", c.primaryLayer)
				writeAndSync(t, primary, 0x30)
				replica := productStore(t, "replica", c.replicaLayer)
				r := rebuildInto(t, primary, replica, 7)
				if !r.Success {
					t.Fatalf("rebuild into a brand-new replica failed: %+v", r)
				}
				if imageSum(t, replica) != imageSum(t, primary) {
					t.Fatal("the new replica's image differs from the primary's")
				}
			})
			t.Run("removed and added back", func(t *testing.T) {
				primary := productStore(t, "primary", c.primaryLayer)
				writeAndSync(t, primary, 0x30)
				replica := productStore(t, "replica", c.replicaLayer)
				if r := rebuildInto(t, primary, replica, 7); !r.Success {
					t.Fatalf("first rebuild failed: %+v", r)
				}
				// The replica is removed; the primary goes on writing.
				writeAndSync(t, primary, 0x50)
				// Added back: its store holds the older image.
				r := rebuildInto(t, primary, replica, 8)
				if !r.Success {
					t.Fatalf("rebuild of the re-added replica failed: %+v", r)
				}
				if imageSum(t, replica) != imageSum(t, primary) {
					t.Fatal("the re-added replica's image differs from the primary's")
				}
			})
		})
	}
}
