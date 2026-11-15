//go:build chunk_takeover_red

// RED tests for the takeover CAS at attach (W2 spec §6.3, P5). They are
// written by W2 and implemented by W4; the build tag keeps them out of the
// default suite until then. Run them with:
//
//	go test -tags chunk_takeover_red -run TestChunkAttach_TakeOver ./core/storage/
//
// The implementation should remove the build tag.

package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
)

func takeoverImage() []byte {
	img := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := uint32(0); lba < 16; lba++ { // chunks 0-3 hold data; 4-7 are ZERO
		copy(img[int(lba)*ctBlockSize:], makeBlock(ctBlockSize, byte(0x70+lba)))
	}
	return img
}

// A fresh host attaching at a newer epoch must raise HEAD's epoch before it
// serves, without changing the published chunks, so a cut-off old holder's
// next CAS fails at once instead of after the new host's first commit.
func TestChunkAttach_TakeOverRaisesEpoch(t *testing.T) {
	f := newFakeChunkRemote()
	f.seedHead(t, takeoverImage(), 100, 1)
	seeded := f.stats().head

	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	mustAttach(t, s, f, cfg)

	head := f.stats().head
	if head.Epoch != 2 || head.Holder != cfg.Holder {
		t.Fatalf("after attach HEAD is epoch %d holder %q, want the attaching epoch 2 holder %q", head.Epoch, head.Holder, cfg.Holder)
	}
	if head.RemoteLSN != seeded.RemoteLSN || len(head.Chunks) != len(seeded.Chunks) {
		t.Fatalf("takeover changed the image: remote_lsn %d chunks %d, want %d and %d", head.RemoteLSN, len(head.Chunks), seeded.RemoteLSN, len(seeded.Chunks))
	}
	for k, v := range seeded.Chunks {
		if head.Chunks[k] != v {
			t.Fatalf("takeover changed chunk %d: %+v, want %+v", k, head.Chunks[k], v)
		}
	}
	// The old holder (epoch 1) can no longer commit.
	if _, err := f.CASHead(ctTimeout(t), chunkHeadVersion{Gen: seeded.Gen, Epoch: 1}, seeded); !errors.Is(err, errChunkHeadPrecondition) {
		t.Fatalf("old holder's CAS after takeover: %v, want errChunkHeadPrecondition", err)
	}
}

// The old holder commits a newer generation between the new host's HEAD
// read and its takeover CAS. Attach must then either take over that newer
// generation (and serve exactly its image) or fail; it must never serve a
// mix of two generations.
func TestChunkAttach_TakeOverRacesOldHolder(t *testing.T) {
	f := newFakeChunkRemote()
	img := takeoverImage()
	f.seedHead(t, img, 100, 1)

	// The old holder's newer generation rewrites chunk 1 (LBA 4-7).
	newer := append([]byte(nil), img...)
	for lba := uint32(4); lba < 8; lba++ {
		copy(newer[int(lba)*ctBlockSize:], makeBlock(ctBlockSize, byte(0xE0+lba)))
	}
	injected := false
	f.beforeCAS = func(f *fakeChunkRemote) {
		injected = true
		next := copyChunkHead(f.head)
		key := "seed/newer/1"
		c := newer[1*ctChunkBytes : 2*ctChunkBytes]
		f.objs[key] = append([]byte(nil), c...)
		f.everStored[key] = true
		next.Chunks[1] = chunkRef{Key: key, SHA256: sha256.Sum256(c)}
		next.RemoteLSN = 101
		f.installLocked(next)
	}

	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	layer, err := attachChunkLayer(s, f, cfg)
	f.mu.Lock()
	ran := injected
	f.mu.Unlock()
	if !ran {
		// The race is injected before the takeover CAS; without that CAS the
		// test has not exercised the race at all, whatever attach returned.
		t.Fatalf("the old holder's competing CAS was never injected (attach err %v): attach made no takeover CAS", err)
	}
	if err != nil {
		// Failing the attach is an allowed outcome, but only as a refusal
		// to serve: nothing may have been published by the new host.
		if h := f.stats().head; h.Holder == cfg.Holder {
			t.Fatalf("attach failed (%v) but HEAD names the new holder: %+v", err, h)
		}
		return
	}
	t.Cleanup(func() { _ = layer.Close() })

	head := f.stats().head
	if head.Epoch != 2 || head.Holder != cfg.Holder {
		t.Fatalf("attach returned without taking over: HEAD epoch %d holder %q", head.Epoch, head.Holder)
	}
	want := f.imageOf(t, head)
	for lba := uint32(0); lba < ctNumBlocks; lba++ {
		got, err := s.Read(lba)
		if err != nil {
			t.Fatalf("read LBA %d: %v", lba, err)
		}
		if !bytes.Equal(got, blockOf(want, lba)) {
			t.Fatalf("LBA %d differs from the HEAD the new host took over (gen %d): a mix of generations", lba, head.Gen)
		}
	}
}
