package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
)

// The engine's own check on every pool read, independent of the remote's:
// a chunk whose bytes do not hash (or size) to the HEAD ref that names it is
// refused with ErrChunkContentMismatch on a guest read miss and on a
// background hydration, and none of its blocks is served or installed. Once
// the remote serves the named bytes again, the same reads return HEAD's data:
// nothing of the refused copy was kept.

// damageSeed replaces the stored bytes of seeded chunk idx and returns a
// restore function.
func damageSeed(t *testing.T, r *fakeChunkRemote, idx int, damage func([]byte) []byte) func() {
	t.Helper()
	key := fmt.Sprintf("seed/%d", idx)
	r.mu.Lock()
	defer r.mu.Unlock()
	orig, ok := r.objs[key]
	if !ok {
		t.Fatalf("no seeded chunk %d", idx)
	}
	r.objs[key] = damage(append([]byte(nil), orig...))
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.objs[key] = orig
	}
}

var readVerifyDamages = map[string]func([]byte) []byte{
	"flipped byte in the read block": func(b []byte) []byte { b[7] ^= 0xff; return b },
	"flipped byte elsewhere":         func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b },
	"truncated":                      func(b []byte) []byte { return b[:len(b)-ctBlockSize] },
	"extended":                       func(b []byte) []byte { return append(b, 0) },
}

func TestPoolReadVerify_GuestReadMissRefusesAChunkOffItsName(t *testing.T) {
	for name, damage := range readVerifyDamages {
		t.Run(name, func(t *testing.T) {
			s, layer, remote, _ := hydrationFixture(t, 2, 64)
			restore := damageSeed(t, remote, 1, damage)
			lba := uint32(1 * ctChunkBlocks) // first block of chunk 1
			got, err := s.ReadContext(boundedCtx(t, context.Background()), lba)
			if !errors.Is(err, ErrChunkContentMismatch) {
				t.Fatalf("read of a damaged chunk: %v; want ErrChunkContentMismatch", err)
			}
			if got != nil {
				t.Fatalf("a refused read returned %d bytes", len(got))
			}
			layer.stMu.Lock()
			absent := layer.absent[1]
			installed := false
			for i := uint64(0); i < ctChunkBlocks; i++ {
				installed = installed || layer.localBlocks[uint64(lba)+i]
			}
			layer.stMu.Unlock()
			if !absent || installed {
				t.Fatalf("after the refusal: chunk absent %v, a block installed %v; want absent, none", absent, installed)
			}
			restore()
			got, err = s.ReadContext(boundedCtx(t, context.Background()), lba)
			if err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, byte(0x40+lba))) {
				t.Fatalf("read once the named bytes are back: %v; want HEAD's data", err)
			}
		})
	}
}

func TestPoolReadVerify_HydrationRefusesAChunkOffItsName(t *testing.T) {
	fastWalker(t)
	for name, damage := range readVerifyDamages {
		t.Run(name, func(t *testing.T) {
			s, layer, remote, _ := hydrationFixture(t, 2, 64)
			restore := damageSeed(t, remote, 2, damage)
			_, err := layer.HydrateAll(boundedCtx(t, bgFill()))
			if !errors.Is(err, ErrChunkContentMismatch) {
				t.Fatalf("hydration over a damaged chunk: %v; want ErrChunkContentMismatch", err)
			}
			layer.stMu.Lock()
			absent := layer.absent[2]
			layer.stMu.Unlock()
			if !absent {
				t.Fatal("hydration installed a chunk whose bytes do not match HEAD")
			}
			restore()
			if p, err := layer.HydrateAll(boundedCtx(t, bgFill())); err != nil || p.Remaining != 0 {
				t.Fatalf("hydration once the named bytes are back: %+v %v", p, err)
			}
			for b := uint32(2 * ctChunkBlocks); b < 3*ctChunkBlocks; b++ {
				got, err := s.Read(b)
				if err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, byte(0x40+b))) {
					t.Fatalf("block %d after hydration: %v; want HEAD's data", b, err)
				}
			}
		})
	}
}
