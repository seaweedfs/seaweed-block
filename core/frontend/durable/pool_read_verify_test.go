package durable

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Every pool read verifies the content against the name, end to end: a
// volume captured to the layout pool, the pooled chunk's bytes then damaged
// in the blob store under the filer entry, and a fresh replica attached from
// HEAD. Its guest read of a block in that chunk fails with a typed error and
// never returns the stored bytes; the refusal installs nothing, so once the
// pool holds the named bytes again the same read returns HEAD's data.
//
// Two checks stand on this path: the layout reader (bytes vs the object's
// name, chunkstore.ErrRemoteDamaged) and the engine's fill (bytes vs the
// HEAD ref, storage.ErrChunkContentMismatch). Either alone refuses; the
// assertion accepts either typed error, and the unit controls in
// layout/read_test.go and storage/chunk_read_verify_test.go pin each one.
func TestPoolReadVerify_FreshReplicaNeverServesADamagedPoolChunk(t *testing.T) {
	for name, damage := range map[string]func([]byte) []byte{
		"flipped byte in the read block": func(b []byte) []byte { b[11] ^= 0xff; return b },
		"flipped byte elsewhere":         func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b },
		"truncated":                      func(b []byte) []byte { return b[:len(b)/2] },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			remote := newKZLayout(t)
			src := kzProvider(t, t.TempDir(), remote)
			if _, err := src.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := src.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte{0x6d}, 4096)
			const chunk = 5
			if _, err := src.Backend("v1").Write(ctx, chunk*16*4096, want); err != nil {
				t.Fatal(err)
			}
			if err := src.Backend("v1").Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := src.CaptureVolume(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}

			// The pooled chunk: the one blob holding a whole chunk that starts with want.
			remote.blobs.Lock()
			var id string
			for k, v := range remote.blobs.b {
				if len(v) == 16*4096 && bytes.Equal(v[:4096], want) {
					if id != "" {
						remote.blobs.Unlock()
						t.Fatal("more than one blob holds the written chunk")
					}
					id = k
				}
			}
			if id == "" {
				remote.blobs.Unlock()
				t.Fatal("the captured chunk is not in the blob store")
			}
			orig := remote.blobs.b[id]
			damaged := damage(append([]byte(nil), orig...))
			remote.blobs.b[id] = damaged
			remote.blobs.Unlock()

			p := kzProviderAt(t, t.TempDir(), remote, "r2", 4)
			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, 4096)
			rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
			n, err := p.Backend("v1").Read(rctx, chunk*16*4096, got)
			rcancel()
			if err == nil {
				t.Fatalf("a fresh replica read a damaged pool chunk without error (%d bytes, equal to HEAD %v)", n, bytes.Equal(got, want))
			}
			if !errors.Is(err, chunkstore.ErrRemoteDamaged) && !errors.Is(err, storage.ErrChunkContentMismatch) {
				t.Fatalf("read of a damaged pool chunk: %v; want ErrRemoteDamaged or ErrChunkContentMismatch", err)
			}
			if len(damaged) >= 4096 && !bytes.Equal(damaged[:4096], want) && bytes.Equal(got, damaged[:4096]) {
				t.Fatal("the refused read handed out the damaged bytes")
			}

			remote.blobs.Lock()
			remote.blobs.b[id] = orig
			remote.blobs.Unlock()
			got = make([]byte, 4096)
			if n, err := p.Backend("v1").Read(ctx, chunk*16*4096, got); err != nil || n != len(got) || !bytes.Equal(got, want) {
				t.Fatalf("read once the pool holds the named bytes: %d %v, equal %v; want HEAD's data (nothing of the refused copy kept)", n, err, bytes.Equal(got, want))
			}
		})
	}
}
