//go:build chunkstore_integration

package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Against a real filer (CHUNKSTORE_WEED): a key binding reads back from the
// filer in the live shape; the reclaim's conditional update makes the filer
// delete the dropped needle (polled: the filer's deletion is queued); a
// second reclaim fails its condition; a chunked HEAD reads back through
// the filer's gRPC and HTTP paths byte for byte.
func TestIntegration_FilerOwnsTheNeedles(t *testing.T) {
	c := startWeed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const root = "/swblock/fs/v1"
	cl := c.client(t, AssignPathFor(path.Dir(root)))
	r, err := NewChunkRemote(cl, cl, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Put(ctx, "chunks/e1/c0/k", bytes.Repeat([]byte{7}, 4096)); err != nil {
		t.Fatal(err)
	}
	name, _ := r.ownKeyName("chunks/e1/c0/k")
	e, err := cl.LookupEntry(ctx, r.keyDir(), name)
	if err != nil {
		t.Fatal(err)
	}
	live, err := decodeBinding(e)
	if err != nil || live.done || live.size != 4096 {
		t.Fatalf("binding read back from the filer: %+v %v", live, err)
	}
	if got, err := cl.Get(ctx, live.fid); err != nil || len(got) != 4096 {
		t.Fatalf("bound needle: %d bytes, %v", len(got), err)
	}
	if err := reclaimBinding(ctx, cl, r.keyDir(), e); err != nil {
		t.Fatal(err)
	}
	if err := reclaimBinding(ctx, cl, r.keyDir(), e); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("second reclaim: %v; want a precondition failure", err)
	}
	done, err := cl.LookupEntry(ctx, r.keyDir(), name)
	if b, derr := decodeBinding(done); err != nil || derr != nil || !b.done || len(done.GetChunks()) != 0 {
		t.Fatalf("reclaimed binding: %+v %v %v", b, err, derr)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := cl.Get(ctx, live.fid)
		if errors.Is(err, ErrBlobNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the filer did not delete the dropped needle %s within 60 s: last %v", live.fid, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// A chunked HEAD: gRPC read and the filer's HTTP stream agree.
	old := recordInline
	recordInline = 0
	defer func() { recordInline = old }()
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "h1", Chunks: map[uint64]storage.ChunkRef{}}); err != nil {
		t.Fatal(err)
	}
	he, body, err := lookupRecord(ctx, cl, cl, root, headName)
	if err != nil || len(he.GetChunks()) == 0 || len(he.GetContent()) != 0 {
		t.Fatalf("chunked HEAD from the filer: chunks=%d content=%d %v", len(he.GetChunks()), len(he.GetContent()), err)
	}
	resp, err := http.Get("http://" + c.filerHTTP + root + "/" + headName)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	streamed, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Equal(streamed, body) {
		t.Fatalf("HEAD over filer HTTP: status %d, %d bytes (gRPC read %d), %v", resp.StatusCode, len(streamed), len(body), err)
	}
	if h, err := r.GetHead(ctx); err != nil || h.Holder != "h1" {
		t.Fatalf("GetHead of the chunked HEAD: %+v %v", h, err)
	}
}

// C8-1: stock volume.fsck in report mode (no delete flag) lists no needle a
// binding or a record names; an upload never bound is listed (the control).
func TestIntegration_FsckReportsOnlyTrueOrphans(t *testing.T) {
	c := startWeed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const root = "/swblock/fsck/v1"
	cl := c.client(t, AssignPathFor(path.Dir(root)))
	r, err := NewChunkRemote(cl, cl, root)
	if err != nil {
		t.Fatal(err)
	}
	var live []string
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("chunks/e1/c%d/k", i)
		if err := r.Put(ctx, key, bytes.Repeat([]byte{byte(i + 1)}, 4096)); err != nil {
			t.Fatal(err)
		}
		name, _ := r.ownKeyName(key)
		e, err := cl.LookupEntry(ctx, r.keyDir(), name)
		if err != nil {
			t.Fatal(err)
		}
		b, err := decodeBinding(e)
		if err != nil {
			t.Fatal(err)
		}
		live = append(live, b.fid)
	}
	old := recordInline
	recordInline = 0
	defer func() { recordInline = old }()
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "h1", Chunks: map[uint64]storage.ChunkRef{}}); err != nil {
		t.Fatal(err)
	}
	he, err := cl.LookupEntry(ctx, root, headName)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range he.GetChunks() {
		live = append(live, ch.GetFileId())
	}
	unbound, err := cl.Put(ctx, []byte("uploaded, never bound"))
	if err != nil {
		t.Fatal(err)
	}
	unbound, _, _ = canonicalFid(unbound)
	time.Sleep(2 * time.Second) // every needle older than the 1 s cutoff
	cmd := exec.CommandContext(ctx, c.weed, "shell", "-master="+c.master, "-filer="+c.filerHTTP)
	cmd.Dir = c.dir
	cmd.Stdin = strings.NewReader("lock\nvolume.fsck -v -cutoffTimeAgo=1s\nunlock\n")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("volume.fsck: %v\n%s", err, out)
	}
	report := string(out)
	// fsck prints an orphan as <collection>:<vid>,<key> with the cookie
	// zeroed (it reads needle keys from the volume index, not cookies).
	asFsck := func(fid string) string {
		_, id, err := canonicalFid(fid)
		if err != nil {
			t.Fatal(err)
		}
		s, _, _ := canonicalFid(fmt.Sprintf("%d,%x%08x", id.GetVolumeId(), id.GetFileKey(), 0))
		return ":" + s + "\n"
	}
	for _, fid := range live {
		if strings.Contains(report, asFsck(fid)) {
			t.Errorf("fsck lists live needle %s as an orphan", fid)
		}
	}
	if !strings.Contains(report, asFsck(unbound)) {
		t.Fatalf("fsck did not list the unbound upload %s (control):\n%s", unbound, report)
	}
	t.Logf("fsck report:\n%s", report)
}
