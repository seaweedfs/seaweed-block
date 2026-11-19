package volume

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestChunkMetricsUsesTheStatusListener(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	s.SetDurableStatusSource(stubDurableStatusSource{vols: []durable.VolumeStatus{{VolumeID: "v1", Chunk: &durable.ChunkStatus{Attached: true, Stats: storage.ChunkLayerStats{DirtyChunks: 3}}}}})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + addr + "/metrics?volume=v1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("metrics: %d %s", resp.StatusCode, b)
	}
	for _, want := range []string{`swblock_chunk_dirty_chunks{volume="v1"} 3`, `swblock_chunk_cut_time_known{volume="v1"} 0`, `swblock_chunk_upload_pending{volume="v1"} 0`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing %q in %s", want, b)
		}
	}
	resp2, err := client.Get("http://" + addr + "/metrics?volume=other")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 404 {
		t.Fatalf("wrong volume: %d", resp2.StatusCode)
	}
	if strings.Contains(string(b), "swblock_chunk_cut_age_seconds{") {
		t.Fatal("unknown cut age fabricated")
	}
	s.SetDurableStatusSource(stubDurableStatusSource{vols: []durable.VolumeStatus{{VolumeID: "v1", Chunk: &durable.ChunkStatus{Attached: true, Stats: storage.ChunkLayerStats{LastCutNs: time.Now().Add(-2 * time.Second).UnixNano()}}}}})
	known, err := client.Get("http://" + addr + "/metrics?volume=v1")
	if err != nil {
		t.Fatal(err)
	}
	defer known.Body.Close()
	kb, err := io.ReadAll(known.Body)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, line := range strings.Split(string(kb), "\n") {
		if strings.HasPrefix(line, `swblock_chunk_cut_age_seconds{volume="v1"} `) {
			v, err := strconv.ParseFloat(strings.Fields(line)[1], 64)
			if err != nil || v < 0 {
				t.Fatalf("invalid cut age %q", line)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("known cut age missing: %s", kb)
	}
}
