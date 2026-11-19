package volume

import (
	"context"
	"io"
	"net/http"
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
	for _, want := range []string{`swblock_chunk_dirty_chunks{volume="v1"} 3`, `swblock_chunk_publish_time_known{volume="v1"} 0`, `swblock_chunk_upload_pending{volume="v1"} 0`} {
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
}
