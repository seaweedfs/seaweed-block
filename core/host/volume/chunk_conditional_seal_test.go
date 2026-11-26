package volume

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// A delayed request from epoch 4 must leave the current epoch-5 holder
// untouched: no detach reaches storage, and readiness stays true.
func TestChunkConditionalSealRefusesAChangedHolderBeforeStop(t *testing.T) {
	c := &plainDetachControl{fakeChunkControl: &fakeChunkControl{}}
	s := chunkServer(t, c, true)
	w := serve(t, s, http.MethodPost, "/chunk/detach", `{"volumeID":"v1","plain":true,"timeoutMs":1000,"expectedHolder":{"replicaID":"r1","epoch":4}}`, "")
	var refusal struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &refusal); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusConflict || refusal.Code != "holder_changed" || refusal.Retryable {
		t.Fatalf("want non-retryable holder_changed, got %d %s", w.Code, w.Body)
	}
	if c.calls != 0 || c.plainCalls != 0 {
		t.Fatalf("stale seal reached storage: calls=%d plain=%d", c.calls, c.plainCalls)
	}
	if p := s.statusProjection(); !p.FrontendPrimaryReady || p.Epoch != 5 {
		t.Fatalf("replacement stopped serving: %+v", p)
	}
}

func TestChunkConditionalSealWithRealProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := testutil.NewLayoutFixture(t, "v1", 64, 4)
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "node-a/r1"}
	f.Configure(&cfg)
	view := NewAdapterProjectionView(stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 5, EndpointVersion: 1}}, "v1", "r1", nil)
	p, err := durable.NewDurableProvider(cfg, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err = p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	s := NewStatusServer(view)
	s.EnableChunkControlEndpoint()
	s.SetChunkControl(NewChunkControl(p, f.Catalog, f.Ref))
	s.SetDurableStatusSource(p)
	before, _, err := f.Catalog.ReadHead(ctx, f.Ref)
	if err != nil {
		t.Fatal(err)
	}
	var w *httptest.ResponseRecorder
	for _, expected := range []string{
		`{"replicaID":"r1","epoch":4}`,
		`{"replicaID":"r2","epoch":5}`,
		`{"replicaID":"r1","epoch":5,"claimID":"old-claim"}`,
	} {
		w = serve(t, s, http.MethodPost, "/chunk/detach", `{"volumeID":"v1","plain":true,"timeoutMs":1000,"expectedHolder":`+expected+`}`, "")
		if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"code":"holder_changed"`) || !strings.Contains(w.Body.String(), `"retryable":false`) {
			t.Fatalf("expected %s not refused: %d %s", expected, w.Code, w.Body)
		}
	}
	after, _, err := f.Catalog.ReadHead(ctx, f.Ref)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("refusal changed HEAD: %+v %v", after, err)
	}
	want := bytes.Repeat([]byte{0x53}, 4096)
	if _, err = b.Write(ctx, 0, want); err != nil {
		t.Fatalf("receiver stopped on stale seal: %v", err)
	}
	if err = b.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	if _, err = b.Read(ctx, 0, got); err != nil || !bytes.Equal(want, got) {
		t.Fatalf("readback: %v", err)
	}
	body := fmt.Sprintf(`{"volumeID":"v1","plain":true,"timeoutMs":1000,"expectedHolder":{"replicaID":"r1","epoch":5,"claimID":%q}}`, before.ClaimID)
	w = serve(t, s, http.MethodPost, "/chunk/detach", body, "")
	if w.Code != http.StatusOK {
		t.Fatalf("matching seal: %d %s", w.Code, w.Body)
	}
	var sealed ChunkDetachResponse
	if err = json.Unmarshal(w.Body.Bytes(), &sealed); err != nil || sealed.HeadEpoch != 5 || sealed.Holder != "node-a/r1" || sealed.CutLSN != sealed.LastWrittenLSN {
		t.Fatalf("wrong seal identity/proof: %s %v", w.Body, err)
	}
	first := sealed
	w = serve(t, s, http.MethodPost, "/chunk/detach", body, "")
	if w.Code != http.StatusOK {
		t.Fatalf("matching seal replay: %d %s", w.Code, w.Body)
	}
	if err = json.Unmarshal(w.Body.Bytes(), &sealed); err != nil || !reflect.DeepEqual(first, sealed) {
		t.Fatalf("seal replay changed: %s %v", w.Body, err)
	}
}

// Each malformed conditional form is refused before the stop boundary.
func TestChunkConditionalSealRequiresACompleteIdentity(t *testing.T) {
	for _, body := range []string{
		`{"volumeID":"v1","plain":true,"timeoutMs":1000,"expectedHolder":{}}`,
		`{"volumeID":"v1","plain":true,"timeoutMs":1000,"expectedHolder":{"replicaID":"r1"}}`,
		`{"volumeID":"v1","plain":true,"timeoutMs":1000,"expectedHolder":{"epoch":5}}`,
		`{"volumeID":"v1","timeoutMs":1000,"expectedHolder":{"replicaID":"r1","epoch":5}}`,
	} {
		c := &plainDetachControl{fakeChunkControl: &fakeChunkControl{}}
		s := chunkServer(t, c, true)
		w := serve(t, s, http.MethodPost, "/chunk/detach", body, "")
		if w.Code != http.StatusBadRequest || c.calls != 0 || c.plainCalls != 0 {
			t.Fatalf("malformed identity reached stop: %d %s calls=%d plain=%d", w.Code, w.Body, c.calls, c.plainCalls)
		}
	}
}
