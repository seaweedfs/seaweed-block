package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// refusedDetachProvider counts every detach entry point of the provider, so a
// refused request can be shown to reach none of them.
type refusedDetachProvider struct {
	*frozenDetachProvider
	detachCalls int
	plainCalls  int
}

func (p *refusedDetachProvider) DetachVolume(ctx context.Context, id string) (storage.ChunkCutResult, uint64, error) {
	p.detachCalls++
	return p.fakeChunkProvider.DetachVolume(ctx, id)
}

func (p *refusedDetachProvider) DetachVolumePlain(ctx context.Context, id string) (storage.ChunkCutResult, uint64, error) {
	p.plainCalls++
	return p.fakeChunkProvider.DetachVolume(ctx, id)
}

// A well-formed frozen detach is refused with a typed, non-retryable 409
// before the provider is called, with or without allowOverBudget.
func TestChunkDetachFrozenRefusedBeforeProvider(t *testing.T) {
	p := &refusedDetachProvider{frozenDetachProvider: &frozenDetachProvider{fakeChunkProvider: &fakeChunkProvider{
		detach: storage.ChunkCutResult{H: 7, Head: storage.ChunkHead{Gen: 2, Epoch: 1, State: storage.ChunkHeadDetached}}, last: 7}}}
	s := chunkServer(t, NewChunkControl(p, nil), true)
	at := time.Now().Add(-time.Second).UnixNano()
	for _, body := range []string{
		fmt.Sprintf(`{"volumeID":"v1","frozen":true,"freezeStartedAtNs":%d}`, at),
		fmt.Sprintf(`{"volumeID":"v1","frozen":true,"freezeStartedAtNs":%d,"allowOverBudget":true}`, at),
	} {
		w := serve(t, s, http.MethodPost, "/chunk/detach", body, "")
		var got map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("%s: body %s: %v", body, w.Body, err)
		}
		var code, message string
		var retryable bool
		_ = json.Unmarshal(got["code"], &code)
		_ = json.Unmarshal(got["message"], &message)
		_ = json.Unmarshal(got["retryable"], &retryable)
		if w.Code != http.StatusConflict || code != "frozen_detach_unsupported" || retryable || message == "" {
			t.Errorf("%s: got %d %s, want 409 frozen_detach_unsupported retryable=false", body, w.Code, w.Body)
		}
		if _, ok := got["frozenEstimateNs"]; ok {
			t.Errorf("%s: refusal carries a freeze budget: %s", body, w.Body)
		}
		if p.calls != 0 || p.detachCalls != 0 || p.plainCalls != 0 {
			t.Fatalf("%s: refused request reached the provider: frozen=%d detach=%d plain=%d", body, p.calls, p.detachCalls, p.plainCalls)
		}
	}
	// The ordinary detach on the same server is unaffected.
	w := serve(t, s, http.MethodPost, "/chunk/detach", `{"volumeID":"v1"}`, "")
	if w.Code != http.StatusOK || p.detachCalls != 1 || p.calls != 0 {
		t.Fatalf("ordinary detach after refusals: %d %s detach=%d frozen=%d", w.Code, w.Body, p.detachCalls, p.calls)
	}
}
