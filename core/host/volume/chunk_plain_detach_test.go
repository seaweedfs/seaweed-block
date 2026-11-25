package volume

import (
 "github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"context"
	"encoding/json"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"net/http"
	"testing"
)

type plainDetachControl struct {
	*fakeChunkControl
	plainCalls int
}

func (c *plainDetachControl) DetachPlain(ctx context.Context, id string) (ChunkDetachResponse, error) {
	c.plainCalls++
	return c.fakeChunkControl.Detach(ctx, id)
}
func TestChunkDetachPlainFieldDispatch(t *testing.T) {
	c := &plainDetachControl{fakeChunkControl: &fakeChunkControl{detach: ChunkDetachResponse{VolumeID: "v1"}}}
	s := chunkServer(t, c, true)
	for _, body := range []string{`{"volumeID":"v1"}`, `{"volumeID":"v1","plain":false}`} {
		w := serve(t, s, http.MethodPost, "/chunk/detach", body, "")
		if w.Code != 200 || c.plainCalls != 0 {
			t.Fatalf("ordinary request changed: %d %s plain=%d", w.Code, w.Body, c.plainCalls)
		}
	}
	w := serve(t, s, http.MethodPost, "/chunk/detach", `{"volumeID":"v1","plain":true,"timeoutMs":60000}`, "")
	if w.Code != 200 || c.plainCalls != 1 {
		t.Fatalf("plain request not dispatched: %d %s plain=%d", w.Code, w.Body, c.plainCalls)
	}
}

type plainDetachProvider struct {
	*fakeChunkProvider
	plainCalls int
	err        error
}

func (p *plainDetachProvider) DetachVolumePlain(ctx context.Context, id string) (storage.ChunkCutResult, uint64, error) {
	p.plainCalls++
	if p.err != nil {
		return storage.ChunkCutResult{}, 0, p.err
	}
	return p.DetachVolume(ctx, id)
}
func TestChunkDetachPlainAdapterAndBusyResponse(t *testing.T) {
	p := &plainDetachProvider{fakeChunkProvider: &fakeChunkProvider{detach: storage.ChunkCutResult{H: 9, Head: storage.ChunkHead{Gen: 3, Epoch: 2, State: storage.ChunkHeadDetached}}, last: 9}}
	s := chunkServer(t, NewChunkControl(p, nil, chunkstore.VolumeRef{}), true)
	w := serve(t, s, http.MethodPost, "/chunk/detach", `{"volumeID":"v1","plain":true,"timeoutMs":60000}`, "")
	if w.Code != 200 || p.plainCalls != 1 {
		t.Fatalf("plain adapter: %d %s calls=%d", w.Code, w.Body, p.plainCalls)
	}
	p.err = errors.Join(durable.ErrPlainDetachBusy, errors.New("held I/O"))
	w = serve(t, s, http.MethodPost, "/chunk/detach", `{"volumeID":"v1","plain":true,"timeoutMs":60000}`, "")
	if w.Code != 409 {
		t.Fatalf("busy status: %d %s", w.Code, w.Body)
	}
	var body struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Code != "plain_detach_busy" || !body.Retryable {
		t.Fatalf("busy classification: %s %v", w.Body, err)
	}
}
