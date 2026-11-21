package volume

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type offlineSealControl struct {
	*plainDetachControl
	got context.Context
}

func (c *offlineSealControl) DetachPlain(ctx context.Context, id string) (ChunkDetachResponse, error) {
	c.got = ctx
	return c.plainDetachControl.DetachPlain(ctx, id)
}
func TestChunkPlainDetachUsesCallerDeadline(t *testing.T) {
	for _, limited := range []bool{false, true} {
		c := &offlineSealControl{plainDetachControl: &plainDetachControl{fakeChunkControl: &fakeChunkControl{}}}
		s := chunkServer(t, c, true)
		ctx := context.Background()
		cancel := func() {}
		if limited {
			ctx, cancel = context.WithTimeout(ctx, time.Hour)
		}
		req := httptest.NewRequest(http.MethodPost, "/chunk/detach", strings.NewReader(`{"volumeID":"v1","plain":true}`)).WithContext(ctx)
		req.RemoteAddr = "127.0.0.1:5555"
		w := httptest.NewRecorder()
		s.chunkMux().ServeHTTP(w, req)
		cancel()
		if w.Code != 200 || c.got == nil {
			t.Fatalf("plain dispatch failed: %d %s", w.Code, w.Body)
		}
		got, gok := c.got.Deadline()
		want, wok := ctx.Deadline()
		if gok != wok || !got.Equal(want) {
			t.Errorf("offline HTTP inherited operation cap: got=%v known=%t want=%v known=%t", got, gok, want, wok)
		}
	}
}
