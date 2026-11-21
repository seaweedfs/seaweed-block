package volume

import (
	"context"
	"fmt"
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
			ctx, cancel = context.WithTimeout(ctx, 5*time.Minute)
		}
		req := httptest.NewRequest(http.MethodPost, "/chunk/detach", strings.NewReader(`{"volumeID":"v1","plain":true,"timeoutMs":3600000}`)).WithContext(ctx)
		req.RemoteAddr = "127.0.0.1:5555"
		w := httptest.NewRecorder()
		before := time.Now()
		s.chunkMux().ServeHTTP(w, req)
		after := time.Now()
		cancel()
		if w.Code != 200 || c.got == nil {
			t.Fatalf("plain dispatch failed: %d %s", w.Code, w.Body)
		}
		got, gok := c.got.Deadline()
		if want, ok := ctx.Deadline(); ok {
			if !gok || !got.Equal(want) {
				t.Errorf("offline HTTP ignored earlier caller: got=%v want=%v", got, want)
			}
		} else if !gok || got.Before(before.Add(time.Hour)) || got.After(after.Add(time.Hour)) {
			t.Errorf("offline HTTP inherited operation cap: got=%v want explicit one-hour bound", got)
		}
	}
}

func TestChunkPlainDetachRequiresExplicitBound(t *testing.T) {
	for _, suffix := range []string{"", `,"timeoutMs":0`, `,"timeoutMs":-1`} {
		c := &plainDetachControl{fakeChunkControl: &fakeChunkControl{}}
		s := chunkServer(t, c, true)
		w := serve(t, s, http.MethodPost, "/chunk/detach", fmt.Sprintf(`{"volumeID":"v1","plain":true%s}`, suffix), "")
		if w.Code != http.StatusBadRequest || c.calls != 0 || c.plainCalls != 0 {
			t.Errorf("unbounded plain request reached provider: status=%d calls=%d plain=%d", w.Code, c.calls, c.plainCalls)
		}
	}
}
