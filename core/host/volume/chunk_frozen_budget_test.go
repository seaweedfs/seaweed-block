package volume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"net/http"
	"testing"
	"time"
)

type frozenDetachProvider struct {
	*fakeChunkProvider
	opts  storage.FrozenDetachOptions
	calls int
	err   error
}

func (p *frozenDetachProvider) DetachVolumeFrozen(_ context.Context, _ string, opts storage.FrozenDetachOptions) (storage.ChunkCutResult, uint64, error) {
	p.opts = opts
	p.calls++
	return p.detach, p.last, p.err
}
func TestChunkFrozenDetachWireContractAndPrecedence(t *testing.T) {
	p := &frozenDetachProvider{fakeChunkProvider: &fakeChunkProvider{detach: storage.ChunkCutResult{H: 7, Head: storage.ChunkHead{Gen: 2, Epoch: 1, State: storage.ChunkHeadDetached}}, last: 7}}
	s := chunkServer(t, NewChunkControl(p, nil), true)
	at := time.Now().Add(-time.Second).UnixNano()
	body := fmt.Sprintf(`{"volumeID":"v1","frozen":true,"freezeStartedAtNs":%d,"allowOverBudget":true}`, at)
	w := serve(t, s, http.MethodPost, "/chunk/detach", body, "")
	if w.Code != 200 || p.calls != 1 || p.opts.StartedAt.UnixNano() != at || !p.opts.AllowOverBudget {
		t.Fatalf("frozen wire lost fields: %d %s %+v", w.Code, w.Body, p.opts)
	}
	for _, invalid := range []string{`{"volumeID":"v1","frozen":true}`, `{"volumeID":"v1","allowOverBudget":true}`, fmt.Sprintf(`{"volumeID":"v1","frozen":true,"freezeStartedAtNs":%d}`, time.Now().Add(time.Hour).UnixNano()), fmt.Sprintf(`{"volumeID":"v1","frozen":true,"plain":true,"freezeStartedAtNs":%d}`, at)} {
		before := p.calls
		w = serve(t, s, http.MethodPost, "/chunk/detach", invalid, "")
		if w.Code != 400 || p.calls != before {
			t.Fatalf("invalid request reached provider: %d %s", w.Code, w.Body)
		}
	}
	p.err = storage.ErrFrozenTailTooLarge
	w = serve(t, s, http.MethodPost, "/chunk/detach", body, "")
	var e chunkErrorBody
	_ = json.Unmarshal(w.Body.Bytes(), &e)
	if w.Code != 409 || e.Code != "frozen_tail_too_large" || !e.Retryable {
		t.Fatalf("wrong frozen refusal: %d %s", w.Code, w.Body)
	}
	for _, terminal := range []error{storage.ErrChunkLayerClosed, storage.ErrChunkLayerFenced, storage.ErrChunkCASOutcomeUnknown} {
		expectedStatus, expectedCode, expectedRetry := chunkErrorCode(terminal)
		status, code, retry := chunkErrorCode(errors.Join(terminal, storage.ErrFrozenTailTooLarge))
		if status != expectedStatus || code != expectedCode || retry != expectedRetry {
			t.Fatalf("frozen refusal hid terminal/unknown cause %v: %d %s %t", terminal, status, code, retry)
		}
	}
}
func TestChunkCheckpointCarriesCandidateTail(t *testing.T) {
	tail := &storage.ChunkCandidateTail{Bytes: 4096, Estimate: time.Second, Known: true, RateSamples: 3, BytesPerSecond: 4096}
	p := &fakeChunkProvider{cut: storage.ChunkCutResult{CandidateTail: tail}}
	s := chunkServer(t, NewChunkControl(p, nil), true)
	w := serve(t, s, http.MethodPost, "/chunk/cut", `{"volumeID":"v1"}`, "")
	var body ChunkCutResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != 200 || body.CandidateTail == nil || *body.CandidateTail != *tail {
		t.Fatalf("cut response lost tail: %d %s %v", w.Code, w.Body, err)
	}
}

func TestChunkFrozenValidationHasDistinctPreMutationCode(t *testing.T) {
	p := &frozenDetachProvider{fakeChunkProvider: &fakeChunkProvider{}}
	s := chunkServer(t, NewChunkControl(p, nil), true)
	past := time.Now().Add(-time.Second).UnixNano()
	for _, body := range []string{
		`{"volumeID":"v1","frozen":true}`,
		fmt.Sprintf(`{"volumeID":"v1","frozen":true,"freezeStartedAtNs":%d}`, time.Now().Add(time.Hour).UnixNano()),
		fmt.Sprintf(`{"volumeID":"v1","frozen":true,"plain":true,"freezeStartedAtNs":%d}`, past),
		`{"volumeID":"v1","allowOverBudget":true}`,
	} {
		w := serve(t, s, http.MethodPost, "/chunk/detach", body, "")
		var got chunkErrorBody
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if w.Code != 400 || got.Code != "invalid_freeze_request" || got.Retryable || p.calls != 0 {
			t.Errorf("freeze validation lacked pre-mutation classification: status=%d body=%s calls=%d", w.Code, w.Body, p.calls)
		}
	}
	// A malformed/generic400 is not evidence of a typed clean refusal.
	w := serve(t, s, http.MethodPost, "/chunk/detach", `{"volumeID":`, "")
	var got chunkErrorBody
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != 400 || got.Code != "bad_request" || p.calls != 0 {
		t.Errorf("generic400 relabelled: %d %s calls=%d", w.Code, w.Body, p.calls)
	}
	status, code, retry := chunkErrorCode(storage.ErrInvalidFreezeBudget)
	if status != 400 || code != "invalid_freeze_request" || retry {
		t.Errorf("storage validation lost classification: %d %s %t", status, code, retry)
	}
	for _, terminal := range []error{storage.ErrChunkCASOutcomeUnknown, storage.ErrChunkLayerClosed, storage.ErrChunkLayerFenced} {
		wantStatus, wantCode, wantRetry := chunkErrorCode(terminal)
		status, code, retry = chunkErrorCode(errors.Join(terminal, storage.ErrInvalidFreezeBudget))
		if status != wantStatus || code != wantCode || retry != wantRetry {
			t.Errorf("validation hid terminal/unknown cause: %d %s %t", status, code, retry)
		}
	}
}
