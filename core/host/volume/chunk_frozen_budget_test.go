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
	var refused chunkErrorBody
	_ = json.Unmarshal(w.Body.Bytes(), &refused)
	// Frozen detach is not supported in this release: a well-formed request
	// is refused before the provider is called.
	if w.Code != 409 || refused.Code != "frozen_detach_unsupported" || refused.Retryable || p.calls != 0 {
		t.Fatalf("frozen request not refused before the provider: %d %s calls=%d", w.Code, w.Body, p.calls)
	}
	for _, invalid := range []string{`{"volumeID":"v1","frozen":true}`, `{"volumeID":"v1","allowOverBudget":true}`, fmt.Sprintf(`{"volumeID":"v1","frozen":true,"freezeStartedAtNs":%d}`, time.Now().Add(time.Hour).UnixNano()), fmt.Sprintf(`{"volumeID":"v1","frozen":true,"plain":true,"freezeStartedAtNs":%d}`, at)} {
		before := p.calls
		w = serve(t, s, http.MethodPost, "/chunk/detach", invalid, "")
		if w.Code != 400 || p.calls != before {
			t.Fatalf("invalid request reached provider: %d %s", w.Code, w.Body)
		}
	}
	// The handler no longer reaches the provider's frozen path; its error
	// mapping is checked directly.
	if status, code, retry := chunkErrorCode(storage.ErrFrozenTailTooLarge); status != 409 || code != "frozen_tail_too_large" || !retry {
		t.Fatalf("wrong frozen refusal: %d %s %t", status, code, retry)
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
		if w.Code != 400 || got.Code != "invalid_freeze_budget" || got.Retryable || p.calls != 0 {
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
	if status != 400 || code != "invalid_freeze_budget" || retry {
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

// Frozen detach is not supported in this release: even when the provider
// would answer with a frozen refusal that carries a freeze budget, the
// request is refused before the provider and the response carries no budget.
func TestChunkFrozenRequestRefusalCarriesNoBudget(t *testing.T) {
	report := storage.PrecopyReport{FrozenBudget: storage.FrozenBudget{FrozenEstimate: 5570 * time.Millisecond, FrozenEstimateKnown: true, FreezeLimit: 5 * time.Second}, Estimate: 5570 * time.Millisecond, RemainingFreeze: 4 * time.Second, GuestFreezeElapsed: time.Second, Reason: "frozen_tail_too_large"}
	refusal := &storage.FrozenRefusal{Report: report}
	p := &frozenDetachProvider{fakeChunkProvider: &fakeChunkProvider{}, err: refusal}
	srv := chunkServer(t, NewChunkControl(p, nil), true)
	body := fmt.Sprintf(`{"volumeID":"v1","frozen":true,"freezeStartedAtNs":%d}`, time.Now().Add(-time.Second).UnixNano())
	w := serve(t, srv, http.MethodPost, "/chunk/detach", body, "")
	var got map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != 409 {
		t.Fatalf("refusal %d %s", w.Code, w.Body)
	}
	var code string
	_ = json.Unmarshal(got["code"], &code)
	if code != "frozen_detach_unsupported" || p.calls != 0 {
		t.Fatalf("frozen request reached the provider: %s calls=%d", w.Body, p.calls)
	}
	for _, key := range []string{"frozenEstimateNs", "frozenEstimateKnown", "freezeLimitNs", "maxRefreezeQuiesceNs", "remainingFreezeNs", "guestFreezeElapsedNs"} {
		if _, ok := got[key]; ok {
			t.Errorf("refusal carries %s: %s", key, w.Body)
		}
	}
}
