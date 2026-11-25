package volume

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type fakeChunkControl struct {
	err      error
	cut      ChunkCutResponse
	snap     chunkstore.SnapshotInfo
	list     []chunkstore.SnapshotInfo
	next     string
	state    string
	detach   ChunkDetachResponse
	usage    chunkstore.Usage
	lastOpts chunkstore.ListOptions
	lastSnap ChunkSnapshotRequest
	lastMin  uint64
	deadline time.Duration
	calls    int
}

func (f *fakeChunkControl) remember(ctx context.Context) {
	if d, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(d)
	}
}

func (f *fakeChunkControl) Cut(ctx context.Context, vol string, minLSN uint64) (ChunkCutResponse, error) {
	f.calls++
	f.remember(ctx)
	f.lastMin = minLSN
	return f.cut, f.err
}
func (f *fakeChunkControl) Snapshot(ctx context.Context, vol string, req ChunkSnapshotRequest) (chunkstore.SnapshotInfo, error) {
	f.calls++
	f.remember(ctx)
	f.lastSnap = req
	return f.snap, f.err
}
func (f *fakeChunkControl) ListSnapshots(ctx context.Context, vol string, opts chunkstore.ListOptions) ([]chunkstore.SnapshotInfo, string, error) {
	f.calls++
	f.lastOpts = opts
	return f.list, f.next, f.err
}
func (f *fakeChunkControl) DeleteSnapshot(ctx context.Context, vol, name string) (string, error) {
	f.calls++
	return f.state, f.err
}
func (f *fakeChunkControl) Usage(ctx context.Context, vol string) (chunkstore.Usage, error) {
	f.calls++
	f.remember(ctx)
	return f.usage, f.err
}

func (f *fakeChunkControl) Detach(ctx context.Context, vol string) (ChunkDetachResponse, error) {
	f.calls++
	f.remember(ctx)
	return f.detach, f.err
}

func chunkServer(t *testing.T, ctl ChunkControl, enable bool) *StatusServer {
	t.Helper()
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 5, EndpointVersion: 3}}
	s := NewStatusServer(NewAdapterProjectionView(p, "v1", "r1", nil))
	if enable {
		s.EnableChunkControlEndpoint()
	}
	if ctl != nil {
		s.SetChunkControl(ctl)
	}
	return s
}

func serve(t *testing.T, s *StatusServer, method, target, body, remote string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	if remote == "" {
		remote = "127.0.0.1:5555"
	}
	req.RemoteAddr = remote
	w := httptest.NewRecorder()
	s.chunkMux().ServeHTTP(w, req)
	return w
}

func jsonKeys(t *testing.T, body []byte) []string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("response is not a JSON object: %v: %s", err, body)
	}
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func wantKeys(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s JSON fields\n got  %v\n want %v", what, got, want)
	}
}

func TestStatusServer_ChunkRoutesOffByDefault(t *testing.T) {
	s := chunkServer(t, &fakeChunkControl{}, false)
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Post("http://"+addr+"/chunk/cut", "application/json", strings.NewReader(`{"volumeID":"v1"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("/chunk/cut without --chunk-control-endpoint: %d, want 404", resp.StatusCode)
	}
}

// The success bodies carry exactly B9's field names (spec §3.1-§3.6).
func TestStatusServer_ChunkSuccessBodiesUseSpecFields(t *testing.T) {
	ctl := &fakeChunkControl{
		cut:    ChunkCutResponse{VolumeID: "v1", CutLSN: 12, HeadGen: 3, HeadEpoch: 5, Holder: "r1"},
		snap:   chunkstore.SnapshotInfo{VolumeID: "v1", Name: "s1", HeadGen: 3, State: "active"},
		list:   []chunkstore.SnapshotInfo{{VolumeID: "v1", Name: "s1"}},
		next:   "s1",
		state:  chunkstore.SnapDeleting,
		detach: ChunkDetachResponse{VolumeID: "v1", CutLSN: 12, HeadGen: 4, HeadEpoch: 5, LastWrittenLSN: 12},
	}
	s := chunkServer(t, ctl, true)

	w := serve(t, s, http.MethodPost, "/chunk/cut", `{"volumeID":"v1","minLSN":7,"timeoutMs":1000}`, "")
	if w.Code != 200 || ctl.lastMin != 7 || ctl.deadline <= 0 || ctl.deadline > time.Second {
		t.Fatalf("cut: %d %s (minLSN %d, deadline %v)", w.Code, w.Body, ctl.lastMin, ctl.deadline)
	}
	// A timeoutMs too large for a Duration is clamped to OpDeadline, not overflowed.
	if w := serve(t, s, http.MethodPost, "/chunk/cut", `{"volumeID":"v1","timeoutMs":9223372036855}`, ""); w.Code != 200 ||
		ctl.deadline < chunkstore.DefaultOpDeadline-time.Minute || ctl.deadline > chunkstore.DefaultOpDeadline {
		t.Fatalf("cut with a huge timeoutMs: %d (deadline %v), want OpDeadline", w.Code, ctl.deadline)
	}
	// The chunk layer fills the cut statistics (§3.1), so the body carries them.
	wantKeys(t, "cut", jsonKeys(t, w.Body.Bytes()), "volumeID", "cutLSN", "headGen", "headEpoch", "holder", "noop", "fullHash", "stats")

	w = serve(t, s, http.MethodPost, "/chunk/snapshot", `{"volumeID":"v1","name":"s1","requestID":"r","labels":{"agent.step":"x"},"consistency":"app","expiresAtNs":9}`, "")
	if w.Code != 200 || ctl.lastSnap.Name != "s1" || ctl.lastSnap.RequestID != "r" || ctl.lastSnap.Consistency != "app" || ctl.lastSnap.Labels["agent.step"] != "x" || ctl.lastSnap.ExpiresAtNs != 9 {
		t.Fatalf("snapshot: %d %s %+v", w.Code, w.Body, ctl.lastSnap)
	}
	wantKeys(t, "snapshot", jsonKeys(t, w.Body.Bytes()), "volumeID", "name", "headGen", "headEpoch", "cutLSN", "createdNs", "chunkCount",
		"manifestBytes", "consistency", "labels", "replayed")

	w = serve(t, s, http.MethodGet, "/chunk/snapshots?volume=v1&prefix=a&startAfter=b&limit=7&includeDeleting=true", "", "")
	if w.Code != 200 || ctl.lastOpts != (chunkstore.ListOptions{Prefix: "a", StartAfter: "b", Limit: 7, IncludeDeleting: true}) {
		t.Fatalf("snapshots: %d %s %+v", w.Code, w.Body, ctl.lastOpts)
	}
	wantKeys(t, "snapshots", jsonKeys(t, w.Body.Bytes()), "volumeID", "snapshots", "next")
	var list struct {
		Snapshots []json.RawMessage `json:"snapshots"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list.Snapshots) != 1 {
		t.Fatalf("snapshots body: %v %s", err, w.Body)
	}
	wantKeys(t, "snapshots[0]", jsonKeys(t, list.Snapshots[0]), "name", "headGen", "headEpoch", "cutLSN", "createdNs", "state",
		"chunkCount", "labels", "consistency", "parent")

	w = serve(t, s, http.MethodPost, "/chunk/snapshot/delete", `{"volumeID":"v1","name":"s1"}`, "")
	if w.Code != 200 {
		t.Fatalf("delete: %d %s", w.Code, w.Body)
	}
	wantKeys(t, "snapshot/delete", jsonKeys(t, w.Body.Bytes()), "volumeID", "name", "state")

	w = serve(t, s, http.MethodPost, "/chunk/detach", `{"volumeID":"v1","timeoutMs":120000}`, "")
	if w.Code != 200 {
		t.Fatalf("detach: %d %s", w.Code, w.Body)
	}
	wantKeys(t, "detach", jsonKeys(t, w.Body.Bytes()), "volumeID", "cutLSN", "headGen", "headEpoch", "lastWrittenLSN")
}

// Failures come back as {"code","message","retryable"} with the spec's code
// and HTTP status (spec §2.2).
func TestStatusServer_ChunkErrorBodyIsTyped(t *testing.T) {
	cases := []struct {
		err       error
		code      string
		status    int
		retryable bool
	}{
		{fmt.Errorf("x: %w", storage.ErrChunkLayerFenced), "fenced", 409, false},
		{fmt.Errorf("%w: %w", storage.ErrChunkCaptureRefused, durable.ErrChunkNotPrimary), "not_primary", 409, false},
		{fmt.Errorf("x: %w", durable.ErrChunkAwaitingAuthority), "awaiting_assignment", 409, true},
		{fmt.Errorf("x: %w", durable.ErrVolumeNotOpen), "not_chunk_volume", 409, false},
		{fmt.Errorf("%w: %w", storage.ErrChunkCASOutcomeUnknown, storage.ErrChunkRemoteUnavailable), "outcome_unknown", 503, true},
		{&chunkstore.CatalogError{Code: chunkstore.CodeSnapshotDeleting, Message: "m"}, "snapshot_deleting", 409, false},
		{&chunkstore.CatalogError{Code: chunkstore.CodeOutcomeUnknown, Message: "m", Retryable: true}, "outcome_unknown", 503, true},
		{fmt.Errorf("%w: %w", storage.ErrChunkCaptureRefused, durable.ErrChunkEpochStale), "epoch_stale", 409, false},
		{fmt.Errorf("%w: %w", storage.ErrChunkCaptureRefused, storage.ErrChunkBaseInstalling), "base_installing", 409, true},
		{fmt.Errorf("%w: %w", storage.ErrChunkCaptureRefused, storage.ErrChunkReconciling), "reconciling", 409, true},
		{fmt.Errorf("%w: %w", storage.ErrChunkReconciling, context.DeadlineExceeded), "reconciling", 409, true},
		{fmt.Errorf("x: %w", storage.ErrChunkCASOutcomeUnknown), "outcome_unknown", 503, true},
		{fmt.Errorf("x: %w", storage.ErrChunkHeadPrecondition), "head_conflict", 409, true},
		{fmt.Errorf("save: %w", storage.ErrCutCOWAborted), "cut_aborted", 503, true},
		// Local capacity abort cancels the in-flight remote PUT.
		{errors.Join(storage.ErrCutCOWAborted, storage.ErrChunkRemoteUnavailable, context.Canceled, errors.New("storage: saver capacity")), "cut_aborted", 503, true},
		{errors.Join(storage.ErrCutCOWAborted, storage.ErrChunkCASOutcomeUnknown, storage.ErrChunkRemoteUnavailable), "outcome_unknown", 503, true},
		{errors.Join(storage.ErrCutCOWAborted, storage.ErrChunkLayerFenced, storage.ErrChunkRemoteUnavailable), "fenced", 409, false},
		{fmt.Errorf("x: %w", storage.ErrChunkRemoteUnavailable), "remote_unavailable", 503, true},
		{fmt.Errorf("x: %w", storage.ErrChunkLayerClosed), "closing", 503, false},
		{errors.Join(storage.ErrCutCOWAborted, storage.ErrChunkLayerClosed, storage.ErrChunkRemoteUnavailable, context.Canceled), "closing", 503, false},
		{errors.Join(storage.ErrCutCOWAborted, storage.ErrChunkLayerDetached, storage.ErrChunkRemoteUnavailable, context.Canceled), "closing", 409, false},
		{fmt.Errorf("x: %w", storage.ErrChunkLayerDetached), "closing", 409, false},
		{fmt.Errorf("%w: %w", durable.ErrDetachAborted, storage.ErrChunkRemoteUnavailable), "detach_aborted", 409, true},
		// The detach sentinels decide even over a catalog-error cause (which
		// alone maps to its own code), and without the storage CAS sentinel.
		{fmt.Errorf("%w: %w", durable.ErrDetachAborted, &chunkstore.CatalogError{Code: chunkstore.CodeSnapshotExists, Message: "m"}), "detach_aborted", 409, true},
		{fmt.Errorf("%w: %w", durable.ErrDetachOutcomeUnknown, &chunkstore.CatalogError{Code: chunkstore.CodeSnapshotExists, Message: "m"}), "outcome_unknown", 503, true},
		{fmt.Errorf("%w: %w: cut LSN 1, HEAD remote_lsn 10", storage.ErrChunkCaptureRefused, storage.ErrChunkLSNBehind), "lsn_behind", 409, false},
		{&chunkstore.CatalogError{Code: chunkstore.CodeInvalidLabel, Message: "m"}, "invalid_label", 400, false},
		{fmt.Errorf("x: %w", durable.ErrNotChunkVolume), "not_chunk_volume", 409, false},
		{&chunkstore.CatalogError{Code: chunkstore.CodeSnapshotExists, Message: "m"}, "snapshot_exists", 409, false},
		{&chunkstore.CatalogError{Code: chunkstore.CodeSnapshotNotFound, Message: "m"}, "snapshot_not_found", 404, false},
		{&chunkstore.CatalogError{Code: chunkstore.CodeInvalidName, Message: "m"}, "invalid_name", 400, false},
		{&chunkstore.CatalogError{Code: chunkstore.CodeDeadlineExceeded, Message: "m", Retryable: true}, "deadline_exceeded", 503, true},
		{context.DeadlineExceeded, "deadline_exceeded", 503, true},
		{errors.New("something else"), "internal", 500, false},
		// A held CAS whose re-proposal was refused: the uncertainty is reported.
		{fmt.Errorf("x (%w); re-proposal refused: %w", storage.ErrChunkCASOutcomeUnknown,
			fmt.Errorf("%w: %w", storage.ErrChunkCaptureRefused, durable.ErrChunkNotPrimary)), "outcome_unknown", 503, true},
		{fmt.Errorf("x (%w); re-proposal refused: %w", storage.ErrChunkCASOutcomeUnknown,
			fmt.Errorf("%w: %w", storage.ErrChunkCaptureRefused, durable.ErrChunkEpochStale)), "outcome_unknown", 503, true},
	}
	for _, c := range cases {
		ctl := &fakeChunkControl{err: c.err}
		s := chunkServer(t, ctl, true)
		w := serve(t, s, http.MethodPost, "/chunk/cut", `{"volumeID":"v1"}`, "")
		var body struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			Retryable bool   `json:"retryable"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("%v: body is not JSON: %s", c.err, w.Body)
		}
		if w.Code != c.status || body.Code != c.code || body.Retryable != c.retryable || body.Message == "" {
			t.Errorf("%v: got %d %+v, want %d %q retryable=%v", c.err, w.Code, body, c.status, c.code, c.retryable)
		}
	}
}

func TestStatusServer_ChunkRequestGuards(t *testing.T) {
	s := chunkServer(t, &fakeChunkControl{}, true)
	code := func(w *httptest.ResponseRecorder) string {
		var b struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &b)
		return b.Code
	}
	if w := serve(t, s, http.MethodPost, "/chunk/cut", `{"volumeID":"v9"}`, ""); w.Code != 404 || code(w) != "volume_not_served" {
		t.Fatalf("other volume: %d %s", w.Code, w.Body)
	}
	// Transport errors are JSON too (ruling of 2026-11-15).
	if w := serve(t, s, http.MethodGet, "/chunk/cut", "", ""); w.Code != http.StatusMethodNotAllowed || code(w) != "method_not_allowed" {
		t.Fatalf("GET on a POST route: %d %s", w.Code, w.Body)
	}
	if w := serve(t, s, http.MethodPost, "/chunk/cut", `{"volumeID":"v1"}`, "10.0.0.9:4000"); w.Code != http.StatusForbidden || code(w) != "forbidden" {
		t.Fatalf("non-loopback client: %d %s", w.Code, w.Body)
	}
	if w := serve(t, s, http.MethodPost, "/chunk/cut", `{not json`, ""); w.Code != 400 || code(w) != "bad_request" {
		t.Fatalf("bad body: %d %s", w.Code, w.Body)
	}
	if w := serve(t, s, http.MethodGet, "/chunk/snapshots?volume=v1&limit=x", "", ""); w.Code != 400 || code(w) != "bad_request" {
		t.Fatalf("bad limit: %d %s", w.Code, w.Body)
	}
	unwired := chunkServer(t, nil, true)
	if w := serve(t, unwired, http.MethodPost, "/chunk/cut", `{"volumeID":"v1"}`, ""); w.Code != 409 || code(w) != "not_chunk_volume" {
		t.Fatalf("no chunk control wired: %d %s", w.Code, w.Body)
	}
}

// A request body is exactly one JSON object: trailing data refuses the
// request with bad_request before any operation runs.
func TestStatusServer_ChunkBodyIsOneJSONObject(t *testing.T) {
	code := func(w *httptest.ResponseRecorder) string {
		var b struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &b)
		return b.Code
	}
	for _, body := range []string{`{"volumeID":"v1"} {"volumeID":"v1"}`, `{"volumeID":"v1"}garbage`, `{"volumeID":"v1"}{}`} {
		for _, route := range []string{"/chunk/cut", "/chunk/detach"} {
			ctl := &fakeChunkControl{}
			w := serve(t, chunkServer(t, ctl, true), http.MethodPost, route, body, "")
			if w.Code != http.StatusBadRequest || code(w) != "bad_request" || ctl.calls != 0 {
				t.Fatalf("%s with body %q: %d %s, %d operations called; want bad_request and none", route, body, w.Code, w.Body, ctl.calls)
			}
		}
	}
	ctl := &fakeChunkControl{}
	if w := serve(t, chunkServer(t, ctl, true), http.MethodPost, "/chunk/cut", "{\"volumeID\":\"v1\"}\n", ""); w.Code != 200 || ctl.calls != 1 {
		t.Fatalf("one object and a newline: %d %s", w.Code, w.Body)
	}
}
