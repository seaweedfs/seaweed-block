package ops

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// newTestServer builds a live httptest.Server wrapping the ops handlers.
// Returns the server URL and a cleanup func.
func newTestServer(t *testing.T, state *State) (string, func()) {
	t.Helper()
	h := &handlers{
		version: "0.5.0-test",
		scope:   "test scope\n",
		state:   state,
	}
	mux := http.NewServeMux()
	mux.Handle("/status", withReadOnly(http.HandlerFunc(h.status)))
	mux.Handle("/projection", withReadOnly(http.HandlerFunc(h.projection)))
	mux.Handle("/trace", withReadOnly(http.HandlerFunc(h.trace)))
	mux.Handle("/", withReadOnly(http.HandlerFunc(h.notFound)))

	srv := httptest.NewServer(mux)
	return srv.URL, srv.Close
}

func TestStatus_ReturnsVersionAndScope(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, err := http.Get(url + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var body statusBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Version != "0.5.0-test" {
		t.Fatalf("version: got %q, want 0.5.0-test", body.Version)
	}
	if body.Scope == "" {
		t.Fatal("scope: empty")
	}
}

func TestProjection_EmptyWhenNoDemo(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, err := http.Get(url + "/projection")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("projection: got %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	// Empty projection has zero values — just verify it's valid JSON
	// and does not error.
	var anyMap map[string]any
	if err := json.Unmarshal(body, &anyMap); err != nil {
		t.Fatalf("projection: not valid JSON: %v  body=%s", err, body)
	}
}

func TestTrace_EmptyArrayWhenNoDemo(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, err := http.Get(url + "/trace")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	// Must be a JSON array, not null.
	trimmed := strings.TrimSpace(string(body))
	if !strings.HasPrefix(trimmed, "[") || !strings.HasSuffix(trimmed, "]") {
		t.Fatalf("trace: not a JSON array: %q", trimmed)
	}
}

// mutationMethods are every non-read verb that must return 501.
var mutationMethods = []string{"POST", "PUT", "PATCH", "DELETE"}

func TestMutations_Return501OnEveryPath(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	paths := []string{"/status", "/projection", "/trace", "/does-not-exist"}
	for _, p := range paths {
		for _, m := range mutationMethods {
			t.Run(m+" "+p, func(t *testing.T) {
				req, err := http.NewRequest(m, url+p, strings.NewReader(`{"x":1}`))
				if err != nil {
					t.Fatal(err)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusNotImplemented {
					t.Fatalf("got %d, want 501 (Not Implemented)", resp.StatusCode)
				}
				allow := resp.Header.Get("Allow")
				if !strings.Contains(allow, "GET") {
					t.Fatalf("Allow header %q should list GET", allow)
				}
				body, _ := io.ReadAll(resp.Body)
				var decoded map[string]string
				if err := json.Unmarshal(body, &decoded); err != nil {
					t.Fatalf("body not JSON: %v  body=%s", err, body)
				}
				if !strings.Contains(decoded["error"], "not supported in Phase 05") {
					t.Fatalf("error message missing phase 05 text: %q", decoded["error"])
				}
			})
		}
	}
}

func TestUnknownPath_Returns404(t *testing.T) {
	url, cleanup := newTestServer(t, NewState())
	defer cleanup()

	resp, err := http.Get(url + "/unknown-thing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("got %d, want 404", resp.StatusCode)
	}
}

func TestState_UpdateReflectsInSnapshot(t *testing.T) {
	st := NewState()
	demo, _, tr := st.Snapshot()
	if demo != "" {
		t.Fatalf("fresh state demo=%q, want empty", demo)
	}
	if tr != nil {
		t.Fatalf("fresh state trace=%v, want nil", tr)
	}

	// Use a live adapter so Snapshot() has something to read.
	exec := &nopExecutor{}
	a := adapter.NewVolumeReplicaAdapter(exec)
	st.Update("healthy", a)

	demo, _, _ = st.Snapshot()
	if demo != "healthy" {
		t.Fatalf("after Update: demo=%q, want healthy", demo)
	}
}

// nopExecutor is the minimum executor interface for State tests.
// It does nothing — the adapter is only used for its Projection()
// and Trace() accessors, not for real transport.
type nopExecutor struct {
	onClose adapter.OnSessionClose
}

func (e *nopExecutor) SetOnSessionClose(fn adapter.OnSessionClose) { e.onClose = fn }
func (e *nopExecutor) Probe(string, string, string) adapter.ProbeResult {
	return adapter.ProbeResult{Success: false, FailReason: "nop"}
}
func (e *nopExecutor) StartCatchUp(string, uint64, uint64) error { return nil }
func (e *nopExecutor) StartRebuild(string, uint64, uint64) error { return nil }
func (e *nopExecutor) InvalidateSession(string, uint64, string)  {}
func (e *nopExecutor) PublishHealthy(string)                     {}
func (e *nopExecutor) PublishDegraded(string, string)            {}
