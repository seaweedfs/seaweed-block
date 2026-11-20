package master

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWorkspaceHEADFormat3DetachedProof(t *testing.T) {
	for _, tc := range []struct {
		name, state     string
		format, levels  int
		valid, detached bool
	}{
		{"detached", `,"state":"detached","detachedBy":{"holder":"node-a/r1","epoch":3,"gen":9}`, 3, 1, true, true},
		{"active", `,"state":"active"`, 3, 1, true, false},
		{"missing-proof", `,"state":"detached"`, 3, 1, false, false},
		{"wrong-proof", `,"state":"detached","detachedBy":{"holder":"node-b/r1","epoch":3,"gen":9}`, 3, 1, false, false},
		{"stale-active-proof", `,"state":"active","detachedBy":{"holder":"node-a/r1","epoch":3,"gen":9}`, 3, 1, false, false},
		{"unknown-format", `,"state":"active"`, 4, 1, false, false},
		{"unknown-levels", `,"state":"active"`, 3, 2, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A data-bearing F4 root: the control-plane reader validates the
			// root and detach proof without fetching immutable segment payloads.
			body := fmt.Sprintf(`{"format":%d,"levels":%d,"gen":9,"epoch":3,"holder":"node-a/r1","remote_lsn":42,"chunk_bytes":4096,"chunk_count":1,"segments":{"0":{"key":"segment-1","sha256":[],"owner":"vol-a","count":1,"seg_format":2,"max_birth_lsn":42}}%s}`, tc.format, tc.levels, tc.state)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/vol-a/HEAD" {
					t.Errorf("unexpected path %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				_, _ = w.Write([]byte(body))
			}))
			defer s.Close()
			read, err := NewWorkspaceHEADReader(s.URL)
			if err != nil {
				t.Fatal(err)
			}
			h, err := read(context.Background(), "vol-a")
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, HEAD=%+v, error=%v", tc.valid, h, err)
			}
			if err == nil && (h.Gen != 9 || h.Epoch != 3 || h.Holder != "node-a/r1" || h.RemoteLSN != 42 || h.Detached != tc.detached) {
				t.Fatalf("incorrect HEAD evidence: %+v", h)
			}
		})
	}
}
