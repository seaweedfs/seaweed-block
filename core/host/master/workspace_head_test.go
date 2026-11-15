package master

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// These are decoder controls, not the W2 Detach/filer integration proof.
func TestWorkspaceHEADCanonicalState(t *testing.T) {
	for _, tc := range []struct {
		name, suffix    string
		valid, detached bool
	}{
		{"legacy-active", ``, true, false},
		{"active", `,"state":"active"`, true, false},
		{"detached", `,"state":"detached","detachedBy":{"holder":"old","epoch":3,"gen":9}`, true, true},
		{"obsolete-attached", `,"state":"attached"`, false, false},
		{"missing-proof", `,"state":"detached"`, false, false},
		{"wrong-holder", `,"state":"detached","detachedBy":{"holder":"other","epoch":3,"gen":9}`, false, false},
		{"wrong-epoch", `,"state":"detached","detachedBy":{"holder":"old","epoch":2,"gen":9}`, false, false},
		{"wrong-gen", `,"state":"detached","detachedBy":{"holder":"old","epoch":3,"gen":8}`, false, false},
		{"active-stale-proof", `,"state":"active","detachedBy":{"holder":"old","epoch":3,"gen":9}`, false, false},
		{"legacy-boolean", `,"detached":true`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"format":1,"gen":9,"epoch":3,"holder":"old","remote_lsn":42` + tc.suffix + `}`))
			}))
			defer s.Close()
			read, err := NewWorkspaceHEADReader(s.URL)
			if err != nil {
				t.Fatal(err)
			}
			h, err := read(context.Background(), "v")
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, HEAD=%+v, error=%v", tc.valid, h, err)
			}
			if err == nil && h.Detached != tc.detached {
				t.Fatalf("detached=%v, HEAD=%+v", tc.detached, h)
			}
		})
	}
}
