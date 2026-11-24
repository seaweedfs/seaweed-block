package master

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
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

// The initial HEAD of a volume no holder ever claimed (epoch 0, no holder,
// active) reads as ErrWorkspaceHeadUnclaimed with its generation and epoch 0:
// still an error to every caller that needs an identity, and the delete's
// evidence of "never started". A claimed or detached HEAD is unchanged, and
// an epoch-0 HEAD with a holder or a detach proof stays invalid.
func TestWorkspaceHEADUnclaimed(t *testing.T) {
	for _, tc := range []struct {
		name, doc string
		unclaimed bool
	}{
		{"initial", `{"format":1,"gen":1,"epoch":0,"holder":"","remote_lsn":0}`, true},
		{"initial active", `{"format":1,"gen":1,"epoch":0,"holder":"","remote_lsn":0,"state":"active"}`, true},
		{"epoch 0 with a holder", `{"format":1,"gen":1,"epoch":0,"holder":"x/r1","remote_lsn":0}`, false},
		{"epoch 0 detached", `{"format":1,"gen":1,"epoch":0,"holder":"","remote_lsn":0,"state":"detached","detachedBy":{"holder":"","epoch":0,"gen":1}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(tc.doc)) }))
			defer s.Close()
			read, err := NewWorkspaceHEADReader(s.URL)
			if err != nil {
				t.Fatal(err)
			}
			h, err := read(context.Background(), "v")
			if got := errors.Is(err, authority.ErrWorkspaceHeadUnclaimed); got != tc.unclaimed {
				t.Fatalf("unclaimed=%v (err %v); want %v", got, err, tc.unclaimed)
			}
			if tc.unclaimed && (h.Gen != 1 || h.Epoch != 0) {
				t.Fatalf("unclaimed HEAD %+v; want its generation and epoch 0", h)
			}
			if !tc.unclaimed && err == nil {
				t.Fatalf("HEAD %+v accepted; want an error", h)
			}
		})
	}
}

// A HEAD the delete has marked (deleting, purging, deleted) reads as
// ErrWorkspaceHeadDeleting with Deleting set: the delete's re-run sees it as
// past the fence; every other caller still gets an error.
func TestWorkspaceHEADDeleting(t *testing.T) {
	for _, state := range []string{"deleting", "purging", "deleted"} {
		t.Run(state, func(t *testing.T) {
			doc := `{"format":1,"gen":5,"epoch":0,"holder":"","remote_lsn":0,"state":"` + state + `"}`
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(doc)) }))
			defer s.Close()
			read, err := NewWorkspaceHEADReader(s.URL)
			if err != nil {
				t.Fatal(err)
			}
			h, err := read(context.Background(), "v")
			if !errors.Is(err, authority.ErrWorkspaceHeadDeleting) || !h.Deleting || h.Gen != 5 {
				t.Fatalf("HEAD %s: %+v, %v; want ErrWorkspaceHeadDeleting with Deleting and its generation", state, h, err)
			}
		})
	}
}
