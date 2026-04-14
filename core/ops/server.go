package ops

import (
	"encoding/json"
	"net/http"
	"time"
)

// NewServer builds an *http.Server with read-only handlers wired in.
// The caller runs it with ListenAndServe() and shuts it down with
// Shutdown(). Every mutation verb (POST, PUT, PATCH, DELETE) on any
// ops path returns 501 with the Phase 05 "not supported" body — the
// boundary is enforced at the HTTP layer, not by omission.
//
// Arguments:
//
//	addr    Listen address (e.g. ":9090" or "127.0.0.1:0")
//	version Version string surfaced by /status
//	scope   Authoritative scope statement surfaced by /status
//	state   Shared *State written by the caller, read by handlers
func NewServer(addr, version, scope string, state *State) *http.Server {
	h := &handlers{
		version: version,
		scope:   scope,
		state:   state,
		started: time.Now(),
	}
	mux := http.NewServeMux()
	mux.Handle("/status", withReadOnly(http.HandlerFunc(h.status)))
	mux.Handle("/projection", withReadOnly(http.HandlerFunc(h.projection)))
	mux.Handle("/trace", withReadOnly(http.HandlerFunc(h.trace)))
	// Catch-all for unknown paths — still enforce read-only and give
	// an honest 404 (not 501) so mistyped paths are distinguishable
	// from attempted mutations.
	mux.Handle("/", withReadOnly(http.HandlerFunc(h.notFound)))

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// withReadOnly rejects all non-GET/HEAD/OPTIONS requests with 501
// and a structured "not supported in Phase 05" body. This is the
// enforcement point for the ops-surface mutation boundary.
func withReadOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Allow", "GET, HEAD, OPTIONS")
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":  "not supported in Phase 05",
				"reason": "the ops surface is read-only; no mutation verbs are wired",
				"method": r.Method,
				"path":   r.URL.Path,
			})
		}
	})
}
