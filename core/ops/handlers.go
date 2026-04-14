package ops

import (
	"encoding/json"
	"net/http"
	"time"
)

type handlers struct {
	version string
	scope   string
	state   *State
	started time.Time
}

// statusBody is the /status response shape.
// Deliberately small — anything bigger should live on /projection or
// /trace so each endpoint has one clear meaning.
type statusBody struct {
	Version     string `json:"version"`
	UptimeSecs  int64  `json:"uptime_secs"`
	Scope       string `json:"scope"`
	CurrentDemo string `json:"current_demo,omitempty"`
}

// status returns version, uptime, scope statement, and the current
// demo label (if any). It intentionally does not include projection
// or trace — callers go to the dedicated endpoints for those.
func (h *handlers) status(w http.ResponseWriter, r *http.Request) {
	demo, _, _ := h.state.Snapshot()
	writeJSON(w, http.StatusOK, statusBody{
		Version:     h.version,
		UptimeSecs:  int64(time.Since(h.started).Seconds()),
		Scope:       h.scope,
		CurrentDemo: demo,
	})
}

// projection returns the most recent adapter projection. Shape is the
// engine.ReplicaProjection struct serialized verbatim — no extra
// interpretation layer, because that would be a second authority path.
func (h *handlers) projection(w http.ResponseWriter, r *http.Request) {
	_, proj, _ := h.state.Snapshot()
	writeJSON(w, http.StatusOK, proj)
}

// trace returns the accumulated adapter trace for the current demo.
// Useful for debugging — lets a tester see the exact sequence of
// events the engine processed.
func (h *handlers) trace(w http.ResponseWriter, r *http.Request) {
	_, _, tr := h.state.Snapshot()
	// Emit an empty JSON array rather than null when nothing has run
	// yet — callers parsing the response shouldn't need a null check.
	if tr == nil {
		writeJSON(w, http.StatusOK, []struct{}{})
		return
	}
	writeJSON(w, http.StatusOK, tr)
}

func (h *handlers) notFound(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotFound, map[string]string{
		"error": "unknown ops path",
		"path":  r.URL.Path,
		"hint":  "try /status, /projection, or /trace",
	})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(body)
}
