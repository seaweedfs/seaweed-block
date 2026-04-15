package ops

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// lastTraceStep returns the Step label of the last trace entry, or
// empty if the trace is empty. Used by /diagnose to give a one-line
// "what is the engine's most recent decision" summary without
// exposing the full trace.
func lastTraceStep(tr []engine.TraceEntry) string {
	if len(tr) == 0 {
		return ""
	}
	last := tr[len(tr)-1]
	if last.Detail == "" {
		return last.Step
	}
	return last.Step + ": " + last.Detail
}

// summarizeWatchdog counts watchdog-event kinds into a small struct.
// Used by /diagnose. The full log stays at /watchdog.
func summarizeWatchdog(log []adapter.WatchdogEvent) watchdogSummary {
	var s watchdogSummary
	s.Total = len(log)
	for _, ev := range log {
		switch ev.Kind {
		case adapter.WatchdogArm:
			s.Arms++
		case adapter.WatchdogClearStart:
			s.ClearedStart++
		case adapter.WatchdogClearClose:
			s.ClearedClose++
		case adapter.WatchdogFire:
			s.Fires++
		case adapter.WatchdogFireNoop:
			s.FireNoops++
		case adapter.WatchdogSupersede:
			s.Supersedes++
		}
	}
	return s
}

type handlers struct {
	version string
	scope   string
	state   *State
	started time.Time
}

// statusBody is the /status response shape.
// Deliberately small — anything bigger should live on /projection,
// /trace, or /watchdog so each endpoint has one clear meaning.
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
	demo, _, _, _ := h.state.Snapshot()
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
	_, proj, _, _ := h.state.Snapshot()
	writeJSON(w, http.StatusOK, proj)
}

// trace returns the accumulated adapter trace for the current demo.
// Useful for debugging — lets a tester see the exact sequence of
// events the engine processed.
func (h *handlers) trace(w http.ResponseWriter, r *http.Request) {
	_, _, tr, _ := h.state.Snapshot()
	// Emit an empty JSON array rather than null when nothing has run
	// yet — callers parsing the response shouldn't need a null check.
	if tr == nil {
		writeJSON(w, http.StatusOK, []struct{}{})
		return
	}
	writeJSON(w, http.StatusOK, tr)
}

// watchdog returns the adapter's watchdog lifecycle evidence. This is
// a bounded local inspection surface over accepted lower truth; it
// does not reinterpret watchdog state into new semantic authority.
func (h *handlers) watchdog(w http.ResponseWriter, r *http.Request) {
	_, _, _, wd := h.state.Snapshot()
	if wd == nil {
		writeJSON(w, http.StatusOK, []struct{}{})
		return
	}
	writeJSON(w, http.StatusOK, wd)
}

// index returns the surface map: one endpoint describes what the
// single-node ops surface is and what each sub-endpoint owns. This
// is P11's "product surface is self-describing" commitment — an
// operator can discover the bounded capability without reading
// source or design docs.
//
// Only GET on exactly "/" returns the map; any other unknown path
// still 404s through notFound.
func (h *handlers) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		h.notFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, surfaceIndex{
		Scope: h.scope,
		Endpoints: []surfaceEndpoint{
			{Path: "/status", Owns: "version, uptime, scope, current demo label"},
			{Path: "/projection", Owns: "adapter projection — semantic truth derived from accepted lower institutions"},
			{Path: "/trace", Owns: "engine decision trace for the current demo"},
			{Path: "/watchdog", Owns: "adapter start-timeout watchdog lifecycle evidence"},
			{Path: "/diagnose", Owns: "bounded single-node diagnosis summary over projection / trace / watchdog"},
		},
		Note: "single-node read-only surface; any mutation verb returns 501. Broader control semantics belong to later phases.",
	})
}

// diagnose returns a bounded single-node diagnosis summary derived
// entirely from the adapter's projection, trace, and watchdog log.
// It is a MIRROR — no new authority. It groups the lower-institution
// evidence into a shape useful for "is this node healthy, and if
// not, what is the runtime saying" without the caller having to
// cross-reference three endpoints themselves.
//
// Honest on empty state: if no demo has run, every bounded field is
// either zero or an empty array.
func (h *handlers) diagnose(w http.ResponseWriter, r *http.Request) {
	demo, proj, tr, wd := h.state.Snapshot()

	summary := diagnoseBody{
		CurrentDemo:   demo,
		Mode:          string(proj.Mode),
		SessionPhase:  string(proj.SessionPhase),
		Decision:      string(proj.RecoveryDecision),
		Reason:        proj.Reason,
		LastTraceStep: lastTraceStep(tr),
		WatchdogSummary: summarizeWatchdog(wd),
		Note: "bounded summary; full trace at /trace, full watchdog log at /watchdog",
	}
	writeJSON(w, http.StatusOK, summary)
}

func (h *handlers) notFound(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotFound, map[string]string{
		"error": "unknown ops path",
		"path":  r.URL.Path,
		"hint":  "try / for the surface map",
	})
}

// surfaceIndex is the / response shape.
type surfaceIndex struct {
	Scope     string            `json:"scope"`
	Endpoints []surfaceEndpoint `json:"endpoints"`
	Note      string            `json:"note"`
}

type surfaceEndpoint struct {
	Path string `json:"path"`
	Owns string `json:"owns"`
}

// diagnoseBody is the /diagnose response shape. Small on purpose:
// one field per question an operator might plausibly ask about a
// single-node replica.
type diagnoseBody struct {
	CurrentDemo     string           `json:"current_demo,omitempty"`
	Mode            string           `json:"mode"`
	SessionPhase    string           `json:"session_phase"`
	Decision        string           `json:"decision"`
	Reason          string           `json:"reason,omitempty"`
	LastTraceStep   string           `json:"last_trace_step,omitempty"`
	WatchdogSummary watchdogSummary  `json:"watchdog_summary"`
	Note            string           `json:"note"`
}

// watchdogSummary collapses the watchdog log into counts per kind.
// The full log remains available at /watchdog; this is for quick
// at-a-glance reading.
type watchdogSummary struct {
	Total        int `json:"total"`
	Arms         int `json:"arms"`
	ClearedStart int `json:"cleared_by_start"`
	ClearedClose int `json:"cleared_by_close"`
	Fires        int `json:"fires"`
	FireNoops    int `json:"fire_noops"`
	Supersedes   int `json:"supersedes"`
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(body)
}
