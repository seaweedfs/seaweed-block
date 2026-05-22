package ops

import (
	"fmt"
	"net/http"
)

// NewObservationDashboardHandler serves the same read-only artifacts produced by
// sw-block ops report. It is intentionally observation-only: unsafe methods are
// rejected here before any future UI layer can accidentally grow mutations.
func NewObservationDashboardHandler(cluster ClusterEvidence) http.Handler {
	cluster = NormalizeObservationCluster(cluster)
	return observationDashboardHandler{cluster: cluster}
}

type observationDashboardHandler struct {
	cluster ClusterEvidence
}

func (h observationDashboardHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "read-only dashboard: method not allowed", http.StatusMethodNotAllowed)
		return
	}

	switch r.URL.Path {
	case "/", "/" + ObservationReportHTMLArtifact:
		writeDashboardResponse(w, r, "text/html; charset=utf-8", []byte(RenderObservationReportHTML(h.cluster)))
	case "/" + ObservationReportJSONArtifact:
		raw, err := MarshalObservationJSON(h.cluster)
		if err != nil {
			http.Error(w, fmt.Sprintf("render cluster evidence: %v", err), http.StatusInternalServerError)
			return
		}
		writeDashboardResponse(w, r, "application/json; charset=utf-8", raw)
	case "/" + ObservationReportJSONLArtifact:
		raw, err := RenderClusterEventsJSONL(h.cluster.Events)
		if err != nil {
			http.Error(w, fmt.Sprintf("render timeline: %v", err), http.StatusInternalServerError)
			return
		}
		writeDashboardResponse(w, r, "application/x-ndjson; charset=utf-8", []byte(raw))
	case "/" + ObservationReportTextArtifact:
		writeDashboardResponse(w, r, "text/plain; charset=utf-8", []byte(RenderObservationReportSummary(h.cluster)))
	case "/healthz":
		writeDashboardResponse(w, r, "text/plain; charset=utf-8", []byte("ok\n"))
	default:
		http.NotFound(w, r)
	}
}

func writeDashboardResponse(w http.ResponseWriter, r *http.Request, contentType string, body []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(body)
}
