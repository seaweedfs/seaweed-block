package volume

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// The existing diagnostic listener and access policy apply; no new listener,
// remote query or readiness authority is introduced. One scrape names a volume.
func (s *StatusServer) handleChunkMetrics(w http.ResponseWriter, r *http.Request) {
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
		http.Error(w, "status endpoint restricted to loopback", http.StatusForbidden)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	vol := r.URL.Query().Get("volume")
	if vol == "" {
		http.Error(w, "missing volume query param", http.StatusBadRequest)
		return
	}
	if s.view.Projection().VolumeID != vol {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	src := s.durableSource
	s.mu.Unlock()
	if src == nil {
		http.NotFound(w, r)
		return
	}
	for _, st := range src.DurableStatuses() {
		if st.VolumeID != vol || st.Chunk == nil || !st.Chunk.Attached {
			continue
		}
		c := st.Chunk.Stats
		label := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"").Replace(vol)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		gauge := func(name, help string, v any) {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s{volume=\"%s\"} %v\n", name, help, name, name, label, v)
		}
		gauge("swblock_chunk_dirty_chunks", "Known dirty chunks; incomplete while reconciling.", c.DirtyChunks)
		gauge("swblock_chunk_dirty_chunk_bytes", "Full-chunk coverage estimate of known dirt, not changed payload bytes.", c.DirtyChunkBytes)
		gauge("swblock_chunk_upload_pending", "Queued and in-flight chunk uploads in the current batch; excludes manifests.", c.UploadPending)
		gauge("swblock_chunk_absent_chunks", "Chunks awaiting lazy fill; not an eager-fill queue.", c.AbsentChunks)
		reconciling := 0
		if c.Reconciling {
			reconciling = 1
		}
		gauge("swblock_chunk_reconciling", "One while the dirty set is incomplete.", reconciling)
		known := 0
		if c.LastCutNs > 0 {
			known = 1
		}
		gauge("swblock_chunk_cut_time_known", "One after a successful cut in this attachment; zero means age unknown.", known)
		if known == 1 {
			gauge("swblock_chunk_cut_age_seconds", "Seconds since the last successful cut, including no-op cuts.", max(0, time.Since(time.Unix(0, c.LastCutNs)).Seconds()))
		}
		return
	}
	http.NotFound(w, r)
}
