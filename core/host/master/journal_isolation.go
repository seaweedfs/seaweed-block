package master

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/storeident"
)

// journalIsolation tracks nodes whose durable root shares the authority
// store's file system. On one ext4 file system every fsync goes through one
// jbd2 journal: a volume's WAL fsync storm delays the authority journal's
// lease extensions, grants clamp to the durable horizon and leases run out.
//
// The colocated set is the latest heartbeat fact per server (memory only): a
// node that moves its durable root off the store's file system leaves the set
// on its next heartbeat. What a node serves is never read from here; that is
// decided from durable state (nodeServesVolumes).
type journalIsolation struct {
	mu        sync.Mutex
	self      storeident.Identity
	colocated map[string]storeident.Identity // server id -> its reported identity
}

func newJournalIsolation(self storeident.Identity) *journalIsolation {
	return &journalIsolation{self: self, colocated: map[string]storeident.Identity{}}
}

// observe records server's reported identity. It returns whether the node is
// co-located and, on its first detection, the line to log.
func (j *journalIsolation) observe(server string, id storeident.Identity) (bool, string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.self.SameFileSystem(id) {
		delete(j.colocated, server)
		return false, ""
	}
	if _, seen := j.colocated[server]; seen {
		return true, ""
	}
	j.colocated[server] = id
	return true, fmt.Sprintf("blockmaster: node %s shares the authority store's file system (machine %s, dev %d): its fsyncs share the authority journal", server, id.MachineID, id.Dev)
}

func (j *journalIsolation) isColocated(server string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	_, ok := j.colocated[server]
	return ok
}

func (j *journalIsolation) count() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.colocated)
}

func (j *journalIsolation) setSelf(id storeident.Identity) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.self = id
}

// nodeServesVolumes reports whether durable state already gives server work:
// a lifecycle placement slot on it, or a current authority line whose source
// host (an issued move's target, the placement, or a static topology slot) is
// it. Both survive a master restart, so a restart never turns a serving node
// into a new one. A heartbeat must not wait behind a slow authority write: it
// takes neither workspaceMu nor the publisher lock (held across authority
// store writes), only the published authority lines and the lifecycle
// stores, whose locks cover only their own file I/O.
func (h *Host) nodeServesVolumes(server string) bool {
	lines := h.Publisher().PublishedAuthorityLines()
	if h.lifecycle != nil {
		for _, p := range h.lifecycle.Placements.ListPlacements() {
			for _, slot := range p.Slots {
				if slot.ServerID == server {
					return true // also every line a placement slot sources
				}
			}
		}
		for volume, line := range lines {
			if _, live := h.lifecycle.Volumes.GetVolumeSnapshot(volume); live && line.IssuedHost == server {
				return true
			}
		}
	}
	for _, v := range h.topo.Volumes {
		line, ok := lines[v.VolumeID]
		if !ok {
			continue
		}
		for _, slot := range v.Slots {
			if slot.ServerID == server && slot.ReplicaID == line.ReplicaID {
				return true
			}
		}
	}
	return false
}

// refuseColocatedHeartbeat is the strict heartbeat rule: a co-located node
// that serves nothing yet is refused; a serving one is only alarmed.
func (h *Host) refuseColocatedHeartbeat(server string, id storeident.Identity) error {
	colocated, note := h.journal.observe(server, id)
	if note != "" {
		h.log.Print(note)
	}
	if !colocated || !h.cfg.StrictJournalIsolation || h.nodeServesVolumes(server) {
		return nil
	}
	return fmt.Errorf("heartbeat: node %s shares the authority store's file system (machine %s, dev %d) and serves no volume; strict journal isolation refuses it", server, id.MachineID, id.Dev)
}

// colocatedTargetRefused reports whether strict isolation refuses new work
// (a placement or move target) on server.
func (h *Host) colocatedTargetRefused(server string) bool {
	return h.cfg.StrictJournalIsolation && h.journal.isColocated(server)
}

// reconcilePlacements runs the product loop's placement reconcile. Under
// strict isolation each volume plans against the nodes without co-located
// ones, except the nodes its existing placement already names: a placement
// on a co-located node is kept (re-planning it away would move a serving
// volume), and no new placement chooses a co-located node.
func (h *Host) reconcilePlacements(stores *LifecycleStores, nodes []lifecycle.NodeRegistration) []lifecycle.ReconcileResult {
	if !h.cfg.StrictJournalIsolation {
		return lifecycle.ReconcilePlacement(stores.liveVolumes(), nodes, stores.Placements)
	}
	var out []lifecycle.ReconcileResult
	for _, v := range stores.liveVolumes() {
		placed := map[string]bool{}
		if p, ok := stores.Placements.GetPlacement(v.Spec.VolumeID); ok {
			for _, slot := range p.Slots {
				placed[slot.ServerID] = true
			}
		}
		allowed := make([]lifecycle.NodeRegistration, 0, len(nodes))
		for _, n := range nodes {
			if placed[n.ServerID] || !h.journal.isColocated(n.ServerID) {
				allowed = append(allowed, n)
			}
		}
		out = append(out, lifecycle.ReconcilePlacement([]lifecycle.VolumeRecord{v}, allowed, stores.Placements)...)
	}
	return out
}

// ValidateStatusListen accepts "" (disabled) or a loopback host:port
// (127.0.0.0/8, ::1, localhost). The status endpoint is unauthenticated.
func ValidateStatusListen(addr string) error {
	if addr == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("--status-listen %q: %v", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("--status-listen %q must be a loopback address (127.0.0.0/8, ::1 or localhost)", addr)
}

// WorkspaceLeaseStatus is the GET /status/workspace-lease body.
type WorkspaceLeaseStatus struct {
	ColocatedNodes         int    `json:"colocated_nodes"`
	HeadroomAlarms         uint64 `json:"headroom_alarms"`
	ClampsTotal            uint64 `json:"clamps_total"`
	ClampsLastMinute       uint64 `json:"clamps_last_minute"`
	MinHeadroomMS          int64  `json:"min_headroom_ms"` // -1: no grant observed yet
	StrictJournalIsolation bool   `json:"strict_journal_isolation"`
	// Lease extension store write latency, the earliest lag signal.
	ExtensionWriteLastMS          int64 `json:"extension_write_last_ms"`            // -1: no extension write yet
	ExtensionWriteMaxLastMinuteMS int64 `json:"extension_write_max_last_minute_ms"` // -1: none in the last minute
}

// WorkspaceLeaseStatus returns the journal-isolation and lease headroom
// counters served by the status endpoint.
func (h *Host) WorkspaceLeaseStatus() WorkspaceLeaseStatus {
	st := WorkspaceLeaseStatus{ColocatedNodes: h.journal.count(), MinHeadroomMS: -1, StrictJournalIsolation: h.cfg.StrictJournalIsolation,
		ExtensionWriteLastMS: -1, ExtensionWriteMaxLastMinuteMS: -1}
	if p := h.Publisher(); p != nil {
		s := p.WorkspaceLeaseStats()
		st.HeadroomAlarms, st.ClampsTotal, st.ClampsLastMinute = s.HeadroomAlarms, s.ClampsTotal, s.ClampsLastMinute
		if s.MinHeadroomObserved {
			st.MinHeadroomMS = s.MinHeadroom.Milliseconds()
		}
		if s.ExtensionWriteObserved {
			st.ExtensionWriteLastMS = s.ExtensionWriteLast.Milliseconds()
		}
		if s.ExtensionWriteInLastMinute {
			st.ExtensionWriteMaxLastMinuteMS = s.ExtensionWriteMaxLastMinute.Milliseconds()
		}
	}
	return st
}

func (h *Host) statusHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/status/workspace-lease", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(h.WorkspaceLeaseStatus())
	})
	return mux
}
