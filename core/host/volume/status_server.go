package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/replication"
)

// StatusServer exposes this volume host's frontend.ProjectionView
// over a small HTTP endpoint so external processes (L2 tests,
// operator dashboards, the future frontend sidecar) can observe
// readiness without linking against the host binary.
//
// Scope + trust (architect review, 2026-04-21):
//
//	This is a DIAGNOSTIC / TEST surface, not a production API.
//	No authentication is performed. To avoid accidentally
//	shipping an unauthenticated status API:
//	  1. The listener MUST bind on a loopback address
//	     (127.0.0.1 or ::1). Any other bind is rejected.
//	  2. Handler rejects remote clients (non-loopback RemoteAddr).
//	Real frontend/operator endpoints are T5+ territory and will
//	ship with AuthN/AuthZ.
//
// Contract:
//
//	GET /status?volume=<id>
//	  -> 200 JSON frontend.Projection         (healthy or not)
//	  -> 404                                  (volume_id doesn't match this host)
//	  -> 400                                  (missing volume query param)
//	  -> 403                                  (non-loopback client)
//
// The server binds on a caller-supplied addr (":0" for tests);
// the actual bound addr is available via Addr() after Start.
type StatusServer struct {
	view *AdapterProjectionView

	mu              sync.Mutex
	ln              net.Listener
	srv             *http.Server
	addr            string
	recoveryEnabled bool // G5-5: gates /status/recovery; off by default
	peerSource      peerStatusSource
	durableSource   durableStatusSource
}

type peerStatusSource interface {
	PeerStatuses() []replication.ReplicaPeerStatus
}

type durableStatusSource interface {
	DurableStatuses() []durable.VolumeStatus
}

const (
	AuthorityRolePrimary    = "primary"
	AuthorityRoleSuperseded = "superseded"
	AuthorityRoleUnknown    = "unknown"

	ReplicationRoleNone       = "none"
	ReplicationRoleRecovering = "recovering"
	ReplicationRoleNotReady   = "not_ready"
	ReplicationRoleReady      = "replica_ready"
	ReplicationRoleUnknown    = "unknown"
)

// StatusProjection is the append-only /status shape. It embeds the
// original frontend.Projection fields for wire compatibility, then
// adds G9A control-plane vocabulary so operators/tests do not infer
// replica readiness from authority movement.
type StatusProjection struct {
	frontend.Projection
	FrontendPrimaryReady bool
	AuthorityRole        string
	ReplicationRole      string
}

// NewStatusServer wires a server to one volume host's view.
func NewStatusServer(view *AdapterProjectionView) *StatusServer {
	return &StatusServer{view: view}
}

// EnableRecoveryEndpoint opts in the test/operator-only
// `/status/recovery?volume=v1` endpoint, which surfaces the
// engine.ReplicaProjection (Mode, R/S/H, RecoveryDecision,
// SessionKind/Phase, Reason) over HTTP. The endpoint is
// loopback-only (same guard as `/status`).
//
// G5-5 uses this for hardware-test catch-up verification: the
// orchestration script polls primary's H and replica's R to assert
// catch-up convergence. Production binaries do NOT enable it
// (default off; opt-in via --status-recovery flag). The endpoint
// surfaces existing engine projection — no engine/adapter
// behavior change.
//
// Must be called BEFORE Start.
func (s *StatusServer) EnableRecoveryEndpoint() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recoveryEnabled = true
}

// SetPeerStatusSource enables /status/peers by wiring a primary-side
// replication status source. It may be called before or after Start:
// the route is registered at Start, and returns 404 until a source is
// installed. This fits blockvolume startup, where the status listener
// binds before durable storage and ReplicationVolume are constructed.
func (s *StatusServer) SetPeerStatusSource(src peerStatusSource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerSource = src
}

// SetDurableStatusSource enables /status/durable by wiring the durable
// provider's read-only status surface. It may be called before or after Start.
func (s *StatusServer) SetDurableStatusSource(src durableStatusSource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.durableSource = src
}

// Start binds on addr and spawns the HTTP serve goroutine.
// addr MUST be a loopback host (empty-host is rejected; any
// non-loopback IP returns an error). Returns the bound addr
// (useful when addr is ":0"). Safe to call once; second call
// returns an error.
func (s *StatusServer) Start(addr string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return "", fmt.Errorf("status: already started")
	}
	if err := enforceLoopbackBind(addr); err != nil {
		return "", err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("status: listen %q: %w", addr, err)
	}
	s.ln = ln
	s.addr = ln.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/status/peers", s.handleStatusPeers)
	mux.HandleFunc("/status/durable", s.handleStatusDurable)
	if s.recoveryEnabled {
		mux.HandleFunc("/status/recovery", s.handleStatusRecovery)
	}
	s.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() { _ = s.srv.Serve(ln) }()
	return s.addr, nil
}

// Addr returns the bound address, or "" if Start hasn't been
// called (or failed).
func (s *StatusServer) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// Close gracefully shuts the server down. Idempotent.
func (s *StatusServer) Close(ctx context.Context) error {
	s.mu.Lock()
	srv := s.srv
	s.srv = nil
	s.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

func (s *StatusServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRemote(r.RemoteAddr) {
		http.Error(w, "status endpoint restricted to loopback", http.StatusForbidden)
		return
	}
	vol := r.URL.Query().Get("volume")
	if vol == "" {
		http.Error(w, "missing volume query param", http.StatusBadRequest)
		return
	}
	p := s.statusProjection()
	if p.VolumeID != vol {
		http.Error(w, fmt.Sprintf("volume %q not served by this host (serves %q)", vol, p.VolumeID), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p)
}

func (s *StatusServer) statusProjection() StatusProjection {
	p, superseded := s.view.projectionWithSupersede()
	ep := s.view.EngineProjection()
	authorityRole := AuthorityRoleUnknown
	replicationRole := ReplicationRoleUnknown
	supportingReady := s.supportingReplicaReady(p.Epoch, p.EndpointVersion)
	switch {
	case p.Healthy && !superseded && !supportingReady:
		authorityRole = AuthorityRolePrimary
		replicationRole = ReplicationRoleNone
	case superseded:
		authorityRole = AuthorityRoleSuperseded
		replicationRole = ReplicationRoleNotReady
	case ep.Mode == engine.ModeRecovering:
		replicationRole = ReplicationRoleRecovering
	case ep.Mode == engine.ModeHealthy && supportingReady:
		replicationRole = ReplicationRoleReady
	case ep.Mode == engine.ModeDegraded || ep.Mode == engine.ModeIdle:
		replicationRole = ReplicationRoleNotReady
	}
	return StatusProjection{
		Projection:           p,
		FrontendPrimaryReady: p.Healthy && !superseded && !supportingReady,
		AuthorityRole:        authorityRole,
		ReplicationRole:      replicationRole,
	}
}

func (s *StatusServer) supportingReplicaReady(selfEpoch, selfEV uint64) bool {
	return s.view.supportingReplicaReady(selfEpoch, selfEV)
}

// handleStatusRecovery returns engine.ReplicaProjection for the
// targeted volume. Loopback-only; same guards as /status. Surfaced
// fields cover the recovery boundaries (R/S/H), recovery decision,
// session kind/phase, and reason — used by G5-5 hardware-test
// catch-up verification (R/H polling).
//
// This endpoint is OPT-IN (off by default). Production binaries do
// not enable it; m01 orchestration script + tests do via
// --status-recovery / EnableRecoveryEndpoint.
//
// The response shape is the engine projection struct — JSON field
// names follow Go default capitalization. Schema-stable across
// G5-5 close (per `v3-batch-process.md` no engine/adapter behavior
// change rule); future field additions are append-only.
func (s *StatusServer) handleStatusRecovery(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRemote(r.RemoteAddr) {
		http.Error(w, "status endpoint restricted to loopback", http.StatusForbidden)
		return
	}
	vol := r.URL.Query().Get("volume")
	if vol == "" {
		http.Error(w, "missing volume query param", http.StatusBadRequest)
		return
	}
	// Frontend projection is the cheap volume-id check; if this
	// host doesn't serve `vol`, fail-fast with 404 (same as /status).
	fp := s.view.Projection()
	if fp.VolumeID != vol {
		http.Error(w, fmt.Sprintf("volume %q not served by this host (serves %q)", vol, fp.VolumeID), http.StatusNotFound)
		return
	}
	ep := s.view.EngineProjection()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ep)
}

func (s *StatusServer) handleStatusPeers(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRemote(r.RemoteAddr) {
		http.Error(w, "status endpoint restricted to loopback", http.StatusForbidden)
		return
	}
	vol := r.URL.Query().Get("volume")
	if vol == "" {
		http.Error(w, "missing volume query param", http.StatusBadRequest)
		return
	}
	fp := s.view.Projection()
	if fp.VolumeID != vol {
		http.Error(w, fmt.Sprintf("volume %q not served by this host (serves %q)", vol, fp.VolumeID), http.StatusNotFound)
		return
	}
	s.mu.Lock()
	src := s.peerSource
	s.mu.Unlock()
	if src == nil {
		http.NotFound(w, r)
		return
	}
	peers := src.PeerStatuses()
	body := struct {
		VolumeID  string
		ReplicaID string
		PeerCount int
		Peers     []replication.ReplicaPeerStatus
	}{
		VolumeID:  fp.VolumeID,
		ReplicaID: fp.ReplicaID,
		PeerCount: len(peers),
		Peers:     peers,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func (s *StatusServer) handleStatusDurable(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRemote(r.RemoteAddr) {
		http.Error(w, "status endpoint restricted to loopback", http.StatusForbidden)
		return
	}
	vol := r.URL.Query().Get("volume")
	if vol == "" {
		http.Error(w, "missing volume query param", http.StatusBadRequest)
		return
	}
	fp := s.view.Projection()
	if fp.VolumeID != vol {
		http.Error(w, fmt.Sprintf("volume %q not served by this host (serves %q)", vol, fp.VolumeID), http.StatusNotFound)
		return
	}
	s.mu.Lock()
	src := s.durableSource
	s.mu.Unlock()
	if src == nil {
		http.NotFound(w, r)
		return
	}
	var vols []durable.VolumeStatus
	for _, st := range src.DurableStatuses() {
		if st.VolumeID == vol {
			vols = append(vols, st)
		}
	}
	if len(vols) == 0 {
		http.NotFound(w, r)
		return
	}
	body := struct {
		VolumeID    string
		ReplicaID   string
		VolumeCount int
		Volumes     []durable.VolumeStatus
	}{
		VolumeID:    fp.VolumeID,
		ReplicaID:   fp.ReplicaID,
		VolumeCount: len(vols),
		Volumes:     vols,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

// enforceLoopbackBind refuses to bind on anything other than
// 127.0.0.1 / ::1. Empty-host binds (":port") would listen on
// ALL interfaces and are rejected. The guard runs before the
// actual net.Listen so a misconfigured operator gets a clear
// error without ever opening a public port.
func enforceLoopbackBind(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("status: bind addr %q not host:port: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("status: bind addr %q has empty host; must be a loopback address (127.0.0.1 or ::1)", addr)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("status: bind addr %q is not loopback; refusing to expose unauthenticated diagnostic endpoint", addr)
	}
	return nil
}

// isLoopbackRemote returns true if RemoteAddr parses to a
// loopback IP. Used at request time as a defence-in-depth
// check against a reverse-proxy or kernel-level surprise.
func isLoopbackRemote(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Ensure StatusProjection remains a pure append-only wrapper around
// frontend.Projection; existing clients can still decode /status into
// frontend.Projection because json.Decoder ignores unknown fields.
var _ = StatusProjection{Projection: frontend.Projection{}}
