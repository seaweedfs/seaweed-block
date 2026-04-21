package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
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

	mu   sync.Mutex
	ln   net.Listener
	srv  *http.Server
	addr string
}

// NewStatusServer wires a server to one volume host's view.
func NewStatusServer(view *AdapterProjectionView) *StatusServer {
	return &StatusServer{view: view}
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
	p := s.view.Projection()
	if p.VolumeID != vol {
		http.Error(w, fmt.Sprintf("volume %q not served by this host (serves %q)", vol, p.VolumeID), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p)
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

// Ensure compile-time check that the type returned over the wire
// matches frontend.Projection exactly (clients decode the same
// struct).
var _ frontend.Projection = frontend.Projection{}
