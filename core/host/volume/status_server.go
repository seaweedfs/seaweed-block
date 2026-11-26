package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/ublk"
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
//	GET /status/nvme?volume=<id>
//	  -> 200 JSON read-only NVMe target counters (when a TCP target is wired)
//	  -> 404                                  (wrong volume or no TCP source)
//
//	GET /status/ublk?volume=<id>
//	  -> 200 JSON read-only ublk device request counters (when wired)
//	  -> 404                                  (wrong volume or no ublk device)
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
	runtimeEnabled  bool
	allowExternal   bool
	peerSource      peerStatusSource
	durableSource   durableStatusSource
	masterWait      func() *MasterWait // why a workspace primary holds no grant; nil: not wired
	nvmeSource      nvmeStatusSource
	ublkSource      ublkStatusSource
	runtimeSource   runtimeRecoverySource
	frontendCaps    []FrontendTransportCapability
	nvmePending     bool         // configured listener has not started (or is stopping)
	chunkEnabled    bool         // gates /chunk/*; off by default
	chunkControl    ChunkControl // nil until SetChunkControl
}

// SetNVMeListenerReady gates only external readiness, not storage recovery or
// authority. Call false before starting status for a configured NVMe listener,
// true only after Target.Start succeeds, and false before closing the target.
// Unconfigured callers retain the legacy default.
func (s *StatusServer) SetNVMeListenerReady(ready bool) {
	s.mu.Lock()
	s.nvmePending = !ready
	s.mu.Unlock()
}

type peerStatusSource interface {
	PeerStatuses() []replication.ReplicaPeerStatus
}

type durableStatusSource interface {
	DurableStatuses() []durable.VolumeStatus
	// AdmitsIO is the one deciding function for "may serve" of an open
	// volume (StorageBackend.AdmitsIO); an error for any other.
	AdmitsIO(volumeID string) error
}

type nvmeStatusSource interface {
	Stats() nvme.Stats
}

type ublkStatusSource interface {
	Stats() ublk.Stats
}

type runtimeRecoverySource interface {
	StartRuntimeRecovery(context.Context, replication.RuntimeRecoveryRequest) error
	RuntimeRecoveryStatus(context.Context, replication.RuntimeRecoveryRequest) (replication.RuntimeRecoveryStatus, error)
}

type RuntimeRebuildRequest struct {
	VolumeName          string   `json:"volumeName,omitempty"`
	VolumeID            string   `json:"volumeID"`
	PVCName             string   `json:"pvcName,omitempty"`
	ReplicaID           string   `json:"replicaID"`
	RuntimeEndpoint     string   `json:"runtimeEndpoint,omitempty"`
	TargetDataAddr      string   `json:"targetDataAddr,omitempty"`
	SessionID           uint64   `json:"sessionID"`
	Epoch               uint64   `json:"epoch"`
	EndpointVersion     uint64   `json:"endpointVersion"`
	FromLSN             uint64   `json:"fromLsn,omitempty"`
	FrontierHintLSN     uint64   `json:"frontierHintLsn"`
	BasePinLSN          uint64   `json:"basePinLsn,omitempty"`
	DurableFrontierLSN  uint64   `json:"durableFrontierLsn,omitempty"`
	RequiredFrontierLSN uint64   `json:"requiredFrontierLsn,omitempty"`
	EvidenceRefs        []string `json:"evidenceRefs,omitempty"`
}

type RuntimeRebuildResult struct {
	RuntimeState          string   `json:"runtimeState"`
	RebuildTrafficStarted bool     `json:"rebuildTrafficStarted"`
	DurableFrontierKnown  bool     `json:"durableFrontierKnown"`
	DurableFrontierLSN    uint64   `json:"durableFrontierLsn,omitempty"`
	EvidenceRefs          []string `json:"evidenceRefs,omitempty"`
}

type FrontendTransportCapability struct {
	Protocol            string                           `json:"protocol"`              // the block protocol (iscsi, nvme, ublk)
	Transport           string                           `json:"transport"`             // its transport (tcp, rdma, local)
	Supported           bool                             `json:"supported"`             // this build and host can serve it
	ListenerImplemented bool                             `json:"listenerImplemented"`   // a listener exists for it in this binary
	ListenerStarted     bool                             `json:"listenerStarted"`       // the listener is running now
	StartAllowed        bool                             `json:"startAllowed"`          // the preflight checks allow starting it
	StartReason         string                           `json:"startReason,omitempty"` // why it may or may not start
	Reason              string                           `json:"reason"`                // why it is or is not supported
	Preflight           []FrontendTransportPreflightFact `json:"preflight,omitempty"`   // the host facts checked for it
}

type FrontendTransportPreflightFact struct {
	Name      string `json:"name"`             // the host fact checked (a module, device or privilege)
	Available bool   `json:"available"`        // the fact holds on this host
	Reason    string `json:"reason"`           // why it holds or not
	Detail    string `json:"detail,omitempty"` // extra detail, such as the path probed
}

type FrontendCapabilitiesStatus struct {
	VolumeID                     string                        `json:"volumeID"`                     // the served volume
	HostCapabilityIsProductClaim bool                          `json:"hostCapabilityIsProductClaim"` // always false: host facts, not a supported-product claim
	FrontendTransports           []FrontendTransportCapability `json:"frontendTransports"`           // one entry per protocol and transport
}

type NVMeStatus struct {
	VolumeID string     `json:"volumeID"` // the served volume
	Stats    nvme.Stats `json:"stats"`    // the NVMe/TCP target's counters since start
}

// UblkStatus is the /status/ublk body: the ublk device's request counters.
type UblkStatus struct {
	VolumeID string     `json:"volumeID"` // the served volume
	Stats    ublk.Stats `json:"stats"`    // the ublk device's request counters since start
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
	// Memory is the process's memory use at the time of the request.
	Memory ProcessMemoryStatus
	// FrontendPrimaryReady: this replica is the healthy, non-superseded
	// primary, its local durable backend admits I/O, and it may serve guest
	// I/O (INV-FE-PRIMARY-READY-OPERATIONAL-001).
	FrontendPrimaryReady bool
	// NotReadyReason says why FrontendPrimaryReady is false, first match: a
	// workspace primary holds no granted lease now (NotReadyLeaseNotHeld); the
	// local durable backend admits no I/O (its evidence: attach pending,
	// fenced, detached) or has no status for the volume; the frontend target
	// is not listening yet; this replica is not the healthy primary. Empty
	// when ready.
	NotReadyReason string `json:",omitempty"`
	// AuthorityRole is primary, superseded or unknown.
	AuthorityRole string
	// ReplicationRole is none (a primary), recovering, not_ready,
	// replica_ready or unknown.
	ReplicationRole string
	// RoleAdmissionFailure (G1) explains a not-Healthy projection caused
	// by a failed role call on the replica listener; empty otherwise.
	RoleAdmissionFailure string `json:",omitempty"`
	// UnhealthyReason is the slot's explicit unhealthy report (#128 rule
	// 6), the value its heartbeat carries; empty when healthy.
	UnhealthyReason string `json:",omitempty"`
	// CatchUp shows a supporting replica's catch-up progress while it is
	// not yet replica_ready; it is visible without being an alert.
	CatchUp *CatchUpStatus `json:",omitempty"`
	// Warnings include routing disagreements and typed recovery warnings (e.g.
	// wal_hole_suspected, issue #51). Visible, not an alert: they do not
	// change Healthy or the frontend's readiness.
	Warnings []string `json:",omitempty"`
	// MasterWait is why a workspace primary holds no granted lease, present
	// only while NotReadyReason is lease_not_held: Reason (unreachable,
	// credentials, "identity refused: <kind>" with kind unregistered or
	// wrong_host, barrier, no_assignment, refused, moved, deleted), since
	// when, and the startup wait's one bound or the master's barrier time
	// (UntilMs, absent while there is no bound). Detail carries the master's
	// diagnostic text for a typed final refusal, including a moved holder and epoch.
	MasterWait *MasterWaitStatus `json:",omitempty"`
}

// MasterWaitStatus is MasterWait on /status (unix milliseconds).
type MasterWaitStatus struct {
	Reason  string
	Detail  string `json:",omitempty"`
	SinceMs int64
	UntilMs int64 `json:",omitempty"`
}

// SetMasterWait wires the host's master-wait reason into /status.
func (s *StatusServer) SetMasterWait(f func() *MasterWait) {
	s.mu.Lock()
	s.masterWait = f
	s.mu.Unlock()
}

// CatchUpStatus is a supporting replica's catch-up state.
type CatchUpStatus struct {
	// ElapsedMs is the time since this replica was admitted as a
	// supporting replica at its current epoch.
	ElapsedMs int64
	// LagLSN is the engine's H-R (primary head minus this replica's
	// durable frontier) as last observed; 0 when unknown or caught up.
	LagLSN uint64
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

func (s *StatusServer) EnableRuntimeEndpoint() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runtimeEnabled = true
}

// AllowExternalAccess opts into binding and serving on non-loopback addresses.
// This is for controlled Kubernetes node-loss gates where blockmaster must
// probe surviving replicas across nodes. The default remains loopback-only.
// Must be called BEFORE Start.
func (s *StatusServer) AllowExternalAccess() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return
	}
	s.allowExternal = true
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

// SetUblkStatusSource enables /status/ublk with the ublk device's counters.
// It may be called before or after Start.
func (s *StatusServer) SetUblkStatusSource(src ublkStatusSource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ublkSource = src
}

// SetNVMeStatusSource enables /status/nvme with a read-only target snapshot.
// It may be called before or after Start.
func (s *StatusServer) SetNVMeStatusSource(src nvmeStatusSource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nvmeSource = src
}

func (s *StatusServer) SetRuntimeRecoverySource(src runtimeRecoverySource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runtimeSource = src
}

func (s *StatusServer) SetFrontendCapabilities(caps []FrontendTransportCapability) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frontendCaps = append([]FrontendTransportCapability(nil), caps...)
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
	if s.allowExternal {
		if err := enforceExplicitExternalBind(addr); err != nil {
			return "", err
		}
	} else {
		if err := enforceLoopbackBind(addr); err != nil {
			return "", err
		}
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
	mux.HandleFunc("/status/nvme", s.handleStatusNVMe)
	mux.HandleFunc("/status/ublk", s.handleStatusUblk)
	mux.HandleFunc("/status/frontend-capabilities", s.handleFrontendCapabilities)
	if s.recoveryEnabled {
		mux.HandleFunc("/status/recovery", s.handleStatusRecovery)
	}
	if s.runtimeEnabled {
		mux.HandleFunc("/runtime/rebuild", s.handleRuntimeRebuild)
	}
	if s.chunkEnabled {
		mux.Handle("/chunk/", s.chunkMux())
		mux.HandleFunc("/status/chunk", s.handleStatusChunk)
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

func (s *StatusServer) externalAccessAllowed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allowExternal
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
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
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

func (s *StatusServer) handleFrontendCapabilities(w http.ResponseWriter, r *http.Request) {
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
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
	s.mu.Lock()
	caps := append([]FrontendTransportCapability(nil), s.frontendCaps...)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(FrontendCapabilitiesStatus{
		VolumeID:                     p.VolumeID,
		HostCapabilityIsProductClaim: false,
		FrontendTransports:           caps,
	})
}

func (s *StatusServer) handleStatusUblk(w http.ResponseWriter, r *http.Request) {
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
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
	s.mu.Lock()
	src := s.ublkSource
	s.mu.Unlock()
	if src == nil {
		http.Error(w, "ublk status source unavailable", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(UblkStatus{VolumeID: p.VolumeID, Stats: src.Stats()})
}

func (s *StatusServer) handleStatusNVMe(w http.ResponseWriter, r *http.Request) {
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
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
	s.mu.Lock()
	src := s.nvmeSource
	s.mu.Unlock()
	if src == nil {
		http.Error(w, "NVMe status source unavailable", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(NVMeStatus{VolumeID: p.VolumeID, Stats: src.Stats()})
}

// NotReadyLeaseNotHeld: a workspace primary without a granted lease holds at
// the lease-readiness gate (no claim, no listen, not ready).
const NotReadyLeaseNotHeld = "lease_not_held"

func (s *StatusServer) statusProjection() StatusProjection {
	p := s.authorityProjection()
	authorityReady := p.FrontendPrimaryReady
	s.mu.Lock()
	nvmePending := s.nvmePending
	p.Healthy = p.Healthy && !nvmePending
	p.FrontendPrimaryReady = p.FrontendPrimaryReady && !nvmePending
	src := s.durableSource
	masterWait := s.masterWait
	s.mu.Unlock()
	// INV-FE-PRIMARY-READY-OPERATIONAL-001: not FrontendPrimaryReady while the
	// local backend admits no I/O (before the storage claim of a chunk volume,
	// after a refused claim: fenced, after a detach), and not when there is no
	// durable status for the volume at all (fail closed). The authority view
	// alone (the adapter's PublishHealthy) is not readiness; Healthy stays that
	// view. Whenever FrontendPrimaryReady is false, NotReadyReason says why.
	backendReason := ""
	if src != nil { // outside s.mu: the provider takes its own lock
		for _, st := range src.DurableStatuses() {
			if st.VolumeID == p.VolumeID {
				p.Warnings = append(p.Warnings, st.Warnings...)
			}
		}
		// The frontend readiness rule: the backend's one "may serve" function decides; an
		// unopened volume fails closed.
		if err := src.AdmitsIO(p.VolumeID); err != nil {
			backendReason = "durable backend admits no I/O: " + err.Error()
		}
	}
	leaseNotHeld := s.view.workspaceLeaseNotHeld()
	if backendReason != "" || leaseNotHeld {
		p.FrontendPrimaryReady = false
	}
	if !p.FrontendPrimaryReady {
		switch {
		case leaseNotHeld:
			p.NotReadyReason = NotReadyLeaseNotHeld
			if masterWait != nil {
				if w := masterWait(); w != nil {
					p.MasterWait = &MasterWaitStatus{Reason: w.Reason, Detail: w.Detail, SinceMs: w.Since.UnixMilli()}
					if !w.Until.IsZero() {
						p.MasterWait.UntilMs = w.Until.UnixMilli()
					}
				}
			}
		case backendReason != "":
			p.NotReadyReason = backendReason
		case s.view.replicaStoresAwaited() != "":
			p.NotReadyReason = s.view.replicaStoresAwaited()
		case s.view.ackProfileRefusal() != "":
			p.NotReadyReason = s.view.ackProfileRefusal()
		case nvmePending:
			p.NotReadyReason = "frontend target not listening yet"
		case !authorityReady:
			p.NotReadyReason = "not the healthy primary: authority role " + p.AuthorityRole + ", replication role " + p.ReplicationRole
		}
	}
	return p
}

// ProbeRoles is this replica's role as the promotion probe round reads it:
// a replica ready to be promoted, or a primary that serves.
func (s *StatusServer) ProbeRoles() (replicaReady, primaryReady bool) {
	p := s.authorityProjection()
	return p.ReplicationRole == ReplicationRoleReady, p.FrontendPrimaryReady
}

// authorityProjection preserves the storage/authority gate used by runtime
// recovery. Listener startup masks presentation only, not recovery admission.
func (s *StatusServer) authorityProjection() StatusProjection {
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
	out := StatusProjection{
		Memory:               processMemoryStatus(),
		Projection:           p,
		FrontendPrimaryReady: p.Healthy && !superseded && !supportingReady,
		AuthorityRole:        authorityRole,
		ReplicationRole:      replicationRole,
		RoleAdmissionFailure: s.view.roleAdmissionReason(),
		UnhealthyReason:      s.view.unhealthyReason(),
	}
	if warning := s.view.assignmentEndpointWarning(); warning != "" {
		out.Warnings = append(out.Warnings, warning)
	}
	if since := s.view.catchUpSince(); !since.IsZero() && replicationRole != ReplicationRoleReady {
		cu := &CatchUpStatus{ElapsedMs: time.Since(since).Milliseconds()}
		if ep.H > ep.R {
			cu.LagLSN = ep.H - ep.R
		}
		out.CatchUp = cu
	}
	return out
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
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
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
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
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
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
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

func (s *StatusServer) handleRuntimeRebuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "runtime rebuild requires POST", http.StatusMethodNotAllowed)
		return
	}
	if !s.externalAccessAllowed() && !isLoopbackRemote(r.RemoteAddr) {
		http.Error(w, "runtime endpoint restricted to loopback", http.StatusForbidden)
		return
	}
	var req RuntimeRebuildRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&req); err != nil {
		http.Error(w, "decode runtime rebuild request: "+err.Error(), http.StatusBadRequest)
		return
	}
	p := s.authorityProjection()
	if req.VolumeID == "" {
		http.Error(w, "volumeID is required", http.StatusBadRequest)
		return
	}
	if p.VolumeID != req.VolumeID {
		http.Error(w, fmt.Sprintf("volume %q not served by this host (serves %q)", req.VolumeID, p.VolumeID), http.StatusNotFound)
		return
	}
	if !p.FrontendPrimaryReady {
		http.Error(w, "runtime rebuild requires local frontend primary readiness", http.StatusConflict)
		return
	}
	if req.ReplicaID == "" {
		http.Error(w, "replicaID is required", http.StatusBadRequest)
		return
	}
	if req.SessionID == 0 || req.Epoch == 0 || req.EndpointVersion == 0 || req.FrontierHintLSN == 0 {
		http.Error(w, "nonzero sessionID, epoch, endpointVersion, and frontierHintLsn are required", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	src := s.runtimeSource
	s.mu.Unlock()
	if src == nil {
		http.Error(w, "runtime recovery source is not configured", http.StatusNotFound)
		return
	}
	recoveryReq := replication.RuntimeRecoveryRequest{
		ReplicaID:       req.ReplicaID,
		TargetDataAddr:  req.TargetDataAddr,
		SessionID:       req.SessionID,
		Epoch:           req.Epoch,
		EndpointVersion: req.EndpointVersion,
		FromLSN:         req.FromLSN,
		FrontierHintLSN: req.FrontierHintLSN,
		BasePinLSN:      req.BasePinLSN,
	}
	if status, err := src.RuntimeRecoveryStatus(r.Context(), recoveryReq); err != nil {
		http.Error(w, "query runtime recovery status: "+err.Error(), http.StatusConflict)
		return
	} else if status.State == "caught_up" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RuntimeRebuildResult{
			RuntimeState:          "caught_up",
			RebuildTrafficStarted: true,
			DurableFrontierKnown:  true,
			DurableFrontierLSN:    status.AchievedLSN,
			EvidenceRefs: append([]string{
				"blockvolume_runtime_rebuild_caught_up",
			}, req.EvidenceRefs...),
		})
		return
	} else if status.State == "failed" {
		reason := status.FailReason
		if reason == "" {
			reason = status.FailureKind
		}
		http.Error(w, "runtime recovery failed: "+reason, http.StatusConflict)
		return
	} else if status.State == "running" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RuntimeRebuildResult{
			RuntimeState:          "started",
			RebuildTrafficStarted: true,
			DurableFrontierKnown:  false,
			EvidenceRefs: append([]string{
				"blockvolume_runtime_rebuild_running",
			}, req.EvidenceRefs...),
		})
		return
	}
	if err := src.StartRuntimeRecovery(r.Context(), recoveryReq); err != nil {
		http.Error(w, "start runtime recovery: "+err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(RuntimeRebuildResult{
		RuntimeState:          "started",
		RebuildTrafficStarted: true,
		DurableFrontierKnown:  false,
		EvidenceRefs: append([]string{
			"blockvolume_runtime_rebuild_started",
		}, req.EvidenceRefs...),
	})
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

func enforceExplicitExternalBind(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("status: bind addr %q not host:port: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("status: bind addr %q has empty host; external status bind requires an explicit node address", addr)
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("status: bind addr %q is unspecified; external status bind requires a concrete node address", addr)
	}
	if isLocalhostOrLoopbackHost(host) {
		return fmt.Errorf("status: bind addr %q is loopback; external status bind requires a non-loopback node address", addr)
	}
	return nil
}

func isLocalhostOrLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
