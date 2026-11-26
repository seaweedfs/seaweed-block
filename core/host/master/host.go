// Package master implements the block master host: the product-side
// composition of P14 authority components (ObservationHost,
// TopologyController, Publisher, durable authority store) plus
// the gRPC server that exposes ObservationService / AssignmentService
// / EvidenceService to block volume daemons and operator tools.
//
// T0 scope (v3-phase-15-t0-sketch.md §1): this package is the
// master-side hosting layer only. It does NOT implement frontend,
// data path, CSI, security, or operator workflows beyond the
// read-only status surface.
//
// Boundary (sketch §3): gRPC messages carry observation facts
// (volume -> master) and minted assignment facts (master ->
// volume). No RPC endpoint accepts an AssignmentAsk or a
// mutation input.
package master

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/host/bootstrap"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"github.com/seaweedfs/seaweed-block/core/storeident"
	"google.golang.org/grpc"
)

// Config holds the master host construction inputs. Flag parsing
// lives in cmd/blockmaster; this type is the testable seam.
type Config struct {
	// Auth authenticates and authorizes every gRPC call (phase 0): nil is
	// development mode (plaintext; every call is marked so, and the node
	// checks admit it).
	Auth *auth.Authenticator
	// ServerOptions are added to the gRPC server (transport credentials,
	// connection age).
	ServerOptions []grpc.ServerOption

	// AuthorityStoreDir is the directory passed to
	// authority.AcquireStoreLock + NewFileAuthorityStore. Must be
	// non-empty.
	AuthorityStoreDir string

	// Listen is the TCP address for the gRPC server, e.g.
	// "127.0.0.1:0" or ":9180".
	Listen string

	// Topology is the accepted topology the observation host
	// uses to decide supportability.
	Topology authority.AcceptedTopology

	// Freshness configures observation expiry semantics.
	Freshness authority.FreshnessConfig

	// ControllerConfig tunes controller knobs (retry window etc).
	ControllerConfig authority.TopologyControllerConfig

	// LifecycleProductLoop reports that the host's lifecycle product loop
	// runs (blockmaster --lifecycle-product-loop-interval above 0). Only
	// that loop binds, mints and promotes a replicated lifecycle volume, so
	// CreateVolume refuses a replicated volume while it is off.
	LifecycleProductLoop bool

	// LifecycleStoreDir, when non-empty, opens product lifecycle
	// registration stores (desired volumes, node inventory, placement
	// intents). This is read-only composition for G9D: these stores do
	// NOT drive assignment publication.
	LifecycleStoreDir string

	// FailbackRuntimeRPC enables the authority failback RPC surface. It is
	// disabled by default; callers must still provide expected-current and
	// terminal evidence when enabled.
	FailbackRuntimeRPC bool

	// WorkspaceRPC enables the RF1 lease/revoke/move surface. HEAD must be
	// obtained from the configured remote source, never from operator flags.
	WorkspaceMaxClockSkew time.Duration
	WorkspaceRPC          bool
	WorkspaceHeadReader   func(context.Context, string) (authority.WorkspaceHead, error)
	// LayoutCreateVolume writes VOLUME then the unclaimed HEAD using the
	// immutable identity and geometry in the persisted lifecycle record.
	LayoutCreateVolume func(context.Context, lifecycle.VolumeSpec) error
	// Restore creation reads the source VOLUME's immutable geometry before
	// persisting the target, then replays CreateTarget using that record.
	LayoutReadVolume   func(context.Context, lifecycle.VolumeSpec) (layout.Volume, error)
	LayoutReadHead     func(context.Context, lifecycle.VolumeSpec) (layout.Head, error)
	LayoutCreateTarget func(context.Context, lifecycle.VolumeSpec, lifecycle.VolumeSpec, string) error

	// FrontendPublicationRuntimeHTTP enables the frontend publication runtime
	// HTTP surface. It is disabled by default and only confirms a post-failback
	// authority line that has already been minted by the failback owner.
	FrontendPublicationRuntimeHTTP   bool
	FrontendPublicationRuntimeListen string

	// StrictJournalIsolation refuses nodes whose durable root shares the
	// authority store's file system (one journal): a co-located node that
	// serves no volume yet gets its heartbeats refused, and no new placement
	// or move targets a co-located node. A co-located node that already
	// serves volumes is only alarmed (refusing it would stop its lease
	// renewals). Off: alarm only.
	StrictJournalIsolation bool

	// StatusListen, when non-empty, serves the read-only status endpoint
	// GET /status/workspace-lease on this loopback address.
	StatusListen string

	// Logger is used for structured startup and error logging. If
	// nil, the default log package logger is used.
	Logger *log.Logger
}

// LifecycleStores groups the product registration stores hosted by
// blockmaster. The stores are controller input only; assignment
// publication remains owned by authority.Controller/Publisher.
type LifecycleStores struct {
	Volumes           *lifecycle.FileStore
	Nodes             *lifecycle.NodeInventoryStore
	Placements        *lifecycle.PlacementIntentStore
	WorkspaceBindings *lifecycle.WorkspaceBindingStore
	// Deletions holds the chunk-volume deletion reservations.
	Deletions *lifecycle.DeletionReservationStore
}

// Host is the composed master-side block product daemon. Lifecycle
// is: New -> Start -> (serve requests) -> Close.
type Host struct {
	probeHub            *probeHub   // the promotion probe round (probe_round.go)
	probeSlots          slotServers // replica -> server, for the round
	cfg                 Config
	attest              *storeAttestations // primaries' attested write-ACK stores
	healthMu            sync.Mutex         // orders replica health events with their accepted heartbeats (#128 rule 6)
	log                 *log.Logger
	boot                *bootstrap.DurableAuthorityBootstrap
	ctrl                *authority.TopologyController
	obs                 *authority.ObservationHost
	ln                  net.Listener
	frontendRuntimeLn   net.Listener
	frontendRuntimeHTTP *http.Server
	statusLn            net.Listener
	statusHTTP          *http.Server
	journal             *journalIsolation
	grpc                *grpc.Server
	topo                authority.AcceptedTopology
	lifecycle           *LifecycleStores
	// RPC registration/place/move/ACK/delete and product ticks serialize here.
	// Order: workspaceMu -> publisher or lifecycle store locks (not reversed).
	workspaceMu     sync.Mutex
	events          *eventRing
	promotionMu     sync.RWMutex
	promotionProber PromotionEvidenceProvider
	// fencePending: volume -> the epoch a fencing round fenced survivors at
	// without a line being published there. Until the line reaches it, those
	// survivors refuse the primary, so the gate keeps trying to heal it.
	fencePending map[string]uint64
	cancel       context.CancelFunc
	wg           sync.WaitGroup

	started atomic.Bool
}

// publisherHolder resolves the chicken-and-egg between the
// controller (which needs the publisher as its reader) and the
// publisher (which needs the controller as its directive).
// Construction order: create holder, create controller(holder),
// Bootstrap(dir, controller) builds publisher, set holder=publisher.
// The controller sees an empty reader until Bootstrap returns;
// that's fine because no rebuild runs until host.Start().
type publisherHolder struct {
	mu sync.Mutex
	p  *authority.Publisher
}

func (h *publisherHolder) set(p *authority.Publisher) {
	h.mu.Lock()
	h.p = p
	h.mu.Unlock()
}

func (h *publisherHolder) LastAuthorityBasis(vid, rid string) (authority.AuthorityBasis, bool) {
	h.mu.Lock()
	p := h.p
	h.mu.Unlock()
	if p == nil {
		return authority.AuthorityBasis{}, false
	}
	return p.LastAuthorityBasis(vid, rid)
}

func (h *publisherHolder) VolumeAuthorityLine(vid string) (authority.AuthorityBasis, bool) {
	h.mu.Lock()
	p := h.p
	h.mu.Unlock()
	if p == nil {
		return authority.AuthorityBasis{}, false
	}
	return p.VolumeAuthorityLine(vid)
}

// New constructs a master host but does NOT start live loops. The
// caller composes it, then calls Start.
//
// Startup order matches T0 sketch §5 + S7 restart pin:
//  1. acquire durable store lock + open store (via sparrow.Bootstrap)
//  2. NewPublisher(WithStore) — synchronous reload (inside Bootstrap)
//  3. controller sees reloaded publisher (via holder back-fill)
//  4. NewObservationHost with reloaded publisher; controller as sink
//  5. bind gRPC listener, register services
//
// Live loops (publisher.Run, host.Start, grpc.Serve) run in Start.
func New(cfg Config) (*Host, error) {
	if cfg.WorkspaceMaxClockSkew < 0 || cfg.WorkspaceMaxClockSkew >= 60*time.Second {
		return nil, fmt.Errorf("master.New: workspace max clock skew must be positive and less than 60s (zero selects default)")
	}
	if cfg.WorkspaceRPC && (cfg.WorkspaceHeadReader == nil || cfg.LifecycleStoreDir == "") {
		return nil, fmt.Errorf("master.New: workspace RPC requires HEAD reader and lifecycle store")
	}
	if cfg.AuthorityStoreDir == "" {
		return nil, fmt.Errorf("master.New: AuthorityStoreDir is required")
	}
	if cfg.Listen == "" {
		return nil, fmt.Errorf("master.New: Listen is required")
	}
	if err := ValidateStatusListen(cfg.StatusListen); err != nil {
		return nil, fmt.Errorf("master.New: %w", err)
	}
	lg := cfg.Logger
	if lg == nil {
		lg = log.Default()
	}

	holder := &publisherHolder{}
	ctrlCfg := cfg.ControllerConfig
	if w := cfg.Freshness.EffectiveFreshnessWindow(); ctrlCfg.PrimaryBootGrace <= 0 || ctrlCfg.PrimaryBootGrace > w {
		// A restarted primary may stay not-ready for at most the
		// window in which its observations are trusted (#128): the
		// store's effective window, also when the config omits it. A
		// larger configured grace is capped, never widened; an unset
		// or nonpositive one takes the window.
		if ctrlCfg.PrimaryBootGrace > w {
			lg.Printf("blockmaster: primary boot grace %s capped at freshness window %s", ctrlCfg.PrimaryBootGrace, w)
		}
		ctrlCfg.PrimaryBootGrace = w
	}
	ctrl := authority.NewTopologyController(ctrlCfg, holder)

	h := &Host{
		probeHub: newProbeHub(),
		cfg:      cfg,
		log:      lg,
		ctrl:     ctrl,
		topo:     cfg.Topology,
		events:   newEventRing(1024),
		journal:  newJournalIsolation(storeident.Identity{}), // sampled once the store exists
	}

	boot, err := bootstrap.BootstrapWithOptions(cfg.AuthorityStoreDir, ctrl, bootstrap.Options{
		WorkspaceMaxClockSkew: cfg.WorkspaceMaxClockSkew,
		PublishObserver:       h.recordAuthorityPublishedEvent,
		PublicationAdmission:  h.admitWorkspacePublication,
	})
	if err != nil {
		return nil, fmt.Errorf("master.New: Bootstrap: %w", err)
	}
	holder.set(boot.Publisher)
	// Bootstrap creates a fresh store directory: sample its file system only
	// now, or a first start would compare against an unknown device forever.
	// An unknown identity (stat error, unsupported platform) never matches.
	h.journal.setSelf(storeident.Of(cfg.AuthorityStoreDir))
	if h.attest, err = loadStoreAttestations(cfg.AuthorityStoreDir); err != nil {
		return nil, fmt.Errorf("master.New: %w", err)
	}

	var sink authority.ControllerSink = ctrl
	if len(cfg.Topology.Volumes) == 0 {
		// Dynamic lifecycle volumes derive their accepted topology from
		// placement intents in RunLifecycleProductTick. With no static
		// topology configured, the observation host should collect facts
		// but must not submit "unknown volume" reports that would clear
		// lifecycle-driven desired authority.
		sink = nil
	}
	obs := authority.NewObservationHost(authority.ObservationHostConfig{
		Freshness: cfg.Freshness,
		Topology:  cfg.Topology,
		Sink:      sink,
		Reader:    boot.Publisher,
	})
	// An ask built outside the controller (a heal, a planned or workspace
	// move, an operator's request) is issued for the process its target
	// reports now.
	boot.Publisher.SetIncarnationSource(func(volumeID, replicaID, dataAddr, ctrlAddr string) string {
		if f, ok := obs.Store().SlotFactAt(volumeID, replicaID, dataAddr, ctrlAddr); ok {
			return f.Incarnation
		}
		return ""
	})

	lifecycleStores, err := openLifecycleStores(cfg.LifecycleStoreDir)
	if err != nil {
		_ = boot.Close()
		return nil, fmt.Errorf("master.New: lifecycle stores: %w", err)
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		_ = boot.Close()
		return nil, fmt.Errorf("master.New: listen %q: %w", cfg.Listen, err)
	}

	h.boot = boot
	h.obs = obs
	h.ln = ln
	h.lifecycle = lifecycleStores
	if h.lifecycle != nil {
		// Rebuild permanent grant exclusions before any request, publisher
		// loop or lifecycle reconciliation can create positive validity.
		for _, volume := range h.lifecycle.Deletions.CommittedVolumes() {
			if err := h.Publisher().ReserveWorkspaceDeletion(context.Background(), volume, nil); err != nil {
				_ = ln.Close()
				_ = boot.Close()
				return nil, fmt.Errorf("master.New: deletion reservation recovery: %w", err)
			}
		}
		if err := h.reconcileWorkspaceBindings(); err != nil {
			_ = ln.Close()
			_ = boot.Close()
			return nil, fmt.Errorf("master.New: workspace binding recovery: %w", err)
		}
	}

	if cfg.FrontendPublicationRuntimeHTTP {
		addr := cfg.FrontendPublicationRuntimeListen
		if addr == "" {
			addr = "127.0.0.1:0"
		}
		frontendLn, err := net.Listen("tcp", addr)
		if err != nil {
			_ = ln.Close()
			_ = boot.Close()
			return nil, fmt.Errorf("master.New: frontend publication runtime listen %q: %w", addr, err)
		}
		h.frontendRuntimeLn = frontendLn
		h.frontendRuntimeHTTP = &http.Server{Handler: h.frontendPublicationRuntimeHandler()}
	}

	if cfg.StatusListen != "" {
		statusLn, err := net.Listen("tcp", cfg.StatusListen)
		if err != nil {
			if h.frontendRuntimeLn != nil {
				_ = h.frontendRuntimeLn.Close()
			}
			_ = ln.Close()
			_ = boot.Close()
			return nil, fmt.Errorf("master.New: status listen %q: %w", cfg.StatusListen, err)
		}
		h.statusLn = statusLn
		// Every connection phase is bounded: a client that withholds a
		// declared body or never reads the reply is cut off.
		h.statusHTTP = &http.Server{Handler: h.statusHandler(), ReadHeaderTimeout: statusReadTimeout, ReadTimeout: statusReadTimeout,
			WriteTimeout: statusWriteTimeout, IdleTimeout: statusIdleTimeout}
	}

	grpcSrv := link.NewServer(serverAuth(cfg.Auth), cfg.ServerOptions...)
	svc := newServices(h)
	control.RegisterObservationServiceServer(grpcSrv, svc)
	control.RegisterAssignmentServiceServer(grpcSrv, svc)
	control.RegisterEvidenceServiceServer(grpcSrv, svc)
	control.RegisterClusterEvidenceServiceServer(grpcSrv, svc)
	control.RegisterLifecycleServiceServer(grpcSrv, svc)
	control.RegisterFailbackServiceServer(grpcSrv, svc)
	control.RegisterWorkspaceServiceServer(grpcSrv, svc)
	h.grpc = grpcSrv

	lg.Printf("blockmaster: lock acquired, reloaded=%d, listen=%s",
		boot.ReloadedRecords, ln.Addr().String())
	if h.frontendRuntimeLn != nil {
		lg.Printf("blockmaster: frontend publication runtime listen=%s", h.frontendRuntimeLn.Addr().String())
	}
	if h.statusLn != nil {
		lg.Printf("blockmaster: status listen=%s", h.statusLn.Addr().String())
	}
	if cfg.StrictJournalIsolation {
		lg.Printf("blockmaster: strict journal isolation: nodes sharing the authority store's file system are refused new work")
	}
	for _, e := range boot.ReloadSkips {
		lg.Printf("blockmaster: reload skip: %v", e)
	}
	return h, nil
}

func openLifecycleStores(dir string) (*LifecycleStores, error) {
	if dir == "" {
		return nil, nil
	}
	volumes, err := lifecycle.OpenFileStore(filepath.Join(dir, "volumes"))
	if err != nil {
		return nil, err
	}
	nodes, err := lifecycle.OpenNodeInventoryStore(filepath.Join(dir, "nodes"))
	if err != nil {
		return nil, err
	}
	placements, err := lifecycle.OpenPlacementIntentStore(filepath.Join(dir, "placements"))
	if err != nil {
		return nil, err
	}
	bindings, err := lifecycle.OpenWorkspaceBindingStore(filepath.Join(dir, "workspace-bindings"))
	if err != nil {
		return nil, err
	}
	deletions, err := lifecycle.OpenDeletionReservationStore(filepath.Join(dir, "deletion-reservations"))
	if err != nil {
		return nil, err
	}
	return &LifecycleStores{
		Volumes:           volumes,
		Nodes:             nodes,
		Placements:        placements,
		WorkspaceBindings: bindings,
		Deletions:         deletions,
	}, nil
}

// Addr returns the bound listener address. Valid after New.
func (h *Host) Addr() string { return h.ln.Addr().String() }

// FrontendPublicationRuntimeAddr returns the bound frontend publication
// runtime HTTP address, or empty when the runtime is disabled.
func (h *Host) FrontendPublicationRuntimeAddr() string {
	if h.frontendRuntimeLn == nil {
		return ""
	}
	return h.frontendRuntimeLn.Addr().String()
}

// StatusAddr returns the bound status HTTP address, or empty when the status
// endpoint is disabled.
func (h *Host) StatusAddr() string {
	if h.statusLn == nil {
		return ""
	}
	return h.statusLn.Addr().String()
}

// Publisher exposes the reloaded publisher for tests and for the
// evidence-query path.
func (h *Host) Publisher() *authority.Publisher {
	if h == nil || h.boot == nil {
		return nil
	}
	return h.boot.Publisher
}

// FailbackAuthorityRuntime exposes the authority-owned failback seam backed by
// the live Publisher. Callers still need an explicit policy gate before using
// it; constructing the runtime does not execute failback.
func (h *Host) FailbackAuthorityRuntime() authority.FailbackAuthorityRuntime {
	return authority.FailbackAuthorityRuntime{Publisher: h.Publisher(), Gate: h.plannedMoveEvidenceAt, TargetEndpoint: h.replicaEndpoint}
}

// replicaEndpoint is the master's own record of a replica's addresses: its
// fresh heartbeat slot (what the automatic path publishes), else its
// placement slot when that names both.
func (h *Host) replicaEndpoint(volumeID, replicaID string) (string, string, bool) {
	if s, ok := h.freshCurrentSlot(volumeID, replicaID); ok && s.DataAddr != "" && s.CtrlAddr != "" {
		return s.DataAddr, s.CtrlAddr, true
	}
	if h.lifecycle != nil {
		if p, ok := h.lifecycle.Placements.GetPlacement(volumeID); ok {
			for _, s := range p.Slots {
				if s.ReplicaID == replicaID && s.DataAddr != "" && s.CtrlAddr != "" {
					return s.DataAddr, s.CtrlAddr, true
				}
			}
		}
	}
	return "", "", false
}

// Controller exposes the controller for tests.
func (h *Host) Controller() *authority.TopologyController { return h.ctrl }

// ObservationHost exposes the observation host for tests.
func (h *Host) ObservationHost() *authority.ObservationHost { return h.obs }

// Lifecycle returns product registration stores when configured. Nil means
// the daemon is running in static-topology-only mode. Reading these stores
// must not be treated as assignment authority.
func (h *Host) Lifecycle() *LifecycleStores { return h.lifecycle }

// replicaSlotsFor returns replica IDs for a volume. Static topology
// remains the allow-list. Dynamic lifecycle volumes may not persist
// placement slots in the lifecycle record, so read-only status paths
// merge fresh observed slots from the observation store; assignment
// authority still comes from the publisher/controller, not this helper.
func (h *Host) replicaSlotsFor(volumeID string) []string {
	for _, v := range h.topo.Volumes {
		if v.VolumeID != volumeID {
			continue
		}
		out := make([]string, 0, len(v.Slots))
		for _, s := range v.Slots {
			out = append(out, s.ReplicaID)
		}
		return out
	}
	var out []string
	if h.lifecycle != nil && h.lifecycle.Placements != nil {
		if placement, ok := h.lifecycle.Placements.GetPlacement(volumeID); ok {
			for _, slot := range placement.Slots {
				if slot.ReplicaID != "" {
					out = appendReplicaID(out, slot.ReplicaID)
				}
			}
		}
	}
	if h.lifecycle != nil && h.obs != nil {
		for _, replicaID := range h.obs.Store().ReplicaIDsForVolume(volumeID) {
			out = appendReplicaID(out, replicaID)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func appendReplicaID(in []string, replicaID string) []string {
	for _, existing := range in {
		if existing == replicaID {
			return in
		}
	}
	return append(in, replicaID)
}

// Start runs the publisher, observation rebuild loop, and gRPC
// server. Returns a context that is cancelled on Close so callers
// can wait on it from their own goroutines. Idempotent — second
// Start() returns the prior ctx.
func (h *Host) Start() context.Context {
	if !h.started.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		_ = h.boot.Publisher.Run(ctx)
	}()

	h.obs.Start(ctx)

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		if err := h.grpc.Serve(h.ln); err != nil && err != grpc.ErrServerStopped {
			h.log.Printf("blockmaster: grpc.Serve: %v", err)
		}
	}()

	if h.frontendRuntimeHTTP != nil && h.frontendRuntimeLn != nil {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			if err := h.frontendRuntimeHTTP.Serve(h.frontendRuntimeLn); err != nil && err != http.ErrServerClosed {
				h.log.Printf("blockmaster: frontend publication runtime Serve: %v", err)
			}
		}()
	}

	if h.statusHTTP != nil && h.statusLn != nil {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			if err := h.statusHTTP.Serve(h.statusLn); err != nil && err != http.ErrServerClosed {
				h.log.Printf("blockmaster: status Serve: %v", err)
			}
		}()
	}

	return ctx
}

// Close stops the gRPC server, cancels live loops, and releases
// the durable store lock. Idempotent.
func (h *Host) Close(ctx context.Context) error {
	if h.cancel != nil {
		h.cancel()
	}
	if h.frontendRuntimeHTTP != nil {
		_ = h.frontendRuntimeHTTP.Shutdown(ctx)
	}
	var statusErr error
	if h.statusHTTP != nil {
		statusErr = h.closeStatusHTTP(ctx)
	}
	h.obs.Stop()

	done := make(chan struct{})
	go func() {
		h.grpc.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		h.grpc.Stop()
	}

	h.wg.Wait()
	// Join lease extenders (authority writers) before the store closes.
	if h.boot != nil && h.boot.Publisher != nil {
		h.boot.Publisher.CloseWorkspaceExtenders()
	}
	return errors.Join(statusErr, h.boot.Close())
}

// Status endpoint connection bounds.
const (
	statusReadTimeout  = 5 * time.Second
	statusWriteTimeout = 5 * time.Second
	statusIdleTimeout  = 30 * time.Second
)

// closeStatusHTTP shuts the status endpoint down gracefully until ctx ends,
// then force-closes the connections still open (Shutdown leaves them). It
// returns the force close's error, if any; an expired graceful shutdown that
// the force close completed is only logged.
func (h *Host) closeStatusHTTP(ctx context.Context) error {
	defer h.statusLn.Close() // a listener Start never served is not Shutdown's
	err := h.statusHTTP.Shutdown(ctx)
	if err == nil {
		return nil
	}
	h.log.Printf("blockmaster: status graceful shutdown: %v; closing its open connections", err)
	if cerr := h.statusHTTP.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
		return fmt.Errorf("status endpoint close: %w", cerr)
	}
	return nil
}
