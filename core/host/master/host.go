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
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/bootstrap"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

// Config holds the master host construction inputs. Flag parsing
// lives in cmd/blockmaster; this type is the testable seam.
type Config struct {
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

	// LifecycleStoreDir, when non-empty, opens product lifecycle
	// registration stores (desired volumes, node inventory, placement
	// intents). This is read-only composition for G9D: these stores do
	// NOT drive assignment publication.
	LifecycleStoreDir string

	// FailbackRuntimeRPC enables the authority failback RPC surface. It is
	// disabled by default; callers must still provide expected-current and
	// terminal evidence when enabled.
	FailbackRuntimeRPC bool

	// FrontendPublicationRuntimeHTTP enables the frontend publication runtime
	// HTTP surface. It is disabled by default and only confirms a post-failback
	// authority line that has already been minted by the failback owner.
	FrontendPublicationRuntimeHTTP   bool
	FrontendPublicationRuntimeListen string

	// Logger is used for structured startup and error logging. If
	// nil, the default log package logger is used.
	Logger *log.Logger
}

// LifecycleStores groups the product registration stores hosted by
// blockmaster. The stores are controller input only; assignment
// publication remains owned by authority.Controller/Publisher.
type LifecycleStores struct {
	Volumes    *lifecycle.FileStore
	Nodes      *lifecycle.NodeInventoryStore
	Placements *lifecycle.PlacementIntentStore
}

// Host is the composed master-side block product daemon. Lifecycle
// is: New -> Start -> (serve requests) -> Close.
type Host struct {
	cfg                 Config
	log                 *log.Logger
	boot                *bootstrap.DurableAuthorityBootstrap
	ctrl                *authority.TopologyController
	obs                 *authority.ObservationHost
	ln                  net.Listener
	frontendRuntimeLn   net.Listener
	frontendRuntimeHTTP *http.Server
	grpc                *grpc.Server
	topo                authority.AcceptedTopology
	lifecycle           *LifecycleStores
	events              *eventRing
	promotionMu         sync.RWMutex
	promotionProber     PromotionEvidenceProvider
	cancel              context.CancelFunc
	wg                  sync.WaitGroup

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
	if cfg.AuthorityStoreDir == "" {
		return nil, fmt.Errorf("master.New: AuthorityStoreDir is required")
	}
	if cfg.Listen == "" {
		return nil, fmt.Errorf("master.New: Listen is required")
	}
	lg := cfg.Logger
	if lg == nil {
		lg = log.Default()
	}

	holder := &publisherHolder{}
	ctrl := authority.NewTopologyController(cfg.ControllerConfig, holder)

	h := &Host{
		cfg:    cfg,
		log:    lg,
		ctrl:   ctrl,
		topo:   cfg.Topology,
		events: newEventRing(1024),
	}

	boot, err := bootstrap.BootstrapWithOptions(cfg.AuthorityStoreDir, ctrl, bootstrap.Options{
		PublishObserver: h.recordAuthorityPublishedEvent,
	})
	if err != nil {
		return nil, fmt.Errorf("master.New: Bootstrap: %w", err)
	}
	holder.set(boot.Publisher)

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

	grpcSrv := grpc.NewServer()
	svc := newServices(h)
	control.RegisterObservationServiceServer(grpcSrv, svc)
	control.RegisterAssignmentServiceServer(grpcSrv, svc)
	control.RegisterEvidenceServiceServer(grpcSrv, svc)
	control.RegisterClusterEvidenceServiceServer(grpcSrv, svc)
	control.RegisterLifecycleServiceServer(grpcSrv, svc)
	control.RegisterFailbackServiceServer(grpcSrv, svc)
	h.grpc = grpcSrv

	lg.Printf("blockmaster: lock acquired, reloaded=%d, listen=%s",
		boot.ReloadedRecords, ln.Addr().String())
	if h.frontendRuntimeLn != nil {
		lg.Printf("blockmaster: frontend publication runtime listen=%s", h.frontendRuntimeLn.Addr().String())
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
	return &LifecycleStores{
		Volumes:    volumes,
		Nodes:      nodes,
		Placements: placements,
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

// Publisher exposes the reloaded publisher for tests and for the
// evidence-query path.
func (h *Host) Publisher() *authority.Publisher { return h.boot.Publisher }

// FailbackAuthorityRuntime exposes the authority-owned failback seam backed by
// the live Publisher. Callers still need an explicit policy gate before using
// it; constructing the runtime does not execute failback.
func (h *Host) FailbackAuthorityRuntime() authority.FailbackAuthorityRuntime {
	return authority.FailbackAuthorityRuntime{Publisher: h.Publisher()}
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
	return h.boot.Close()
}
