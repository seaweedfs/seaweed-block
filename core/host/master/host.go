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
	cfg       Config
	log       *log.Logger
	boot      *bootstrap.DurableAuthorityBootstrap
	ctrl      *authority.TopologyController
	obs       *authority.ObservationHost
	ln        net.Listener
	grpc      *grpc.Server
	topo      authority.AcceptedTopology
	lifecycle *LifecycleStores
	cancel    context.CancelFunc
	wg        sync.WaitGroup

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

	boot, err := bootstrap.Bootstrap(cfg.AuthorityStoreDir, ctrl)
	if err != nil {
		return nil, fmt.Errorf("master.New: Bootstrap: %w", err)
	}
	holder.set(boot.Publisher)

	obs := authority.NewObservationHost(authority.ObservationHostConfig{
		Freshness: cfg.Freshness,
		Topology:  cfg.Topology,
		Sink:      ctrl,
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

	h := &Host{
		cfg:       cfg,
		log:       lg,
		boot:      boot,
		ctrl:      ctrl,
		obs:       obs,
		ln:        ln,
		topo:      cfg.Topology,
		lifecycle: lifecycleStores,
	}

	grpcSrv := grpc.NewServer()
	svc := newServices(h)
	control.RegisterObservationServiceServer(grpcSrv, svc)
	control.RegisterAssignmentServiceServer(grpcSrv, svc)
	control.RegisterEvidenceServiceServer(grpcSrv, svc)
	h.grpc = grpcSrv

	lg.Printf("blockmaster: lock acquired, reloaded=%d, listen=%s",
		boot.ReloadedRecords, ln.Addr().String())
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

// Publisher exposes the reloaded publisher for tests and for the
// evidence-query path.
func (h *Host) Publisher() *authority.Publisher { return h.boot.Publisher }

// Controller exposes the controller for tests.
func (h *Host) Controller() *authority.TopologyController { return h.ctrl }

// ObservationHost exposes the observation host for tests.
func (h *Host) ObservationHost() *authority.ObservationHost { return h.obs }

// Lifecycle returns product registration stores when configured. Nil means
// the daemon is running in static-topology-only mode. Reading these stores
// must not be treated as assignment authority.
func (h *Host) Lifecycle() *LifecycleStores { return h.lifecycle }

// replicaSlotsFor returns the accepted-topology replica IDs for a
// volume, or nil if the volume isn't in accepted topology. Used
// by AssignmentService to fan-in per-(vol, rid) publisher
// subscriptions for volume-scoped delivery.
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
	return nil
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

	return ctx
}

// Close stops the gRPC server, cancels live loops, and releases
// the durable store lock. Idempotent.
func (h *Host) Close(ctx context.Context) error {
	if h.cancel != nil {
		h.cancel()
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
