package nvme

// NVMe/TCP target — TCP listener + accept loop.
//
// Symmetric with core/frontend/iscsi/target.go: one Target per
// volume, opens a frontend.Backend per session via the supplied
// frontend.Provider.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// ProbeBackendProvider supplies a borrowed backend for metadata-only ANA path
// probing when the write-ready Provider.Open path is not available.
type ProbeBackendProvider interface {
	ProbeBackend(ctx context.Context, volumeID string) (frontend.Backend, error)
}

// TargetConfig configures an NVMe/TCP Target.
type TargetConfig struct {
	// Listen TCP address (":0" for tests).
	Listen string

	// SubsysNQN is the subsystem NVMe Qualified Name advertised
	// to hosts. Currently informational — T2 minimal Connect
	// accepts any SubNQN; will be enforced when discovery / multi-
	// subsystem support lands.
	SubsysNQN string

	// VolumeID handed to Provider.Open.
	VolumeID string

	// Provider supplies the frontend.Backend per session.
	Provider frontend.Provider

	// ProbeProvider is optional. If Provider.Open returns ErrNotReady and ANA
	// reports a non-optimized path, the target uses this backend for admin
	// connect / Identify / ANA log visibility. Data commands remain rejected
	// by a metadata-only IOHandler.
	ProbeProvider ProbeBackendProvider

	// ControllerID is the first CNTLID allocated by this target. Zero uses 1.
	// Multi-path deployments must configure distinct values per target serving
	// the same SubsysNQN; Linux rejects duplicate controller IDs inside one
	// subsystem.
	ControllerID uint16

	// IO handler tunables (block size, volume size, NSID).
	// Zero values pick T2 defaults.
	Handler HandlerConfig

	// Logger (nil → log.Default).
	Logger *log.Logger
}

// Target is a TCP-listening NVMe/TCP target.
type Target struct {
	cfg TargetConfig

	mu       sync.Mutex
	ln       net.Listener
	sessions sync.WaitGroup
	logger   Logger
	closed   chan struct{}

	// Admin controller registry — populated on admin-queue
	// Connect (§3.1 A10.5 + R3), looked up on IO-queue Connect
	// to validate the host's CNTLID claim.
	ctrlMu     sync.Mutex
	ctrls      map[uint16]*adminController
	nextCntlID uint16 // monotonic allocator; never reuses a CNTLID within a Target lifetime

	stats targetStats
}

// NewTarget builds a Target. Provider must be non-nil.
func NewTarget(cfg TargetConfig) *Target {
	if cfg.Provider == nil {
		panic("nvme: NewTarget: Provider required")
	}
	lg := cfg.Logger
	if lg == nil {
		lg = log.Default()
	}
	nextCntlID := cfg.ControllerID
	if nextCntlID == 0 || nextCntlID == 0xffff {
		nextCntlID = 1
	}
	return &Target{
		cfg:        cfg,
		logger:     stdlogAdapter{l: lg},
		closed:     make(chan struct{}),
		ctrls:      map[uint16]*adminController{},
		nextCntlID: nextCntlID, // 0 is reserved; 0xFFFF is "request new" on Connect.
	}
}

// allocAdminController allocates a fresh CNTLID and registers
// an admin controller for it. Caller is the admin-queue
// Connect handler. Returns the new controller with register
// state initialized per NVMe 1.3 boot semantics.
func (t *Target) allocAdminController(subNQN, hostNQN, volumeID string) *adminController {
	t.ctrlMu.Lock()
	defer t.ctrlMu.Unlock()
	id := t.nextCntlID
	t.nextCntlID++
	ctrl := newAdminController(id, subNQN, hostNQN, volumeID)
	t.ctrls[id] = ctrl
	return ctrl
}

// lookupAdminController is called by IO-queue Connect to
// validate the host's CNTLID claim. Returns nil if no such
// controller exists.
func (t *Target) lookupAdminController(id uint16) *adminController {
	t.ctrlMu.Lock()
	defer t.ctrlMu.Unlock()
	return t.ctrls[id]
}

// releaseAdminController removes a controller from the registry
// when its admin session closes. IO queue sessions that outlive
// their admin session are expected to fail subsequent IO with
// the session's existing ctrl reference; they do not re-validate
// against the registry per-command (performance + race-window
// tradeoff documented here for the T3 review).
func (t *Target) releaseAdminController(id uint16) {
	t.ctrlMu.Lock()
	defer t.ctrlMu.Unlock()
	delete(t.ctrls, id)
}

// Start binds and spawns the accept loop. Returns the bound addr.
func (t *Target) Start() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ln != nil {
		return "", fmt.Errorf("nvme: Target already started")
	}
	ln, err := net.Listen("tcp", t.cfg.Listen)
	if err != nil {
		return "", fmt.Errorf("nvme: listen %q: %w", t.cfg.Listen, err)
	}
	t.ln = ln
	go t.acceptLoop(ln)
	return ln.Addr().String(), nil
}

// Close stops the accept loop and drains active sessions.
func (t *Target) Close() error {
	t.mu.Lock()
	select {
	case <-t.closed:
		t.mu.Unlock()
		return nil
	default:
		close(t.closed)
	}
	ln := t.ln
	t.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	t.sessions.Wait()
	st := t.Stats()
	t.logger.Printf("nvme: stats sessions=%d admin_connects=%d io_connects=%d reads=%d writes=%d flushes=%d inline_writes=%d inline_bytes=%d r2t_writes=%d r2t_bytes=%d h2c_pdus=%d h2c_bytes=%d c2h_pdus=%d c2h_bytes=%d",
		st.SessionsAccepted, st.AdminConnects, st.IOConnects,
		st.ReadCommands, st.WriteCommands, st.FlushCommands,
		st.InlineWriteCommands, st.InlineWriteBytes,
		st.R2TWriteCommands, st.R2TWriteBytes,
		st.H2CDataPDUs, st.H2CDataBytes,
		st.C2HDataPDUs, st.C2HDataBytes)
	return nil
}

// Stats returns a point-in-time transport counter snapshot.
func (t *Target) Stats() Stats {
	if t == nil {
		return Stats{}
	}
	return t.stats.snapshot()
}

func (t *Target) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-t.closed:
				return
			default:
				t.logger.Printf("nvme: accept: %v", err)
				return
			}
		}
		t.sessions.Add(1)
		go t.handleConn(conn)
	}
}

func (t *Target) handleConn(conn net.Conn) {
	defer t.sessions.Done()
	defer conn.Close()
	t.stats.sessionsAccepted.Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-t.closed:
			_ = conn.Close()
		case <-ctx.Done():
		}
	}()

	metadataOnly := false
	backend, err := t.cfg.Provider.Open(ctx, t.cfg.VolumeID)
	if err != nil {
		var ok bool
		backend, metadataOnly, ok = t.tryProbeBackend(ctx, err)
		if !ok {
			t.logger.Printf("nvme: Provider.Open(%s): %v", t.cfg.VolumeID, err)
			return
		}
	}
	// BUG-005 fix (2026-04-22): do NOT close the Backend here.
	// The Provider owns Backend lifecycle — `DurableProvider`
	// caches one Backend per volumeID so multiple sessions share
	// the underlying LogicalStorage handle. Closing from the
	// per-session path would mark the cached Backend closed,
	// breaking the next session's I/O with ErrBackendClosed.
	// See sw-block/design/bugs/005_backend_close_cross_session.md.

	hcfg := t.cfg.Handler
	hcfg.Backend = backend
	hcfg.MetadataOnly = metadataOnly
	handler := NewIOHandler(hcfg)

	sess := newSession(conn, handler, t, t.cfg.SubsysNQN, t.logger)
	if err := sess.serve(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
		t.logger.Printf("nvme: session error (%s): %v", conn.RemoteAddr(), err)
	}
}

func (t *Target) tryProbeBackend(ctx context.Context, openErr error) (frontend.Backend, bool, bool) {
	if t.cfg.ProbeProvider == nil || !errors.Is(openErr, frontend.ErrNotReady) {
		return nil, false, false
	}
	ana := t.cfg.Handler.ANA
	if ana == nil {
		return nil, false, false
	}
	switch ana.ANAState() {
	case ANAOptimized, ANAInaccessible:
		return nil, false, false
	}
	backend, err := t.cfg.ProbeProvider.ProbeBackend(ctx, t.cfg.VolumeID)
	if err != nil {
		t.logger.Printf("nvme: ProbeBackend(%s): %v", t.cfg.VolumeID, err)
		return nil, false, false
	}
	t.logger.Printf("nvme: Provider.Open(%s): %v; using ANA metadata probe backend", t.cfg.VolumeID, openErr)
	return backend, true, true
}

type stdlogAdapter struct{ l *log.Logger }

func (a stdlogAdapter) Printf(format string, args ...interface{}) {
	a.l.Printf(format, args...)
}
