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
	return &Target{
		cfg:    cfg,
		logger: stdlogAdapter{l: lg},
		closed: make(chan struct{}),
	}
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
	return nil
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-t.closed:
			_ = conn.Close()
		case <-ctx.Done():
		}
	}()

	backend, err := t.cfg.Provider.Open(ctx, t.cfg.VolumeID)
	if err != nil {
		t.logger.Printf("nvme: Provider.Open(%s): %v", t.cfg.VolumeID, err)
		return
	}
	defer backend.Close()

	hcfg := t.cfg.Handler
	hcfg.Backend = backend
	handler := NewIOHandler(hcfg)

	sess := newSession(conn, handler, t.logger)
	if err := sess.serve(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
		t.logger.Printf("nvme: session error (%s): %v", conn.RemoteAddr(), err)
	}
}

type stdlogAdapter struct{ l *log.Logger }

func (a stdlogAdapter) Printf(format string, args ...interface{}) {
	a.l.Printf(format, args...)
}
