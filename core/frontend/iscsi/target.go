package iscsi

// iSCSI target — TCP listener + accept loop.
//
// One Target serves one volume (one SCSIHandler bound to one
// frontend.Backend obtained from a frontend.Provider). That's
// sufficient for T2's per-volume model: the volume host owns
// one Target; cmd/blockvolume will instantiate the Target for
// the volume it serves.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// TargetConfig configures a Target.
type TargetConfig struct {
	// Listen is the TCP address the Target binds on.
	// ":0" picks an OS-assigned port (useful for in-process tests).
	Listen string

	// IQN is the iSCSI Qualified Name advertised to initiators.
	// Currently informational — T2 minimal login does not parse
	// TargetName from login params. Will be used when login
	// negotiation expands.
	IQN string

	// VolumeID is handed to Provider.Open.
	VolumeID string

	// Provider supplies the frontend.Backend each session uses.
	Provider frontend.Provider

	// Backend handler config (block size, volume size, INQUIRY
	// vendor/product strings). Optional — zero values get the
	// T2 defaults from HandlerConfig.
	Handler HandlerConfig

	// Logger. Nil → log.Default wrapped for the session layer.
	Logger *log.Logger
}

// Target is a TCP-listening iSCSI target.
type Target struct {
	cfg TargetConfig

	mu       sync.Mutex
	ln       net.Listener
	sessions sync.WaitGroup
	logger   Logger
	closed   chan struct{}
}

// NewTarget builds a Target. Call Start to begin serving.
func NewTarget(cfg TargetConfig) *Target {
	if cfg.Provider == nil {
		panic("iscsi: NewTarget: Provider required")
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

// Start binds and spawns the accept loop in a goroutine.
// Returns the bound addr (useful when Listen is ":0").
func (t *Target) Start() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ln != nil {
		return "", fmt.Errorf("iscsi: Target already started")
	}
	ln, err := net.Listen("tcp", t.cfg.Listen)
	if err != nil {
		return "", fmt.Errorf("iscsi: listen %q: %w", t.cfg.Listen, err)
	}
	t.ln = ln
	go t.acceptLoop(ln)
	return ln.Addr().String(), nil
}

// Close stops the accept loop and waits for all active sessions
// to drain. Idempotent.
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
				t.logger.Printf("iscsi: accept: %v", err)
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

	// Close session when target is closing.
	go func() {
		select {
		case <-t.closed:
			_ = conn.Close()
		case <-ctx.Done():
		}
	}()

	// Open a backend via the frontend.Provider. The Provider
	// blocks until the projection is healthy or returns
	// ErrNotReady. We pass our own ctx so target-shutdown
	// cancels the wait.
	backend, err := t.cfg.Provider.Open(ctx, t.cfg.VolumeID)
	if err != nil {
		t.logger.Printf("iscsi: Provider.Open(%s): %v", t.cfg.VolumeID, err)
		return
	}
	defer backend.Close()

	cfg := t.cfg.Handler
	cfg.Backend = backend
	handler := NewSCSIHandler(cfg)

	sess := newSession(conn, handler, t.logger)
	if err := sess.serve(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
		t.logger.Printf("iscsi: session error (%s): %v", conn.RemoteAddr(), err)
	}
}

// stdlogAdapter lets us plug a *log.Logger into the tiny Logger
// interface the session layer takes.
type stdlogAdapter struct{ l *log.Logger }

func (a stdlogAdapter) Printf(format string, args ...interface{}) {
	a.l.Printf(format, args...)
}
