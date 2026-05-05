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
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// TargetConfig configures a Target.
type TargetConfig struct {
	// Listen is the TCP address the Target binds on.
	// ":0" picks an OS-assigned port (useful for in-process tests).
	Listen string

	// IQN is the iSCSI Qualified Name advertised to initiators.
	// Used as the SendTargets discovery list entry AND as the
	// expected TargetName during Normal-session login.
	IQN string

	// PortalAddr is what we advertise in SendTargets responses
	// (defaults to the actual bound listen address). Operators
	// override when listen is 0.0.0.0/[::] but clients need a
	// routable IP. Format: "host:port,portal-group-tag".
	PortalAddr string

	// VolumeID is handed to Provider.Open.
	VolumeID string

	// Provider supplies the frontend.Backend each session uses.
	Provider frontend.Provider

	// Backend handler config (block size, volume size, INQUIRY
	// vendor/product strings). Optional — zero values get the
	// T2 defaults from HandlerConfig.
	Handler HandlerConfig

	// Negotiation profile for login parameter exchange. Zero
	// value = DefaultNegotiableConfig().
	Negotiation NegotiableConfig

	// DataOutTimeout bounds how long a session waits for
	// R2T-solicited Data-Out after issuing R2T. Zero picks the
	// default. This is a target-local resource protection knob, not
	// an iSCSI login-negotiated value.
	DataOutTimeout time.Duration

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
	if cfg.Negotiation.isZeroExceptCHAP() {
		chap := cfg.Negotiation.CHAP
		cfg.Negotiation = DefaultNegotiableConfig()
		cfg.Negotiation.CHAP = chap
	}
	if cfg.DataOutTimeout == 0 {
		cfg.DataOutTimeout = 30 * time.Second
	}
	return &Target{
		cfg:    cfg,
		logger: stdlogAdapter{l: lg},
		closed: make(chan struct{}),
	}
}

// HasTarget satisfies TargetResolver: accepts the configured
// IQN. Future multi-target support extends this to a registry.
func (t *Target) HasTarget(name string) bool {
	return t.cfg.IQN != "" && name == t.cfg.IQN
}

// ListTargets satisfies TargetLister for SendTargets discovery.
// Single-target host: emit our one IQN with the bound listen
// addr (or operator-supplied PortalAddr).
func (t *Target) ListTargets() []DiscoveryTarget {
	if t.cfg.IQN == "" {
		return nil
	}
	addr := t.cfg.PortalAddr
	if addr == "" {
		t.mu.Lock()
		if t.ln != nil {
			// Default portal group tag = 1 (matches Negotiation
			// default) so iscsiadm recognizes the entry.
			addr = t.ln.Addr().String() + ",1"
		}
		t.mu.Unlock()
	}
	return []DiscoveryTarget{{Name: t.cfg.IQN, Address: addr}}
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

	// Close session when target is closing. Crucially, cancel
	// the session's ctx in addition to closing conn — otherwise
	// a session blocked inside Provider.Open (waiting on
	// projection readiness) would keep t.sessions.Wait pinned
	// forever because Open's only wakeup channel is the ctx.
	go func() {
		select {
		case <-t.closed:
			cancel()
			_ = conn.Close()
		case <-ctx.Done():
		}
	}()

	// Do NOT open the backend here. Architect residual-risk
	// fix (2026-04-21): Discovery sessions must not depend on
	// frontend.Provider readiness — otherwise a not-yet-Healthy
	// volume blocks `iscsiadm -m discovery` indefinitely when
	// it should at least return SendTargets. The session opens
	// the backend after login succeeds, and only for Normal
	// sessions.
	sess := newSession(conn, t.cfg.Provider, t.cfg.VolumeID, t.cfg.Handler,
		t.cfg.Negotiation, t.cfg.DataOutTimeout, t, t, t.logger)
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
