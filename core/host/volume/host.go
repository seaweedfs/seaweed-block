package volume

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config holds volume-host construction inputs. Flag parsing
// lives in cmd/blockvolume; this struct is the testable seam.
type Config struct {
	// MasterAddr is the gRPC address of the block master daemon.
	MasterAddr string

	// ServerID identifies this volume process to the master.
	// Used as the HeartbeatReport.ServerID field.
	ServerID string

	// Volume identity served by this volume process (T0 ships
	// one volume per process).
	VolumeID  string
	ReplicaID string
	DataAddr  string
	CtrlAddr  string

	// HeartbeatInterval is the loop cadence for ReportHeartbeat.
	// Zero picks the default (2s).
	HeartbeatInterval time.Duration

	// Logger receives structured startup + loop-event logs.
	// Nil falls through to the default log package logger.
	Logger *log.Logger

	// ReadyMarker is a test-only channel that fires exactly once
	// when the volume's adapter first receives an assignment
	// with Epoch > 0 for its (VolumeID, ReplicaID). Lets L2
	// subprocess smokes emit a structured "assignment-received"
	// pass-line. Nil in production.
	ReadyMarker chan<- adapter.AssignmentInfo
}

// Host is the composed volume-side block product daemon.
type Host struct {
	cfg  Config
	log  *log.Logger
	exec *noopExecutor
	adpt *adapter.VolumeReplicaAdapter

	conn   *grpc.ClientConn
	obsCli control.ObservationServiceClient
	asnCli control.AssignmentServiceClient

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started atomic.Bool

	readyOnce atomic.Bool

	// lastOtherLine captures the most recent AssignmentFact that
	// named a REPLICA OTHER than self for this host's VolumeID.
	// Populated when the master's volume-scoped subscription
	// delivers a supersede signal. T0 records only; no
	// demotion side effects in this slice.
	otherMu   sync.Mutex
	lastOther *control.AssignmentFact
}

// LastOtherLine returns the most recent VOLUME authority fact
// that names a replica other than self. Operators can use this
// to detect "this volume process has been superseded" without
// running a full demotion. Returns nil if no supersede event
// has been observed.
func (h *Host) LastOtherLine() *control.AssignmentFact {
	h.otherMu.Lock()
	defer h.otherMu.Unlock()
	if h.lastOther == nil {
		return nil
	}
	// Defensive copy so callers don't mutate internal state.
	// Use proto.Clone to avoid copying the proto-internal
	// MessageState mutex (Go vet flags a raw struct copy as
	// "copies lock value").
	return proto.Clone(h.lastOther).(*control.AssignmentFact)
}

func (h *Host) recordOtherLine(f *control.AssignmentFact) {
	if f == nil {
		return
	}
	// Defensive copy on write (round-4 architect medium fix):
	// the gRPC stack may reuse the receive buffer or wrap the
	// pointer in ways future refactors could expose. proto.Clone
	// is the canonical safe deep-copy for proto messages.
	cp := proto.Clone(f).(*control.AssignmentFact)
	h.otherMu.Lock()
	h.lastOther = cp
	h.otherMu.Unlock()
}

// New dials the master and builds the adapter+executor pair. Does
// NOT start the heartbeat / subscribe loops — caller invokes Start.
func New(cfg Config) (*Host, error) {
	if cfg.MasterAddr == "" {
		return nil, fmt.Errorf("volume.New: MasterAddr required")
	}
	if cfg.ServerID == "" || cfg.VolumeID == "" || cfg.ReplicaID == "" {
		return nil, fmt.Errorf("volume.New: ServerID, VolumeID, ReplicaID required")
	}
	if cfg.DataAddr == "" || cfg.CtrlAddr == "" {
		return nil, fmt.Errorf("volume.New: DataAddr, CtrlAddr required")
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 2 * time.Second
	}
	lg := cfg.Logger
	if lg == nil {
		lg = log.Default()
	}

	conn, err := grpc.NewClient(cfg.MasterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("volume.New: dial master %q: %w", cfg.MasterAddr, err)
	}

	exec := newNoopExecutor()
	adpt := adapter.NewVolumeReplicaAdapter(exec)

	h := &Host{
		cfg:    cfg,
		log:    lg,
		exec:   exec,
		adpt:   adpt,
		conn:   conn,
		obsCli: control.NewObservationServiceClient(conn),
		asnCli: control.NewAssignmentServiceClient(conn),
	}
	return h, nil
}

// Adapter exposes the underlying VolumeReplicaAdapter for tests.
func (h *Host) Adapter() *adapter.VolumeReplicaAdapter { return h.adpt }

// Executor exposes the noopExecutor for tests that want to
// inspect recorded commands.
func (h *Host) Executor() *noopExecutor { return h.exec }

// Start kicks off the heartbeat loop + subscribe loop. Returns a
// context cancelled on Close.
func (h *Host) Start() context.Context {
	if !h.started.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	h.wg.Add(2)
	go h.runHeartbeat(ctx)
	go h.runSubscribe(ctx)
	return ctx
}

// Close stops loops, closes the gRPC connection. Idempotent.
func (h *Host) Close() error {
	if h.cancel != nil {
		h.cancel()
	}
	h.wg.Wait()
	return h.conn.Close()
}

// runHeartbeat sends a HeartbeatReport every HeartbeatInterval.
// Send failures are logged with bounded backoff but do NOT panic
// or hang the volume process (per sketch §8.2.1 test 4).
//
// The loop shape is ported from weed/server/block_heartbeat_loop.go
// — see docs/t0-port-audit.md §2.
func (h *Host) runHeartbeat(ctx context.Context) {
	defer h.wg.Done()
	tick := time.NewTicker(h.cfg.HeartbeatInterval)
	defer tick.Stop()

	send := func() {
		rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := h.obsCli.ReportHeartbeat(rpcCtx, h.buildReport())
		if err != nil {
			h.log.Printf("blockvolume: heartbeat: %v", err)
		}
	}
	send() // immediate first report on start
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			send()
		}
	}
}

// buildReport collects local slot facts into a wire HeartbeatReport.
// T0 single-volume-per-process simplicity: one replica slot.
func (h *Host) buildReport() *control.HeartbeatReport {
	return &control.HeartbeatReport{
		ServerId:  h.cfg.ServerID,
		SentAt:    timestamppb.Now(),
		Reachable: true,
		Eligible:  true,
		Slots: []*control.HeartbeatSlot{
			{
				VolumeId:        h.cfg.VolumeID,
				ReplicaId:       h.cfg.ReplicaID,
				DataAddr:        h.cfg.DataAddr,
				CtrlAddr:        h.cfg.CtrlAddr,
				Reachable:       true,
				ReadyForPrimary: true,
				Eligible:        true,
			},
		},
	}
}

// runSubscribe maintains a SubscribeAssignments stream. Reconnects
// on stream end / error with bounded backoff. Every delivered
// AssignmentFact flows through decodeAssignmentFact → OnAssignment.
//
// Reconnect semantics (sketch §8.2.1 test 3):
//   - Allowed: current-line replay on resubscribe; adapter
//     monotonic guard idempotently no-ops.
//   - Required: no reverse, no stale pollution.
func (h *Host) runSubscribe(ctx context.Context) {
	defer h.wg.Done()
	var backoff time.Duration
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if backoff > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
		err := h.streamOnce(ctx)
		switch {
		case err == nil, err == io.EOF, ctx.Err() != nil:
			// Clean end or shutdown: reconnect immediately.
			backoff = 0
		default:
			h.log.Printf("blockvolume: subscribe stream: %v", err)
			if backoff < 5*time.Second {
				backoff = backoff*2 + 200*time.Millisecond
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
		}
	}
}

// streamOnce opens one SubscribeAssignments RPC and forwards every
// received fact until the stream ends. The master sends
// VOLUME-SCOPED facts (round-4 architect high-1 fix): any replica's
// AssignmentInfo for this volume may arrive. We filter by
// self-replica-id before decoding:
//
//   - fact.ReplicaId == self.ReplicaID: decode via
//     decodeAssignmentFact and feed adapter.
//   - fact.ReplicaId != self.ReplicaID: we have been SUPERSEDED.
//     Record the event; do NOT feed the adapter. T0 surfaces the
//     fact in logs + LastOtherAssignment() for status queries;
//     real demotion side effects (close data-path, release
//     resources) are G3+ territory.
//
// Returns nil on normal close.
func (h *Host) streamOnce(ctx context.Context) error {
	stream, err := h.asnCli.SubscribeAssignments(ctx, &control.SubscribeRequest{
		VolumeId:  h.cfg.VolumeID,
		ReplicaId: h.cfg.ReplicaID,
	})
	if err != nil {
		return err
	}
	for {
		fact, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if fact.ReplicaId != h.cfg.ReplicaID {
			// Superseded. The volume currently serving
			// (vid, self.ReplicaID) is no longer the current
			// authoritative line for this volume.
			h.recordOtherLine(fact)
			h.log.Printf("blockvolume: volume %s authority is now %s@%d (not this replica %s); recording supersede, not applying to adapter",
				h.cfg.VolumeID, fact.ReplicaId, fact.Epoch, h.cfg.ReplicaID)
			continue
		}
		// SOLE permitted decode path. See subscribe.go.
		info := decodeAssignmentFact(fact)
		h.adpt.OnAssignment(info)
		if h.cfg.ReadyMarker != nil && info.Epoch > 0 && h.readyOnce.CompareAndSwap(false, true) {
			select {
			case h.cfg.ReadyMarker <- info:
			default:
			}
		}
	}
}
