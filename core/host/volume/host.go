package volume

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storeident"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config holds volume-host construction inputs. Flag parsing
// lives in cmd/blockvolume; this struct is the testable seam.
type Config struct {
	// MasterAddr is the gRPC address of the block master daemon.
	MasterAddr string
	// AckProfile is this volume's replication ACK profile in canonical form
	// (best_effort, sync_quorum, sync_all), answered on every promotion
	// probe; empty answers none (the master then holds the volume).
	AckProfile string
	// LinesNameTheirProcess: the master stamps every primary line with the
	// process it was issued for, so a line that names no process is not this
	// process's either and is not adopted until re-issued. Off, a line that
	// names none is adopted (fixtures that build lines by hand).
	LinesNameTheirProcess bool
	// MasterTLS is the mutual-TLS dial option of the master link
	// (link.ClientCredentials); nil dials plaintext (development mode).
	MasterTLS grpc.DialOption

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

	// RecoverTimeout bounds how long a primary-assigned replica may stay
	// not promotion-ready before it reports an explicit unhealthy fact
	// (#128 rule 6). Zero picks DefaultRecoverTimeout.
	RecoverTimeout time.Duration

	// Logger receives structured startup + loop-event logs.
	// Nil falls through to the default log package logger.
	Logger *log.Logger

	// ReadyMarker is a test-only channel that fires exactly once
	// when the volume's adapter first receives an assignment
	// with Epoch > 0 for its (VolumeID, ReplicaID). Lets L2
	// subprocess smokes emit a structured "assignment-received"
	// pass-line. Nil in production.
	ReadyMarker chan<- adapter.AssignmentInfo

	// EnableT1Readiness replaces the T0 noop executor with a
	// HealthyPathExecutor so the volume's adapter can actually
	// reach engine.ModeHealthy on a successful assignment. This
	// is the T1-owned "minimum product-route readiness bridge"
	// sketch §11.L2 requires. Off by default (so T0 tests keep
	// their documented non-healthy projection).
	EnableT1Readiness bool
	WorkspaceLease    bool // Opt-in: require a workspace lease before I/O or capture.
	// RF1Only, when set, is why this volume serves single-replica only
	// (walstore-chunk): every assignment with replica slots, primary with
	// peers or supporting replica, is refused before any peer set, role or
	// readiness is installed, and latched like a failed role admission.
	RF1Only string

	// StorageIdentity identifies the file system of the durable root
	// (machine id + st_dev + statfs f_fsid), reported in every heartbeat so the master can
	// detect a durable root that shares the authority store's journal. Zero
	// means unknown (memback, non-unix, or the root could not be read).
	StorageIdentity storeident.Identity

	// ReplicationVolume, when non-nil, receives peer-set updates on
	// every self-replica AssignmentFact. The Host calls
	// UpdateReplicaSet(peer_set_generation, targets) BEFORE
	// adapter.OnAssignment — install-or-refuse ordering (T4a-6
	// follow-up fix). If UpdateReplicaSet fails, OnAssignment is
	// skipped so the adapter's projection stays not-Healthy and the
	// StorageBackend's lineageCheck keeps writes blocked until master
	// stream replay redelivers a fact that can be fully installed.
	//
	// Nil means "no replication fan-out" (T0 observer-only hosts,
	// bootstrap before ReplicationVolume is ready). T4a-5 production
	// wiring sets this to the per-volume ReplicationVolume.
	ReplicationVolume *replication.ReplicationVolume
}

// assignmentConsumer is the narrow interface Host needs from the
// adapter. *adapter.VolumeReplicaAdapter satisfies it. Test doubles
// can substitute to drive applyFact without a real adapter + engine.
type assignmentConsumer interface {
	OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog
}

// replicaSetUpdater is the narrow interface Host needs from the
// replication layer. *replication.ReplicationVolume satisfies it.
// Test doubles can substitute to drive applyFact without needing a
// real per-volume fan-out + transport listener.
type replicaSetUpdater interface {
	UpdateReplicaSet(generation uint64, targets []replication.ReplicaTarget) error
}

// Host is the composed volume-side block product daemon.
type Host struct {
	// incarnation is new on every process start and reported in every
	// heartbeat, so the master can tell a restart from a long silence.
	mw          *masterWait // observed blockmaster call results (workspace startup wait)
	incarnation string
	// wd is the recovery watchdog (#128 rule 6).
	wd *recoveryWatchdog

	cfg    Config
	log    *log.Logger
	exec   *noopExecutor
	t1exec *HealthyPathExecutor
	adpt   assignmentConsumer
	// realAdpt is the underlying concrete adapter. Exposed for
	// production call sites that need the full surface (status
	// server, projection view wiring). Tests swap adpt only and
	// leave realAdpt nil.
	realAdpt *adapter.VolumeReplicaAdapter
	view     *AdapterProjectionView

	// replication may be set at New (via Config.ReplicationVolume) OR
	// post-New via SetReplicationVolume (binary path: storage isn't
	// open until dp.Open completes, which itself depends on host
	// reaching reachable+Healthy via the assignment subscription —
	// so the ReplicationVolume can only be constructed after Host.Start).
	// nil = observer-only (no fan-out).
	replicationMu sync.RWMutex
	replication   replicaSetUpdater

	// selfFence is this host's own replica listener (G2). When the host
	// takes the primary role at epoch e it fences the listener at e before
	// the adapter is told (so before the frontend serves IO); when it is
	// admitted as a supporting replica it re-admits replication at that
	// epoch. selfRole/selfEpoch remember the last decision so a listener
	// wired after the first fact (the binary path) is fenced on install.
	selfFenceMu  sync.Mutex
	selfFence    selfFencer
	selfRole     selfRole
	selfEpoch    uint64
	selfHasPeers bool
	// shipPrepared: the replication ship order was moved to the store since
	// this host last took a replica role (from which it may apply
	// replicated entries that never pass its own ship order). Separate from
	// selfRole, which a primary fence sets before the move succeeds (and
	// which the excluded-replica path sets only to close ingress).
	shipPrepared bool
	// roleAdmissionFailed (G1) latches a failed role call on the listener
	// (a floor that could not be persisted, or a fence that did not drain).
	// While set, the frontend projection is not Healthy, so writes refuse
	// even on a host that was healthy at its prior epoch; only a later
	// successful role call for the current role and epoch clears it.
	roleAdmissionFailed atomic.Value // string reason; "" = not failed
	// storeWait: the primary fact's replica-store wait (ReplicaStoresAwaited,
	// awaitingStoresEpoch), one value under its own lock.
	storeWait storeWait
	// ackProfileRefusal (string; "" = none): the last primary fact named a
	// replicated line under another ack profile than this volume's own
	// (AckProfileRefusal); while set the frontend is not Healthy.
	ackProfileRefusal atomic.Value
	// roleRetry: the last replica role call failed with an error a retry
	// may outlast (a floor that could not be persisted); runRoleRetry
	// re-applies the current role until it succeeds or is refused for good.
	// Under selfFenceMu.
	roleRetry bool
	// pendingSupport (under selfFenceMu): the supporting assignment a fact
	// skipped because its replica role call failed with an error a retry
	// may outlast. runRoleRetry completes it, under selfFenceMu, once the
	// role call succeeds while the desired role is still replica at that
	// epoch and no newer fact has been seen (factEpochSeen).
	pendingSupport *pendingSupport
	// factEpochSeen (under selfFenceMu): the highest epoch of any fact
	// applyFact has started on, recorded at its very start, before the fact
	// acts in any way (the watchdog, the role call, the adapter). The retry
	// completes a pending assignment, under selfFenceMu, only while no newer
	// epoch was seen; a newer fact arriving meanwhile waits at that record
	// until the completion is done, so none of its actions precede it and no
	// older completion lands after a newer transition. The record is the
	// only part of applyFact under the lock: facts applied concurrently (a
	// replay beside the stream, one inside another's hook) do not nest it.
	factEpochSeen uint64
	// RF1Only admission state, one unit under rf1Mu (field access only, no
	// calls): the highest refused epoch with its reason ("" = cleared), and the
	// highest epoch an RF1 fact was admitted at. A clear removes only the
	// refusal it can still see as not newer than its own fact.
	rf1Mu          sync.Mutex
	rf1Reason      string
	rf1Refused     uint64
	rf1Admitted    uint64
	rf1BeforeAdmit func() // test hook: between the stale check and the admit

	conn   *grpc.ClientConn
	obsCli control.ObservationServiceClient
	// probeRoles answers the promotion probe round's role fields (probe_round.go).
	probeRoles probeRoles
	asnCli     control.AssignmentServiceClient

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started atomic.Bool

	workspaceLeaseStarted atomic.Bool
	// Guard installation and terminal host fencing share this lock. Order:
	// workspaceLeaseMu -> guard.mu; never hold either across RPC or drain.
	workspaceLeaseMu sync.Mutex
	// workspaceStopPending is the guard's stop-pending mode, kept here for a
	// guard installed later; workspaceStopAttempt numbers detach attempts so
	// only the latest one's resume clears it.
	workspaceStopPending bool
	workspaceStopAttempt uint64
	readyOnce            atomic.Bool
	readyMarkerMu        sync.Mutex
	supportingReadyMarks map[string]struct{}

	assignmentEndpointWarning atomic.Value // diagnostic string; never an admission guard
	localReadinessBlocked     atomic.Bool
	localReadinessReason      atomic.Value // string
	// localRecoveryReason is the typed reason local durable recovery
	// failed (#133); empty when none or unclassified.
	localRecoveryReason atomic.Value // string
	readinessReplayMu   sync.Mutex
	readinessReplayFact *control.AssignmentFact

	// lastOtherLine captures the most recent AssignmentFact that
	// named a REPLICA OTHER than self for this host's VolumeID.
	// Populated when the master's volume-scoped subscription
	// delivers a supersede signal. T0 records only; no
	// demotion side effects in this slice.
	otherMu   sync.Mutex
	lastOther *control.AssignmentFact

	ownAssignmentMu                     sync.RWMutex
	ownAssignmentEpoch, ownAssignmentEV uint64

	frontendMu      sync.RWMutex
	frontendTargets []*control.FrontendTarget
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
	// A stale replay must not erase the newer foreign-holder fence (INV-AUTH-HOLDER-IDENTITY-001).
	if old := h.lastOther; old != nil && (cp.Epoch < old.Epoch || cp.Epoch == old.Epoch && cp.EndpointVersion < old.EndpointVersion) {
		h.otherMu.Unlock()
		return
	}
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

	conn, err := link.DialMaster(cfg.MasterAddr, cfg.MasterTLS, link.MasterReconnect())
	if err != nil {
		return nil, fmt.Errorf("volume.New: dial master %q: %w", cfg.MasterAddr, err)
	}

	// Pick executor: T0 default is noopExecutor (adapter stays
	// not-Healthy); T1 readiness bridge swaps in HealthyPathExecutor
	// so the adapter can actually reach engine.ModeHealthy.
	var (
		noopExec  *noopExecutor
		t1Exec    *HealthyPathExecutor
		execIface adapter.CommandExecutor
	)
	if cfg.EnableT1Readiness {
		t1Exec = NewHealthyPathExecutor()
		execIface = t1Exec
	} else {
		noopExec = newNoopExecutor()
		execIface = noopExec
	}
	adpt := adapter.NewVolumeReplicaAdapter(execIface)

	h := &Host{
		cfg:      cfg,
		log:      lg,
		exec:     noopExec,
		t1exec:   t1Exec,
		adpt:     adpt,
		realAdpt: adpt,
		conn:     conn,
		obsCli:   control.NewObservationServiceClient(conn),
		asnCli:   control.NewAssignmentServiceClient(conn),
	}
	h.incarnation = newIncarnation()
	h.mw = newMasterWait(nil, lg.Printf)
	h.wd = newRecoveryWatchdog(cfg.RecoverTimeout)
	h.log.Printf("blockvolume: volume %s recovery watchdog: a primary assignment must become promotion-ready within %s", cfg.VolumeID, h.wd.timeout)
	if cfg.ReplicationVolume != nil {
		h.setReplicationLocked(cfg.ReplicationVolume)
	}
	// View wires host as the SupersedeProbe so fail-closed kicks
	// in when master names another replica at a newer lineage.
	h.view = NewAdapterProjectionView(adpt, cfg.VolumeID, cfg.ReplicaID, h)
	h.view.workspaceRequired = cfg.WorkspaceLease
	return h, nil
}

// BlockLocalReadiness prevents assignment facts from entering the local
// adapter while preserving heartbeat/status serving. Use this when local
// storage evidence says this replica must not publish Healthy.
func (h *Host) BlockLocalReadiness(reason string) {
	if reason == "" {
		reason = "local readiness blocked"
	}
	h.localReadinessReason.Store(reason)
	h.localReadinessBlocked.Store(true)
}

// ClearLocalReadinessBlock allows future assignment stream replays to enter
// the adapter after local startup/recovery evidence is clean.
func (h *Host) ClearLocalReadinessBlock() {
	h.localReadinessReason.Store("")
	h.localReadinessBlocked.Store(false)
	h.localRecoveryReason.Store("")
}

func (h *Host) localReadinessBlock() (bool, string) {
	if !h.localReadinessBlocked.Load() {
		return false, ""
	}
	reason, _ := h.localReadinessReason.Load().(string)
	if reason == "" {
		reason = "local readiness blocked"
	}
	return true, reason
}

// SetReplicationVolume installs (or replaces) the per-volume
// replication fan-out coordinator AFTER Host.New. Used by the
// production binary path: storage isn't available until the durable
// provider opens, which itself depends on the host reaching a
// reachable+Healthy projection via the assignment subscription —
// so ReplicationVolume can only be constructed post-Start.
//
// The host's apply path reads h.replication under a read-lock at
// every assignment; SetReplicationVolume writes under the write-lock,
// so it is safe to call concurrently with assignment processing.
// Effect applies on the next AssignmentFact.
//
// Passing nil reverts to observer-only (no fan-out). The previous
// ReplicationVolume — if any — is NOT closed by SetReplicationVolume;
// the caller owns its lifecycle (Provider/composition root).
func (h *Host) SetReplicationVolume(rv *replication.ReplicationVolume) {
	h.replicationMu.Lock()
	if rv == nil {
		h.replication = nil
		h.replicationMu.Unlock()
		return
	}
	h.setReplicationLocked(rv)
	h.replicationMu.Unlock()
	h.replayLocalReadinessFactIfReady()
}

// setReplicationLocked installs the replication slot. Caller must
// hold h.replicationMu (or be in the constructor where no other
// goroutine has visibility yet).
func (h *Host) setReplicationLocked(rv replicaSetUpdater) {
	h.replication = rv
	// This host attests its peers' stores to the master: a peer counts only
	// while its store is the one the master recorded.
	if c, ok := rv.(storeRequirer); ok && c != nil {
		c.RequireAttestedStores()
	}
}

func (h *Host) storeLocalReadinessReplayFact(fact *control.AssignmentFact) {
	if fact == nil {
		return
	}
	cp := proto.Clone(fact).(*control.AssignmentFact)
	h.readinessReplayMu.Lock()
	h.readinessReplayFact = cp
	h.readinessReplayMu.Unlock()
}

func (h *Host) replayLocalReadinessFactIfReady() {
	if blocked, _ := h.localReadinessBlock(); blocked {
		return
	}
	h.readinessReplayMu.Lock()
	fact := h.readinessReplayFact
	h.readinessReplayFact = nil
	h.readinessReplayMu.Unlock()
	if fact == nil {
		return
	}
	h.log.Printf("blockvolume: volume %s replaying assignment after local readiness cleared server=%q epoch=%d ev=%d data=%q ctrl=%q",
		h.cfg.VolumeID, fact.ServerId, fact.Epoch, fact.EndpointVersion, fact.DataAddr, fact.CtrlAddr)
	h.applyFact(fact)
}

// selfFencer is the narrow interface Host needs from its own replica
// listener (G2). *transport.ReplicaListener satisfies it.
type selfFencer interface {
	// FenceAsPrimary: this host is primary at epoch; refuse all inbound
	// replication mutations, and any below epoch. It returns once no
	// mutation admitted before the fence is still applying, or an error
	// if that did not happen in time (the host must not take the role).
	FenceAsPrimary(epoch uint64) error
	// AdmitAsReplica: this host supports another primary at epoch;
	// accept replication again, but none below epoch. An error means the
	// floor could not be made durable; the host must not admit the role.
	AdmitAsReplica(epoch uint64) error
}

type selfRole int

const (
	selfRoleNone selfRole = iota
	selfRolePrimary
	selfRoleReplica
)

// SetSelfFence installs this host's own replica listener (G2). The binary
// starts the assignment stream before the listener exists, so a role
// already adopted is applied to the listener here, before it serves (a
// listener that is not yet serving has nothing in flight to wait for).
//
// A replay error is returned (and latched): the caller must not serve the
// listener, and the host's frontend stays unhealthy until a later role call
// succeeds.
func (h *Host) SetSelfFence(f selfFencer) error {
	h.selfFenceMu.Lock()
	defer h.selfFenceMu.Unlock()
	h.selfFence = f
	if f == nil {
		return nil
	}
	err := applySelfRoleWithPeers(f, h.selfRole, h.selfEpoch, h.selfHasPeers)
	h.latchRoleAdmission(h.selfRole, h.selfEpoch, err)
	return err
}

// latchRoleAdmission records the outcome of a role call: a failure sets the
// latch (projection not Healthy); a success clears it. Caller holds
// selfFenceMu.
func (h *Host) latchRoleAdmission(role selfRole, epoch uint64, err error) {
	h.roleRetry = role == selfRoleReplica && err != nil && !errors.Is(err, ErrRoleRefused)
	if role == selfRoleNone {
		return
	}
	if err != nil {
		h.roleAdmissionFailed.Store(fmt.Sprintf("replica listener role call failed at epoch %d: %v", epoch, err))
		return
	}
	h.roleAdmissionFailed.Store("")
}

// pendingSupport is a supporting assignment waiting for its role call.
type pendingSupport struct {
	info    adapter.AssignmentInfo
	primary string
}

// ErrRoleRefused marks a role call refused for good (the listener is closed,
// or the volume refuses replica peers): retrying cannot succeed, so the
// latch stays with its reason and no retry runs.
var ErrRoleRefused = errors.New("role refused")

// roleRetryMin and roleRetryMax bound the backoff between retries of a
// failed replica role call.
var roleRetryMin, roleRetryMax = 250 * time.Millisecond, 10 * time.Second

// runRoleRetry re-applies a failed replica role call with backoff, so a
// replica whose floor could not be persisted (a full or read-only disk)
// rejoins once it can, with no operator: the latch clears on success and
// the primary's probe of degraded peers finds it ready again.
func (h *Host) runRoleRetry(ctx context.Context) {
	defer h.wg.Done()
	delay := roleRetryMin
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		h.selfFenceMu.Lock()
		if !h.roleRetry || h.selfFence == nil || h.selfRole != selfRoleReplica {
			h.selfFenceMu.Unlock()
			delay = roleRetryMin
			continue
		}
		role, epoch := h.selfRole, h.selfEpoch
		// Re-applied directly, not through fenceSelfWithPeers: the desired
		// role is unchanged, and shipPrepared was already cleared when this
		// replica role was first taken there.
		err := applySelfRoleWithPeers(h.selfFence, role, epoch, h.selfHasPeers)
		h.latchRoleAdmission(role, epoch, err)
		if err == nil {
			h.log.Printf("blockvolume: replica role at epoch %d admitted on retry", epoch)
			// Finish what the fact skipped when its role call failed, as the
			// assignment path would have after a successful one, still under
			// selfFenceMu: the desired role and epoch read above cannot move
			// until it is done, so no newer transition lands in between.
			// (The completion takes no host lock: the adapter's executor is
			// local, the watchdog and the marker have their own.)
			if p := h.pendingSupport; p != nil && p.info.Epoch == epoch {
				h.pendingSupport = nil
				h.completeSupportingIfCurrentLocked(p.info, p.primary)
			}
			h.selfFenceMu.Unlock()
			delay = roleRetryMin
			continue
		}
		h.selfFenceMu.Unlock()
		if delay *= 2; delay > roleRetryMax {
			delay = roleRetryMax
		}
	}
}

// supersedesPendingLocked reports whether a supporting fact at epoch may
// clear or replace the pending assignment: only one at its epoch or below
// (selfFenceMu held).
func (h *Host) supersedesPendingLocked(epoch uint64) bool {
	return h.pendingSupport == nil || h.pendingSupport.info.Epoch <= epoch
}

// completeSupportingIfCurrentLocked is the one completion of a supporting
// assignment, for the fact's own successful role call and for the retry's:
// it completes only while the desired role is still replica at info's epoch
// and no newer fact has been seen (factEpochSeen), checked and completed
// without releasing selfFenceMu (held by the caller). A newer fact records
// its epoch under that lock before it acts at all, so it either was seen
// here (nothing old is completed) or waits until this completion is done:
// no older admission, watchdog step or marker lands after it. Facts run
// concurrently by design (a replay beside the stream); only this and the
// record at applyFact's start are under the lock.
func (h *Host) completeSupportingIfCurrentLocked(info adapter.AssignmentInfo, primary string) {
	if h.selfRole != selfRoleReplica || h.selfEpoch != info.Epoch || h.factEpochSeen > info.Epoch {
		h.log.Printf("blockvolume: volume %s supporting assignment at epoch %d superseded before its completion (desired %v@%d, newest fact %d)",
			h.cfg.VolumeID, info.Epoch, h.selfRole, h.selfEpoch, h.factEpochSeen)
		return
	}
	h.completeSupporting(info, primary)
}

// completeSupporting applies a supporting assignment once its replica role
// call succeeded: the adapter's identity, the watchdog, the ready marker.
// Called only through completeSupportingIfCurrentLocked.
func (h *Host) completeSupporting(info adapter.AssignmentInfo, primary string) {
	h.adpt.OnAssignment(info)
	h.wd.supportingAdmitted(info.Epoch)
	h.log.Printf("blockvolume: volume %s admitted as SUPPORTING replica %s at epoch=%d EV=%d behind primary %s; frontend remains gated",
		h.cfg.VolumeID, info.ReplicaID, info.Epoch, info.EndpointVersion, primary)
	// Supporting replicas need durable lineage latching for promotion-readiness,
	// but this must not consume the primary readyOnce. A later promotion to
	// PRIMARY has to emit its own marker for the promoted lineage.
	h.emitSupportingReadyMarker(info)
}

// RoleAdmissionFailure reports the G1 admission latch: whether the last role
// call on this host's replica listener failed, and why. The projection view
// treats a failure as not Healthy.
func (h *Host) RoleAdmissionFailure() (bool, string) {
	h.rf1Mu.Lock()
	rf1 := h.rf1Reason
	h.rf1Mu.Unlock()
	if rf1 != "" {
		return true, rf1
	}
	reason, _ := h.roleAdmissionFailed.Load().(string)
	return reason != "", reason
}

// fenceSelf records the role this host just took and applies it to its
// listener, if installed. An error means the listener could not be fenced
// in time; the caller must not adopt the primary role.
//
// The desired role is monotonic in epoch: a call below the current desired
// epoch is a stale fact (master replay) and is refused without replacing the
// desired role or touching the admission latch, so a stale success can never
// reconcile a newer failure (review of #83).
func (h *Host) fenceSelf(role selfRole, epoch uint64) error {
	return h.fenceSelfWithPeers(role, epoch, true)
}

func (h *Host) fenceSelfWithPeers(role selfRole, epoch uint64, hasPeers bool) error {
	h.selfFenceMu.Lock()
	defer h.selfFenceMu.Unlock()
	if epoch < h.selfEpoch {
		return fmt.Errorf("stale role call at epoch %d (current desired epoch %d)", epoch, h.selfEpoch)
	}
	h.selfRole, h.selfEpoch, h.selfHasPeers = role, epoch, hasPeers
	if role == selfRoleReplica {
		h.shipPrepared = false
	}
	if h.selfFence == nil {
		return nil
	}
	err := applySelfRoleWithPeers(h.selfFence, role, epoch, hasPeers)
	h.latchRoleAdmission(role, epoch, err)
	return err
}

// Optional listener lifecycle policy consumes only master-assigned peer slots.
// Called under selfFenceMu, before role admission; legacy fencers are unchanged.
func applySelfRoleWithPeers(f selfFencer, role selfRole, epoch uint64, hasPeers bool) error {
	switch role {
	case selfRolePrimary:
		if gated, ok := f.(interface{ FenceAsPrimaryWithPeers(uint64, bool) error }); ok {
			return gated.FenceAsPrimaryWithPeers(epoch, hasPeers)
		}
		return f.FenceAsPrimary(epoch)
	case selfRoleReplica:
		return f.AdmitAsReplica(epoch)
	}
	return nil
}

// getReplication returns the current replication slot under a
// read-lock. Used by applyFact at every AssignmentFact.
func (h *Host) getReplication() replicaSetUpdater {
	h.replicationMu.RLock()
	defer h.replicationMu.RUnlock()
	return h.replication
}

// IsSuperseded is the one foreign-holder comparison. Callers supply the
// named lineage: adopted engine state for fencing, observed own assignment
// for exit classification. Zero lineage selects neither fencing nor exit78
// (INV-AUTH-HOLDER-IDENTITY-001; INV-WS-STARTUP-RETRY-001).
func (h *Host) IsSuperseded(selfReplicaID string, selfEpoch, selfEV uint64) bool {
	h.otherMu.Lock()
	defer h.otherMu.Unlock()
	other := h.lastOther
	if selfEpoch == 0 || other == nil || other.ReplicaId == "" || other.VolumeId != h.cfg.VolumeID {
		return false
	}
	if other.ReplicaId == selfReplicaID && h.assignmentNamesSelf(other) {
		return false
	}
	return other.Epoch > selfEpoch || other.Epoch == selfEpoch && other.EndpointVersion > selfEV
}

// ProjectionView returns the AdapterProjectionView that bridges
// the adapter's engine projection to the frontend contract. Used
// by the volume's status endpoint and by L1/L2 tests that want
// to consume frontend readiness directly.
func (h *Host) ProjectionView() *AdapterProjectionView { return h.view }

// Adapter exposes the underlying VolumeReplicaAdapter for tests.
func (h *Host) Adapter() *adapter.VolumeReplicaAdapter { return h.realAdpt }

// Executor exposes the noopExecutor for tests that want to
// inspect recorded commands.
func (h *Host) Executor() *noopExecutor { return h.exec }

// SetFrontendTargets publishes read-only frontend target facts into
// the next heartbeat. These facts are endpoint observations only:
// they do not change assignment, readiness, or authority state.
func (h *Host) SetFrontendTargets(targets []*control.FrontendTarget) {
	h.frontendMu.Lock()
	defer h.frontendMu.Unlock()
	h.frontendTargets = cloneFrontendTargets(targets)
}

func (h *Host) frontendTargetsSnapshot() []*control.FrontendTarget {
	h.frontendMu.RLock()
	defer h.frontendMu.RUnlock()
	return cloneFrontendTargets(h.frontendTargets)
}

func cloneFrontendTargets(in []*control.FrontendTarget) []*control.FrontendTarget {
	if len(in) == 0 {
		return nil
	}
	out := make([]*control.FrontendTarget, 0, len(in))
	for _, t := range in {
		if t == nil {
			continue
		}
		out = append(out, proto.Clone(t).(*control.FrontendTarget))
	}
	return out
}

// Start kicks off the heartbeat loop + subscribe loop. Returns a
// context cancelled on Close.
func (h *Host) Start() context.Context {
	if !h.started.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	h.wg.Add(4)
	go h.runHeartbeat(ctx)
	go h.runSubscribe(ctx)
	go h.runProbeRounds(ctx)
	go h.runRoleRetry(ctx)
	return ctx
}

// Close stops loops, closes the gRPC connection. Idempotent.
func (h *Host) Close() error {
	h.FenceWorkspaceLease()
	if h.cancel != nil {
		h.cancel()
	}
	h.wg.Wait()
	return h.conn.Close()
}

// runHeartbeat sends a HeartbeatReport every HeartbeatInterval.
// Send failures are logged with bounded backoff but do NOT panic
// or hang the volume process.
//
// The loop shape is intentionally simple: immediate first report,
// then periodic reports until the host context is cancelled.
func (h *Host) runHeartbeat(ctx context.Context) {
	defer h.wg.Done()
	tick := time.NewTicker(h.cfg.HeartbeatInterval)
	defer tick.Stop()

	send := func() {
		rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := h.obsCli.ReportHeartbeat(rpcCtx, h.buildReport())
		if ctx.Err() == nil {
			h.mw.call(err)
			// The wait's reason reaches the journal on a change and every
			// 10 min from here, whether or not anyone reads /status.
			_ = h.MasterWaitStatus()
		}
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
	fact, ready := h.promotionReady()
	unhealthy := h.unhealthyReason(fact, ready)
	return &control.HeartbeatReport{
		ServerId:  h.cfg.ServerID,
		SentAt:    timestamppb.Now(),
		Reachable: true,
		Eligible:  true,
		// Raw identity facts; the master compares them with its own store.
		MachineId:        h.cfg.StorageIdentity.MachineID,
		DurableRootDev:   h.cfg.StorageIdentity.Dev,
		DurableRootFsid:  h.cfg.StorageIdentity.FSID,
		StoreAttestation: h.storeAttestation(),
		Slots: []*control.HeartbeatSlot{
			{
				VolumeId:        h.cfg.VolumeID,
				ReplicaId:       h.cfg.ReplicaID,
				DataAddr:        h.cfg.DataAddr,
				CtrlAddr:        h.cfg.CtrlAddr,
				Frontends:       h.frontendTargetsSnapshot(),
				Reachable:       true,
				ReadyForPrimary: ready,
				Eligible:        unhealthy == "", // an explicit unhealthy slot is ineligible
				Incarnation:     h.incarnation,
				UnhealthyReason: unhealthy,
			},
		},
	}
}

// storeAttester is the replication slot's view of its write-ACK set
// (replication.ReplicationVolume).
type storeAttester interface {
	LocalStoreID() storage.StoreID
	AttestedStores() map[string]storage.StoreID
}

// storeRequirer is the replication slot's switch that makes peers count
// only with the store the master recorded (replication.ReplicationVolume).
type storeRequirer interface {
	RequireAttestedStores()
}


// storeAttestation is this node's attestation of its write-ACK set while it
// holds the primary role (admitted, at its current epoch): its own store and
// every Healthy peer's last barrier store. nil on a replica, before the
// role is admitted, or while the local store carries no id.
func (h *Host) storeAttestation() *control.StoreAttestation {
	h.selfFenceMu.Lock()
	role, epoch := h.selfRole, h.selfEpoch
	h.selfFenceMu.Unlock()
	if role != selfRolePrimary || epoch == 0 {
		return nil
	}
	if reason, _ := h.roleAdmissionFailed.Load().(string); reason != "" {
		return nil
	}
	rv, ok := h.getReplication().(storeAttester)
	if !ok || rv == nil {
		return nil
	}
	own := rv.LocalStoreID()
	if own.IsZero() {
		return nil
	}
	att := &control.StoreAttestation{VolumeId: h.cfg.VolumeID, ReplicaId: h.cfg.ReplicaID, Epoch: epoch,
		Stores: []*control.AttestedStore{{ReplicaId: h.cfg.ReplicaID, StoreId: append([]byte(nil), own[:]...)}}}
	peers := rv.AttestedStores()
	ids := make([]string, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		sid := peers[id]
		att.Stores = append(att.Stores, &control.AttestedStore{ReplicaId: id, StoreId: append([]byte(nil), sid[:]...)})
	}
	return att
}

// newIncarnation returns a random process incarnation id. The master compares
// incarnations only for equality (a restarted process), never for order, so
// the clock fallback when the random source fails is enough.
func newIncarnation() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("t%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// readyForPrimaryFact is the volume-side promotion-readiness fact used by the
// authority controller for failover/rebalance candidates. A live process or
// Kubernetes Ready pod is not enough; the adapter must expose an engine
// truth-domain promotion fact, and this replica must not have been superseded by
// a newer authority line. Supporting replica-ready at the same lineage is still
// a valid candidate fact even though it remains frontend-unhealthy until
// authority moves.
func (h *Host) readyForPrimaryFact() bool {
	_, ready := h.promotionReady()
	return ready
}

// promotionReady returns the engine's promotion fact and whether it
// counts as ready (ready, and its line not superseded).
func (h *Host) promotionReady() (engine.PromotionReadyFact, bool) {
	if h == nil || h.view == nil {
		return engine.PromotionReadyFact{}, false
	}
	fact := h.view.PromotionReadyFact()
	if !fact.Ready {
		return fact, false
	}
	return fact, !h.view.isSupersededLine(fact.Epoch, fact.EndpointVersion)
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
		ServerId:  h.cfg.ServerID, // INV-AUTH-HOLDER-IDENTITY-001: the master checks it against the stream's host
	})
	if err != nil {
		if ctx.Err() == nil {
			h.mw.call(err)
		}
		return err
	}
	for {
		fact, err := stream.Recv()
		if ctx.Err() == nil && err != io.EOF {
			h.mw.call(err) // a delivered fact or a handler's status is contact
			if err == nil {
				h.mw.identityAccepted() // the stream checks the holder's identity
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		h.log.Printf("blockvolume: assignment stream delivery process=%q server=%q volume=%q replica=%q epoch=%d ev=%d data=%q ctrl=%q",
			h.cfg.ServerID, fact.ServerId, fact.VolumeId, fact.ReplicaId, fact.Epoch, fact.EndpointVersion, fact.DataAddr, fact.CtrlAddr)
		h.applyFact(fact)
	}
}

// applyFact processes one AssignmentFact from the master's
// subscription stream. Extracted from streamOnce so tests can
// exercise the install-or-refuse seam directly without a real
// gRPC stream.
//
// Install-or-refuse ordering (T4a-6 follow-up fix, QA finding #1):
//
//  1. If the fact names another replica, record supersede and return.
//  2. Otherwise (self-replica fact):
//     a. Install replication peer set first (if configured).
//     b. On UpdateReplicaSet failure, LOG and RETURN WITHOUT
//     calling OnAssignment. The adapter's projection stays at
//     its prior (not-Healthy until first successful install)
//     state; StorageBackend.lineageCheck keeps writes blocked.
//     Master's stream replay redelivers this fact on the next
//     cycle; natural eventual convergence.
//     c. Only after replication install succeeds (or observer-only
//     mode): apply OnAssignment, driving the adapter's
//     projection to Healthy.
//
// This preserves the V2 BlockVol.SetReplicaAddrs-then-admit-writes
// atomicity that V3 lost when adapter + ReplicationVolume got split
// across packages. Without install-or-refuse, a window existed where
// OnAssignment could flip the projection Healthy before peers were
// installed, admitting local-only writes with zero fan-out.
func (h *Host) applyFact(fact *control.AssignmentFact) {
	// Record the fact's epoch before it acts (see factEpochSeen).
	h.selfFenceMu.Lock()
	if fact.GetEpoch() > h.factEpochSeen {
		h.factEpochSeen = fact.GetEpoch()
	}
	h.selfFenceMu.Unlock()
	// INV-AUTH-HOLDER-IDENTITY-001: matching r1 and loopback endpoints do not identify a host.
	if h.refusePrimaryAssignment(fact) {
		return
	}
	if fact.ReplicaId != h.cfg.ReplicaID {
		h.wd.notPrimary()
		// Another replica is authoritative for this volume line. Record that
		// line first so AdapterProjectionView can keep this replica frontend-
		// unhealthy even if its local engine is otherwise caught up.
		h.recordOtherLine(fact)
		if info, ok := decodeSelfPeerAssignment(fact, h.cfg.ReplicaID); ok {
			if blocked, reason := h.localReadinessBlock(); blocked {
				h.storeLocalReadinessReplayFact(fact)
				h.log.Printf("blockvolume: volume %s local readiness blocked (%s); NOT admitting supporting replica %s at epoch=%d EV=%d",
					h.cfg.VolumeID, reason, info.ReplicaID, info.Epoch, info.EndpointVersion)
				return
			}
			if h.cfg.RF1Only != "" {
				h.refuseRF1("a supporting replica role", info.Epoch)
				return
			}
			if err := h.fenceSelf(selfRoleReplica, info.Epoch); err != nil {
				// G1: fail closed, as for the primary path. The role call is
				// retried (runRoleRetry), which completes this assignment
				// once it succeeds; a later fact supersedes it.
				h.log.Printf("blockvolume: volume %s could not admit its replica listener at epoch %d: %v — NOT admitting supporting replica (fail-closed)",
					h.cfg.VolumeID, info.Epoch, err)
				h.selfFenceMu.Lock()
				// An older fact (a late replay) never touches a newer
				// pending assignment: it neither clears nor replaces it.
				if h.supersedesPendingLocked(info.Epoch) {
					h.pendingSupport = nil
					if !errors.Is(err, ErrRoleRefused) && h.selfRole == selfRoleReplica && h.selfEpoch == info.Epoch {
						h.pendingSupport = &pendingSupport{info: info, primary: fact.ReplicaId}
					}
				}
				h.selfFenceMu.Unlock()
				return
			}
			h.selfFenceMu.Lock()
			if h.supersedesPendingLocked(info.Epoch) {
				h.pendingSupport = nil // this fact supersedes a skipped one at its epoch or below
			}
			h.completeSupportingIfCurrentLocked(info, fact.ReplicaId)
			h.selfFenceMu.Unlock()
			return
		}
		// The new authoritative set excludes us: close assignment-gated
		// ingress as well as withdrawing the frontend through recordOtherLine.
		if err := h.fenceSelfWithPeers(selfRolePrimary, fact.Epoch, false); err != nil {
			h.log.Printf("blockvolume: excluded replica ingress fence: %v", err)
		}
		h.log.Printf("blockvolume: volume %s authority is now %s@%d (not this replica %s); recording supersede, not applying to adapter",
			h.cfg.VolumeID, fact.ReplicaId, fact.Epoch, h.cfg.ReplicaID)
		return
	}

	// A line issued for another process of this replica is the dead
	// process's: its sessions (endpoint version, session id) are the ones
	// each replica still holds, and a session this process opened on it
	// could equal one of them. Nothing is adopted or recorded (no
	// assignment, watchdog, replica set or role) until the master re-issues
	// the line for this process (a new endpoint version).
	if inc := fact.GetLineIncarnation(); inc != h.incarnation && (inc != "" || h.cfg.LinesNameTheirProcess) {
		h.log.Printf("blockvolume: volume %s line epoch=%d EV=%d was issued for process %s, this is %s: NOT adopting until it is re-issued",
			h.cfg.VolumeID, fact.Epoch, fact.EndpointVersion, inc, h.incarnation)
		return
	}

	h.recordSelfAssignment(fact)
	// A primary of a replicated line serves only under the master's ack
	// profile: set (or cleared) before anything below can open the frontend.
	h.noteAckProfile(fact)
	// A fact that awaits replica stores closes the frontend at once; one
	// that carries them all opens it only after the peers' expected stores
	// are installed (below), so no write is served before its peers count.
	if fact.GetReplicaStoresAwaited() > 0 {
		h.noteReplicaStores(fact)
	}
	h.reportAssignmentEndpoints(fact)

	// The recovery watchdog runs from the primary assignment, before any
	// admission step can fail (#128 rule 6).
	h.wd.primaryAssigned(fact.Epoch)
	if blocked, reason := h.localReadinessBlock(); blocked {
		h.storeLocalReadinessReplayFact(fact)
		h.log.Printf("blockvolume: volume %s local readiness blocked (%s); NOT applying primary assignment %s@%d to adapter",
			h.cfg.VolumeID, reason, fact.ReplicaId, fact.Epoch)
		return
	}

	// An RF1-only volume refuses replica peers before anything is installed,
	// and a fact below a refused epoch is stale: it neither adopts nor clears.
	if h.cfg.RF1Only != "" {
		if len(fact.Peers) > 0 {
			h.refuseRF1(fmt.Sprintf("a primary role with %d replica peers", len(fact.Peers)), fact.Epoch)
			return
		}
		h.rf1Mu.Lock()
		refused := h.rf1Refused
		h.rf1Mu.Unlock()
		if fact.Epoch < refused {
			h.log.Printf("blockvolume: volume %s stale RF1 assignment at epoch %d below refused epoch %d — NOT applying to adapter",
				h.cfg.VolumeID, fact.Epoch, refused)
			return
		}
	}

	// The target's own store must still show what the master probed before
	// it adopts the role (nothing is installed yet, so a refusal has no
	// effect to undo).
	rv := h.getReplication()
	if src, ok := rv.(primaryEvidenceSource); ok && rv != nil {
		st, end, err := src.LocalPrimaryEvidence()
		if err != nil {
			h.refusePrimary(fmt.Sprintf("epoch %d: the local store's evidence is unreadable: %v", fact.Epoch, err))
			return
		}
		floor, floorErr := uint64(0), error(nil)
		if fr, ok := h.selfFenceFloor(); ok {
			floor, floorErr = fr.EpochFloor()
		}
		var store storage.StoreID
		if sid, ok := rv.(interface{ LocalStoreID() storage.StoreID }); ok {
			store = sid.LocalStoreID()
		}
		if reason := checkPrimaryEvidence(fact, st, end, floor, floorErr == nil && floor > 0, store); reason != "" {
			h.refusePrimary(reason)
			return
		}
	}

	// Step (2a): install replication peer set first. Decoded via
	// the separate host-only decodeReplicaTargets path (not through
	// AssignmentInfo), so the TestNoOtherAssignmentInfoConstruction
	// AST fence stays green.
	if rv != nil {
		targets, gen := decodeReplicaTargets(fact)
		if err := rv.UpdateReplicaSet(gen, targets); err != nil {
			// Step (2b): fail closed — do NOT apply OnAssignment.
			h.log.Printf("blockvolume: volume %s replication UpdateReplicaSet failed (gen=%d, peers=%d): %v — NOT applying to adapter (fail-closed; master will retry via stream replay)",
				h.cfg.VolumeID, gen, len(targets), err)
			return
		}
		// Always log the peer-set install at INFO so operators (and
		// hardware-test harness) can disambiguate "peer set is what I
		// expect" from "peer set is empty" without -v / debug flags.
		// One line per fact emission; small log footprint.
		peerIDs := make([]string, 0, len(targets))
		for _, t := range targets {
			peerIDs = append(peerIDs, fmt.Sprintf("%s@%s", t.ReplicaID, t.DataAddr))
		}
		h.log.Printf("blockvolume: volume %s replication UpdateReplicaSet ok (gen=%d, peers=%d): %v",
			h.cfg.VolumeID, gen, len(targets), peerIDs)
	}
	h.noteReplicaStores(fact)

	// Step (2c): identity install only after replication is live.
	// SOLE permitted decode path. See subscribe.go.
	info := decodeAssignmentFact(fact)
	// G2: fence our own replica listener at this epoch before the adapter
	// can turn Healthy and the frontend serves IO, so a deposed primary that
	// still reaches us cannot get its writes applied (or counted) here.
	if err := h.fenceSelfWithPeers(selfRolePrimary, info.Epoch, len(fact.Peers) > 0); err != nil {
		// Fail closed, as for an UpdateReplicaSet failure: the frontend
		// stays gated and master's stream replay redelivers the fact.
		h.log.Printf("blockvolume: volume %s could not fence its own replica listener at epoch %d: %v — NOT applying to adapter (fail-closed)",
			h.cfg.VolumeID, info.Epoch, err)
		return
	}
	// A node taking the primary role from another role holds entries it
	// applied as a replica, which never passed its own ship order: move the
	// ship cursor to the store's next LSN before it serves, or its first
	// write waits for an LSN that never comes. Its listener is fenced as
	// primary now (no replicated entry lands any more) and the frontend is
	// not serving yet (no local write is pending).
	h.selfFenceMu.Lock()
	prepared := h.shipPrepared
	h.selfFenceMu.Unlock()
	if r, ok := rv.(interface{ RebaseShipToStore() error }); ok && rv != nil && !prepared {
		if err := r.RebaseShipToStore(); err != nil {
			h.log.Printf("blockvolume: volume %s could not move its ship order to the store at epoch %d: %v — NOT applying to adapter (fail-closed)",
				h.cfg.VolumeID, info.Epoch, err)
			return
		}
		h.selfFenceMu.Lock()
		h.shipPrepared = true
		h.selfFenceMu.Unlock()
	}
	if h.cfg.RF1Only != "" && !h.admitRF1(fact.Epoch) {
		h.log.Printf("blockvolume: volume %s RF1 assignment at epoch %d overtaken by a newer refusal — NOT applying to adapter",
			h.cfg.VolumeID, fact.Epoch)
		return
	}
	h.adpt.OnAssignment(info)
	h.log.Printf("blockvolume: volume %s adopted as PRIMARY at %s@%d (EV=%d) process=%q server=%q data=%q ctrl=%q",
		h.cfg.VolumeID, info.ReplicaID, info.Epoch, info.EndpointVersion, h.cfg.ServerID, fact.ServerId, fact.DataAddr, fact.CtrlAddr)
	if h.cfg.ReadyMarker != nil && info.Epoch > 0 && h.readyOnce.CompareAndSwap(false, true) {
		select {
		case h.cfg.ReadyMarker <- info:
		default:
		}
	}
}

// refuseRF1 latches the refusal of a role with replica slots on an RF1-only
// volume (Config.RF1Only): no peer set, role or readiness was installed.
func (h *Host) refuseRF1(what string, epoch uint64) {
	reason := fmt.Sprintf("volume %s refuses %s at epoch %d: %s", h.cfg.VolumeID, what, epoch, h.cfg.RF1Only)
	h.rf1Mu.Lock()
	stale := epoch < h.rf1Admitted // older than an admitted RF1 fact: does not re-latch
	if !stale && epoch >= h.rf1Refused {
		h.rf1Refused, h.rf1Reason = epoch, reason
	}
	h.rf1Mu.Unlock()
	if stale {
		h.log.Printf("blockvolume: stale %s (admitted RF1 epoch is newer) — NOT applying to adapter", reason)
		return
	}
	h.log.Printf("blockvolume: %s — NOT applying to adapter (fail-closed)", reason)
}

// admitRF1 clears the RF1 refusal for an RF1 fact whose role was admitted,
// unless a refusal at a newer epoch was latched meanwhile (then it reports
// false and the fact must not be adopted).
func (h *Host) admitRF1(epoch uint64) bool {
	if hook := h.rf1BeforeAdmit; hook != nil {
		hook()
	}
	h.rf1Mu.Lock()
	defer h.rf1Mu.Unlock()
	if epoch < h.rf1Refused {
		return false
	}
	h.rf1Reason = ""
	if epoch > h.rf1Admitted {
		h.rf1Admitted = epoch
	}
	return true
}

func (h *Host) emitSupportingReadyMarker(info adapter.AssignmentInfo) {
	if h.cfg.ReadyMarker == nil || info.Epoch == 0 {
		return
	}
	key := fmt.Sprintf("%s/%d/%d", info.ReplicaID, info.Epoch, info.EndpointVersion)
	h.readyMarkerMu.Lock()
	if h.supportingReadyMarks == nil {
		h.supportingReadyMarks = make(map[string]struct{})
	}
	if _, ok := h.supportingReadyMarks[key]; ok {
		h.readyMarkerMu.Unlock()
		return
	}
	h.supportingReadyMarks[key] = struct{}{}
	h.readyMarkerMu.Unlock()
	select {
	case h.cfg.ReadyMarker <- info:
	default:
	}
}
