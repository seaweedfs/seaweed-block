// Command blockvolume is the block volume daemon — the product
// host for per-volume observation reporting and assignment
// subscription.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

type flags struct {
	masterAddr string
	serverID   string
	volumeID   string
	replicaID  string
	dataAddr   string
	ctrlAddr   string
	hbInterval time.Duration
	// printReadyLine: test-only flag — emits one structured JSON
	// line to stdout on the FIRST received assignment with
	// Epoch > 0. L2 subprocess smoke uses this to assert route
	// closure.
	printReadyLine bool

	// T1 closure-loop flags: enable the minimum readiness bridge
	// (HealthyPathExecutor) and expose a status HTTP endpoint.
	// statusAddr ":0" is for tests — the process prints the bound
	// addr on the ready line so the test harness can discover it.
	enableT1Readiness bool
	statusAddr        string
	// statusRecovery — opt-in: enable /status/recovery?volume=v1
	// endpoint exposing engine.ReplicaProjection (Mode/R/S/H/Decision).
	// Default off; production binaries do NOT enable. Used by hardware
	// orchestration scripts for catch-up verification (R/H polling).
	statusRecovery bool

	// iSCSI frontend flags. iscsiListen is the TCP address the iSCSI
	// target binds on; empty disables the frontend. The bind is
	// rejected at startup if it is not a loopback address —
	// unauthenticated frontend on an external port would be unsafe.
	iscsiListen     string
	iscsiIQN        string
	iscsiPortalAddr string
	iscsiDataOutTTL time.Duration
	iscsiLUN        uint
	iscsiCHAPUser   string
	iscsiCHAPSecret string

	// NVMe/TCP frontend flags. Symmetric with iSCSI flags: same
	// loopback-only safe-default rule, same auto-enable of
	// --t1-readiness so the symmetry stays clean.
	nvmeListen    string
	nvmeSubsysNQN string
	nvmeNS        uint

	// Durable-backend flags. When --durable-root is set, the iSCSI
	// and NVMe providers use DurableProvider instead of memback.
	// --durable-impl selects walstore or smartwal (default smartwal).
	// --durable-blocks + --durable-blocksize are used on first-time
	// storage create.
	durableRoot      string
	durableImpl      string // "smartwal" | "walstore"
	durableBlocks    uint
	durableBlockSize uint

	// WAL retention window past checkpoint LSN. Zero = strict
	// checkpoint-driven recycle. Non-zero = walstore relaxes the
	// recovery-scan recycle gate to fromLSN > checkpointLSN -
	// walRetentionLSNs, giving a slow replica room to catch up while
	// it lags within the window. Substrate semantics differ: see
	// ProviderConfig.WALRetentionLSNs godoc.
	walRetentionLSNs uint64

	// Degraded-peer probe loop (primary-side runtime recovery trigger).
	// Off by default; --degraded-probe-interval=0 keeps the loop
	// disabled. When enabled (interval > 0), the loop iterates over
	// peers in ReplicaDegraded at the given interval and dispatches
	// an engine-ingress probe per peer (with per-peer cooldown).
	degradedProbeInterval     time.Duration
	degradedProbeCooldownBase time.Duration
	degradedProbeCooldownCap  time.Duration

	// Recovery mode selector. "legacy" (default) keeps the single-lane
	// core/transport rebuild path. "dual-lane" routes StartRebuild
	// through core/recovery's PrimaryBridge, binding a separate
	// listener at port=data-addr-port+1.
	//
	// Default flip from legacy → dual-lane is a separate operations
	// change after dual-lane is GREEN in CI / hardware.
	recoveryMode string

	// Replication acknowledgement profile. best-effort is the beta
	// default. sync-quorum / sync-all opt into foreground write ACK
	// gating through the durable WriteObserver seam.
	replicationAck string
}

func parseFlags(args []string) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("blockvolume", flag.ContinueOnError)
	fs.StringVar(&f.masterAddr, "master", "", "master gRPC address (required)")
	fs.StringVar(&f.serverID, "server-id", "", "this server identity (required)")
	fs.StringVar(&f.volumeID, "volume-id", "", "served volume (required)")
	fs.StringVar(&f.replicaID, "replica-id", "", "served replica (required)")
	fs.StringVar(&f.dataAddr, "data-addr", "", "data-path address (required)")
	fs.StringVar(&f.ctrlAddr, "ctrl-addr", "", "control-path address (required)")
	fs.DurationVar(&f.hbInterval, "heartbeat-interval", 2*time.Second, "heartbeat send interval")
	fs.BoolVar(&f.printReadyLine, "t0-print-ready", false, "internal test-only: emit one structured JSON line on stdout on first assignment")
	fs.BoolVar(&f.enableT1Readiness, "t1-readiness", false, "enable readiness bridge (HealthyPathExecutor) so adapter projection reaches Healthy")
	fs.StringVar(&f.statusAddr, "status-addr", "", "address for the status HTTP endpoint (e.g. 127.0.0.1:0); empty disables")
	fs.BoolVar(&f.statusRecovery, "status-recovery", false, "expose /status/recovery?volume=<id> with engine.ReplicaProjection (Mode/R/S/H/RecoveryDecision); off by default; loopback-only; intended for hardware test orchestration")
	fs.StringVar(&f.iscsiListen, "iscsi-listen", "", "iSCSI target bind address (e.g. 127.0.0.1:0); empty disables. Loopback-only unless paired with an operator-managed proxy")
	fs.StringVar(&f.iscsiIQN, "iscsi-iqn", "", "iSCSI target IQN (required if --iscsi-listen is set)")
	fs.StringVar(&f.iscsiPortalAddr, "iscsi-portal-addr", "", "iSCSI TargetAddress advertised in SendTargets responses (e.g. 203.0.113.10:3260,1). Defaults to the bound listen address. Does not change the loopback-only bind policy")
	fs.DurationVar(&f.iscsiDataOutTTL, "iscsi-dataout-timeout", 0, "iSCSI R2T/Data-Out wait timeout. 0 uses the target default. Bounds how long a session waits for solicited Data-Out from an initiator")
	fs.UintVar(&f.iscsiLUN, "iscsi-lun", 0, "iSCSI LUN id (default 0)")
	fs.StringVar(&f.iscsiCHAPUser, "iscsi-chap-username", "", "iSCSI target-side CHAP username. Requires --iscsi-chap-secret and --iscsi-listen")
	fs.StringVar(&f.iscsiCHAPSecret, "iscsi-chap-secret", "", "iSCSI target-side CHAP secret. Requires --iscsi-chap-username and --iscsi-listen")
	fs.StringVar(&f.nvmeListen, "nvme-listen", "", "NVMe/TCP target bind address (e.g. 127.0.0.1:0); empty disables. Loopback-only (no auth)")
	fs.StringVar(&f.nvmeSubsysNQN, "nvme-subsysnqn", "", "NVMe subsystem NQN (required if --nvme-listen is set)")
	fs.UintVar(&f.nvmeNS, "nvme-ns", 1, "NVMe namespace id (default 1)")
	fs.StringVar(&f.durableRoot, "durable-root", "", "directory for persistent storage files; empty = memback (non-durable)")
	fs.StringVar(&f.durableImpl, "durable-impl", "smartwal", "LogicalStorage impl: smartwal (default) or walstore; ignored unless --durable-root is set")
	fs.UintVar(&f.durableBlocks, "durable-blocks", 2048, "number of blocks per volume on first create (ignored when opening existing)")
	fs.UintVar(&f.durableBlockSize, "durable-blocksize", 4096, "block size in bytes on first create")
	fs.Uint64Var(&f.walRetentionLSNs, "wal-retention-lsns", 0,
		"WAL retention window past checkpoint LSN (walstore only). "+
			"Zero (default) preserves strict checkpoint-driven recycle. "+
			"Non-zero relaxes the recovery-scan recycle gate so a replica lagging "+
			"within the configured envelope still has a catch-up scan path; once a "+
			"replica falls past this window the engine escalates to rebuild. "+
			"Substrate semantics: walstore (append-only) honors this directly; "+
			"smartwal (fixed-size ring) bounds retention via --durable-blocks instead "+
			"and ignores this flag. Recommended starting value for sustained-write "+
			"workloads on walstore: 10000 LSNs (sized to expected replica lag).")
	fs.DurationVar(&f.degradedProbeInterval, "degraded-probe-interval", 0,
		"primary-side degraded-peer probe loop interval; 0 = OFF (default). "+
			"Recommended production value: 5s. Distinct from --degraded-probe-cooldown-base: this is how often "+
			"the loop wakes up; cooldown-base is how long a single peer is gated AFTER a probe attempt.")
	fs.DurationVar(&f.degradedProbeCooldownBase, "degraded-probe-cooldown-base", 5*time.Second,
		"per-peer cooldown after a probe attempt (also the reset value after a successful probe). "+
			"Doubled on consecutive failures up to --degraded-probe-cooldown-cap. "+
			"Ignored when --degraded-probe-interval=0.")
	fs.DurationVar(&f.degradedProbeCooldownCap, "degraded-probe-cooldown-cap", 60*time.Second,
		"per-peer cooldown ceiling for the consecutive-failure backoff. "+
			"Must be >= --degraded-probe-cooldown-base; values below base are normalized up to base. "+
			"Ignored when --degraded-probe-interval=0.")
	fs.StringVar(&f.recoveryMode, "recovery-mode", "legacy",
		"recovery path. \"legacy\" (default) = single-lane "+
			"core/transport rebuild. \"dual-lane\" = core/recovery PrimaryBridge "+
			"with separate listener at data-addr port+1. Default flip is a "+
			"separate change after dual-lane is GREEN in CI/hardware.")
	fs.StringVar(&f.replicationAck, "replication-ack", "best-effort",
		"replication acknowledgement profile: \"best-effort\" (default), \"sync-quorum\", or \"sync-all\". "+
			"sync-* modes fail foreground writes when required replica acknowledgement is unavailable.")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	if f.recoveryMode != "legacy" && f.recoveryMode != "dual-lane" {
		return flags{}, fmt.Errorf("--recovery-mode=%q invalid; want \"legacy\" or \"dual-lane\"", f.recoveryMode)
	}
	if _, _, err := parseReplicationAckProfile(f.replicationAck); err != nil {
		return flags{}, err
	}
	if f.iscsiCHAPUser == "" {
		f.iscsiCHAPUser = os.Getenv("SW_BLOCK_ISCSI_CHAP_USERNAME")
	}
	if f.iscsiCHAPSecret == "" {
		f.iscsiCHAPSecret = os.Getenv("SW_BLOCK_ISCSI_CHAP_SECRET")
	}
	missing := []string{}
	for name, val := range map[string]string{
		"master": f.masterAddr, "server-id": f.serverID,
		"volume-id": f.volumeID, "replica-id": f.replicaID,
		"data-addr": f.dataAddr, "ctrl-addr": f.ctrlAddr,
	} {
		if val == "" {
			missing = append(missing, "--"+name)
		}
	}
	if len(missing) > 0 {
		return flags{}, fmt.Errorf("required: %v", missing)
	}
	if f.iscsiListen != "" {
		if f.iscsiIQN == "" {
			return flags{}, fmt.Errorf("--iscsi-iqn is required when --iscsi-listen is set")
		}
		if err := enforceFrontendLoopbackBind("iscsi", f.iscsiListen); err != nil {
			return flags{}, err
		}
		// iSCSI needs a Healthy adapter projection to open a
		// memback backend. Auto-enable the T1 readiness bridge
		// rather than failing — safer default for the beta
		// product host. Emits a one-line stderr notice so
		// operators don't misread the auto-enable as an
		// invisible side effect.
		if !f.enableT1Readiness {
			fmt.Fprintln(os.Stderr, "blockvolume: iscsi enabled: t1-readiness auto-enabled")
		}
		f.enableT1Readiness = true
		if (f.iscsiCHAPUser == "") != (f.iscsiCHAPSecret == "") {
			return flags{}, fmt.Errorf("--iscsi-chap-username and --iscsi-chap-secret must be set together")
		}
	} else if f.iscsiPortalAddr != "" {
		return flags{}, fmt.Errorf("--iscsi-portal-addr requires --iscsi-listen")
	} else if f.iscsiDataOutTTL != 0 {
		return flags{}, fmt.Errorf("--iscsi-dataout-timeout requires --iscsi-listen")
	} else if f.iscsiCHAPUser != "" || f.iscsiCHAPSecret != "" {
		return flags{}, fmt.Errorf("--iscsi-chap-username/--iscsi-chap-secret require --iscsi-listen")
	}
	if f.nvmeListen != "" {
		if f.nvmeSubsysNQN == "" {
			return flags{}, fmt.Errorf("--nvme-subsysnqn is required when --nvme-listen is set")
		}
		if err := enforceFrontendLoopbackBind("nvme", f.nvmeListen); err != nil {
			return flags{}, err
		}
		// Symmetric with iSCSI auto-enable per QA checkpoint-7
		// note: keep the two protocols' safe-default behavior
		// identical so the closure report doesn't have to
		// explain asymmetry.
		if !f.enableT1Readiness {
			fmt.Fprintln(os.Stderr, "blockvolume: nvme enabled: t1-readiness auto-enabled")
		}
		f.enableT1Readiness = true
	}
	return f, nil
}

func parseReplicationAckProfile(profile string) (replication.DurabilityMode, durable.WriteAckPolicy, error) {
	switch profile {
	case "best-effort":
		return replication.DurabilityBestEffort, durable.WriteAckBestEffort, nil
	case "sync-quorum":
		return replication.DurabilitySyncQuorum, durable.WriteAckRequireObserverAck, nil
	case "sync-all":
		return replication.DurabilitySyncAll, durable.WriteAckRequireObserverAck, nil
	default:
		return replication.DurabilityBestEffort, durable.WriteAckBestEffort,
			fmt.Errorf("--replication-ack=%q invalid; want \"best-effort\", \"sync-quorum\", or \"sync-all\"", profile)
	}
}

// computeFrontendVolumeSize returns the byte capacity an iSCSI/NVMe
// target should advertise: durableBlocks × blockSize.
//
// iSCSI/NVMe externally-visible volume capacity and block count must
// derive from --durable-blocks × --durable-blocksize, not silently
// fall back to frontend defaults — otherwise the frontend would use
// 1 MiB defaults regardless of the daemon's actual durable config,
// breaking any workload above 1 MiB.
//
// Overflow guard: blocks × blockSize is computed in uint64 and a
// fits-in-uint64 check is applied (32×32 → 64 cannot overflow today
// since both fit in uint32, but keep the guard for future
// hardening).
func computeFrontendVolumeSize(blocks uint, blockSize uint) (uint64, error) {
	if blocks == 0 {
		return 0, fmt.Errorf("computeFrontendVolumeSize: --durable-blocks must be > 0")
	}
	if blockSize == 0 {
		return 0, fmt.Errorf("computeFrontendVolumeSize: --durable-blocksize must be > 0")
	}
	bs := uint64(blockSize)
	bk := uint64(blocks)
	out := bk * bs
	if out/bs != bk {
		return 0, fmt.Errorf("computeFrontendVolumeSize: blocks(%d) * blockSize(%d) overflows uint64", blocks, blockSize)
	}
	return out, nil
}

// enforceFrontendLoopbackBind mirrors volume.StatusServer's guard for
// any frontend (iscsi / nvme): refuses to bind on anything other than
// 127.0.0.1 / ::1. Frontends are currently unauthenticated;
// real-network exposure waits on the security track.
func enforceFrontendLoopbackBind(kind, addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("--%s-listen %q not host:port: %w", kind, addr, err)
	}
	if host == "" {
		return fmt.Errorf("--%s-listen %q has empty host; must be a loopback address (127.0.0.1 or ::1)", kind, addr)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("--%s-listen %q is not loopback; %s target is unauthenticated and refuses external binds", kind, addr, kind)
	}
	return nil
}

func main() {
	f, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume:", err)
		os.Exit(2)
	}
	os.Exit(run(f))
}

type readyLine struct {
	Component       string `json:"component"`
	Phase           string `json:"phase"`
	VolumeID        string `json:"volume_id"`
	ReplicaID       string `json:"replica_id"`
	Epoch           uint64 `json:"epoch"`
	EndpointVersion uint64 `json:"endpoint_version"`
}

type statusReadyLine struct {
	Component  string `json:"component"`
	Phase      string `json:"phase"`
	StatusAddr string `json:"status_addr"`
}

type iscsiReadyLine struct {
	Component string `json:"component"`
	Phase     string `json:"phase"`
	IscsiAddr string `json:"iscsi_addr"`
	IQN       string `json:"iqn"`
}

type nvmeReadyLine struct {
	Component string `json:"component"`
	Phase     string `json:"phase"`
	NvmeAddr  string `json:"nvme_addr"`
	SubsysNQN string `json:"subsys_nqn"`
}

func run(f flags) int {
	var readyCh chan adapter.AssignmentInfo
	if f.printReadyLine {
		readyCh = make(chan adapter.AssignmentInfo, 1)
	}

	h, err := volume.New(volume.Config{
		MasterAddr:        f.masterAddr,
		ServerID:          f.serverID,
		VolumeID:          f.volumeID,
		ReplicaID:         f.replicaID,
		DataAddr:          f.dataAddr,
		CtrlAddr:          f.ctrlAddr,
		HeartbeatInterval: f.hbInterval,
		ReadyMarker:       readyCh,
		EnableT1Readiness: f.enableT1Readiness,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume:", err)
		return 1
	}
	h.Start()

	var status *volume.StatusServer
	if f.statusAddr != "" {
		status = volume.NewStatusServer(h.ProjectionView())
		if f.statusRecovery {
			status.EnableRecoveryEndpoint()
		}
		bound, err := status.Start(f.statusAddr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: status server:", err)
			_ = h.Close()
			return 1
		}
		// Emit a discoverable ready line so subprocess harnesses
		// can learn the bound port when --status-addr is ":0".
		_ = json.NewEncoder(os.Stdout).Encode(statusReadyLine{
			Component:  "blockvolume",
			Phase:      "status-listening",
			StatusAddr: bound,
		})
	}

	if readyCh != nil {
		go func() {
			select {
			case info := <-readyCh:
				_ = json.NewEncoder(os.Stdout).Encode(readyLine{
					Component:       "blockvolume",
					Phase:           "assignment-received",
					VolumeID:        info.VolumeID,
					ReplicaID:       info.ReplicaID,
					Epoch:           info.Epoch,
					EndpointVersion: info.EndpointVersion,
				})
			case <-time.After(60 * time.Second):
				// no assignment arrived — exit caller's concern
			}
		}()
	}

	// T3b: pick Provider per --durable-root flag. When the flag is
	// unset, fall back to memback (non-durable, legacy behavior).
	// When set, DurableProvider opens (or creates) on-disk storage
	// per --durable-impl and runs Recovery before the frontends
	// accept connections.
	var provider frontend.Provider
	var durableProv *durable.DurableProvider
	if f.durableRoot != "" {
		cfg := durable.ProviderConfig{
			Impl:             durable.ImplName(f.durableImpl),
			StorageRoot:      f.durableRoot,
			BlockSize:        int(f.durableBlockSize),
			NumBlocks:        uint32(f.durableBlocks),
			WALRetentionLSNs: f.walRetentionLSNs,
		}
		dp, err := durable.NewDurableProvider(cfg, h.ProjectionView())
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: durable provider:", err)
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		}
		durableProv = dp
		provider = dp

		// Open storage role-agnostically via EnsureStorage so replica
		// roles (assigned to SUPPORTING, never reach Healthy via
		// projection) can still expose LogicalStorage to
		// ReplicaListener. The Healthy-gated dp.Open path is reserved
		// for frontend consumers (iSCSI / NVMe targets); they call it
		// when they accept their first I/O. Primary role: dp.Open
		// will succeed at frontend bind. Replica role: frontends are
		// not enabled (operator doesn't pass --iscsi-listen /
		// --nvme-listen for replicas), so dp.Open is never called.
		if _, sErr := dp.EnsureStorage(f.volumeID); sErr != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: durable storage open:", sErr)
			_ = dp.Close()
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		}
		recCtx, recCancel := context.WithTimeout(context.Background(), 10*time.Second)
		report, recErr := dp.RecoverVolume(recCtx, f.volumeID)
		recCancel()
		if recErr != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: durable recovery failed:", report.Evidence)
		} else {
			fmt.Fprintln(os.Stderr, "blockvolume: durable recovered:", report.Evidence)
		}
	} else {
		// memback fallback — wraps the volume's AdapterProjectionView
		// so per-op lineage fence + supersede fence still apply at
		// SCSI command granularity.
		provider = memback.NewProvider(h.ProjectionView())
	}

	// Bind the replication stack — ReplicationVolume for outbound peer
	// fan-out (primary role) + ReplicaListener for incoming WAL traffic
	// (replica role). Both code paths are bound at startup; whichever
	// role the master assignment selects is the active one. Construction
	// is post-dp.RecoverVolume because both borrow the same
	// LogicalStorage the durable Backend wraps; the LogicalStorage
	// isn't usable until recovery completes.
	//
	// ReplicaListener bind = --data-addr (NOT --ctrl-addr): the
	// transport executor's Probe + Ship paths dial peer.DataAddr
	// (executor.go:303); for the listener to be reachable from
	// remote primaries, it must bind on the address the master
	// will mint into AssignmentFact.peers[*].DataAddr — i.e., the
	// volume's --data-addr. --ctrl-addr stays reserved for future
	// control-plane split (no current binder).
	var (
		replVolume *replication.ReplicationVolume
		replListen *transport.ReplicaListener
	)
	if durableProv != nil {
		store := durableProv.LogicalStorage(f.volumeID)
		if store == nil {
			fmt.Fprintln(os.Stderr, "blockvolume: replication wire: LogicalStorage(", f.volumeID, ") returned nil")
			_ = durableProv.Close()
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		}
		replVolume = replication.NewReplicationVolume(f.volumeID, store)
		repMode, writeAck, err := parseReplicationAckProfile(f.replicationAck)
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockvolume:", err)
			_ = durableProv.Close()
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		}
		replVolume.SetDurabilityMode(repMode)
		h.SetReplicationVolume(replVolume)

		// When --recovery-mode=dual-lane, inject the dual-lane executor
		// factory so per-peer BlockExecutors route StartRebuild through
		// core/recovery's PrimaryBridge. Per-volume coordinator is
		// shared across all peers so MinPinAcrossActiveSessions
		// reflects the true minimum.
		//
		// Dual-lane address derivation: peer.DataAddr's port + 1.
		// Convention only — production may eventually carry a separate
		// dual-lane field in AssignmentFact.
		if f.recoveryMode == "dual-lane" {
			recoveryCoord := recovery.NewPeerShipCoordinator()
			replVolume.SetDualLaneExecutorFactory(func(s storage.LogicalStorage, replicaAddr, replicaID string) *transport.BlockExecutor {
				peerDualLane, err := deriveDualLaneAddr(replicaAddr)
				if err != nil {
					// Fall back to legacy ctor; the executor will not
					// have a dual-lane path. Logged for diagnostics.
					fmt.Fprintf(os.Stderr,
						"blockvolume: recovery-mode=dual-lane but peer addr %q has no port+1 form (%v); falling back to legacy executor for this peer\n",
						replicaAddr, err)
					return transport.NewBlockExecutor(s, replicaAddr)
				}
				return transport.NewBlockExecutorWithDualLane(s, replicaAddr, peerDualLane, recoveryCoord, recovery.ReplicaID(replicaID))
			})
			// Install the recycle-floor gate on the substrate so the
			// WAL recycle path consults the per-volume coordinator's
			// MinPinAcrossActiveSessions. Skipped silently when
			// substrate doesn't implement RecycleFloorGate.
			// walstore + memorywal both satisfy.
			if gate, ok := store.(storage.RecycleFloorGate); ok {
				gate.SetRecycleFloorSource(recoveryCoord)
				fmt.Fprintln(os.Stderr, "blockvolume: dual-lane recycle gate installed (substrate honors min(pin_floor))")
			} else {
				fmt.Fprintln(os.Stderr, "blockvolume: dual-lane substrate does not implement RecycleFloorGate; pin_floor advances unenforced at recycle")
			}
			fmt.Fprintln(os.Stderr, "blockvolume: recovery-mode=dual-lane (PrimaryBridge per-peer; coord per-volume)")
		}

		// Configure + start the degraded-peer probe loop if the
		// operator opted in via --degraded-probe-interval > 0. Off by
		// default. The probe loop is owned by ReplicationVolume
		// lifecycle; volume.Close stops the loop before peer teardown.
		//
		// Notes:
		//   - interval=0 → don't Configure (loop stays absent)
		//   - cooldown-base/cap normalized in NewProbeLoop (cap < base
		//     is bumped up to base; 0 → defaults)
		//   - production probeFn from volume.ProductionProbeFn —
		//     releases peer.mu before transport call; transport failure
		//     → non-nil err (advances backoff); transport success →
		//     nil err (resets cooldown).
		//   - Configure→Start ordering — both here (peers may or may
		//     not exist yet; empty peer set is safely no-op).
		if f.degradedProbeInterval > 0 {
			// Per-peer adapter registry is the engine state holder for
			// each admitted peer (NOT for the host's own slot, which
			// already has h.Adapter()). The probe loop's
			// ProductionProbeFn routes per-peer via this registry so
			// engine.checkReplicaID accepts the events.
			peerRegistry := volume.NewPeerAdapterRegistry(f.volumeID, f.replicaID)
			if err := replVolume.ConfigurePeerLifecycleHook(peerRegistry.OnPeerAdded, peerRegistry.OnPeerRemoved); err != nil {
				fmt.Fprintln(os.Stderr, "blockvolume: peer lifecycle hook:", err)
				_ = replVolume.Close()
				_ = durableProv.Close()
				if status != nil {
					shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = status.Close(shutCtx)
					shutCancel()
				}
				_ = h.Close()
				return 1
			}

			loopCfg := replication.ProbeLoopConfig{
				Interval:      f.degradedProbeInterval,
				MaxConcurrent: 1, // only 1 supported in this version
				CooldownBase:  f.degradedProbeCooldownBase,
				CooldownCap:   f.degradedProbeCooldownCap,
			}
			probeFn := volume.ProductionProbeFn(peerRegistry.AdapterFor)
			if err := replVolume.ConfigureProbeLoop(loopCfg, probeFn, time.Now); err != nil {
				fmt.Fprintln(os.Stderr, "blockvolume: probe loop configure:", err)
				_ = replVolume.Close()
				_ = durableProv.Close()
				if status != nil {
					shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = status.Close(shutCtx)
					shutCancel()
				}
				_ = h.Close()
				return 1
			}
			if err := replVolume.StartProbeLoop(); err != nil {
				fmt.Fprintln(os.Stderr, "blockvolume: probe loop start:", err)
				_ = replVolume.Close()
				_ = durableProv.Close()
				if status != nil {
					shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = status.Close(shutCtx)
					shutCancel()
				}
				_ = h.Close()
				return 1
			}
			fmt.Fprintf(os.Stderr,
				"blockvolume: G5-5C probe loop started (interval=%s cooldown-base=%s cooldown-cap=%s)\n",
				f.degradedProbeInterval, f.degradedProbeCooldownBase, f.degradedProbeCooldownCap)
		}

		// Wire the backend's WriteObserver eagerly via the Backend
		// accessor (same StorageBackend instance dp.Open returns later
		// for primary-role frontend bind). Without this hook, primary
		// writes would land locally but never ship to peers.
		if sb := durableProv.Backend(f.volumeID); sb != nil {
			sb.SetWriteObserver(replVolume)
			sb.SetWriteAckPolicy(writeAck)
		} else {
			fmt.Fprintln(os.Stderr, "blockvolume: write-observer wire: Backend(", f.volumeID, ") returned nil; replication fan-out disabled")
		}

		listener, err := transport.NewReplicaListener(f.dataAddr, store)
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: replica listener:", err)
			_ = replVolume.Close()
			_ = durableProv.Close()
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		}
		listener.Serve()
		replListen = listener

		// When --recovery-mode=dual-lane, also bind a separate listener
		// at port=data-addr-port+1 for inbound dual-lane recover-session
		// connections. Each accepted conn is handed to a fresh
		// recovery.Receiver via ReplicaBridge.AcceptDualLaneLoop.
		// Listener lifecycle is goroutine-scoped here; daemon shutdown
		// closes the underlying net.Listener via the deferred close
		// added below.
		if f.recoveryMode == "dual-lane" {
			dualLaneAddr, err := deriveDualLaneAddr(f.dataAddr)
			if err != nil {
				fmt.Fprintln(os.Stderr, "blockvolume: dual-lane listen addr derive:", err)
				_ = replListen.Stop
				_ = replVolume.Close()
				_ = durableProv.Close()
				if status != nil {
					shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = status.Close(shutCtx)
					shutCancel()
				}
				_ = h.Close()
				return 1
			}
			dlLn, err := net.Listen("tcp", dualLaneAddr)
			if err != nil {
				fmt.Fprintln(os.Stderr, "blockvolume: dual-lane listen:", err)
				_ = replListen.Stop
				_ = replVolume.Close()
				_ = durableProv.Close()
				if status != nil {
					shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = status.Close(shutCtx)
					shutCancel()
				}
				_ = h.Close()
				return 1
			}
			replicaBridge := recovery.NewReplicaBridge(store)
			dualLaneCtx, dualLaneCancel := context.WithCancel(context.Background())
			go replicaBridge.AcceptDualLaneLoop(dualLaneCtx, dlLn)
			defer func() {
				dualLaneCancel()
				_ = dlLn.Close()
			}()
			fmt.Fprintf(os.Stderr, "blockvolume: dual-lane listener bound at %s\n", dualLaneAddr)
		}
	}

	// Frontend volume capacity MUST derive from the daemon's
	// --durable-blocks × --durable-blocksize when --durable-root is
	// set, not silently fall back to frontend defaults. Without this
	// plumb-through, iSCSI/NVMe advertise 1 MiB regardless of the
	// daemon's actual durable capacity, which breaks any workload
	// > 1 MiB.
	//
	// Memback path (--durable-root unset): keep frontend defaults.
	// The frontend HandlerConfig zero-value path picks
	// DefaultBlockSize=512 + DefaultVolumeBlocks=2048 (1 MiB) which
	// is the historical memback contract.
	var (
		frontendBlockSize  uint32
		frontendVolumeSize uint64
	)
	if f.durableRoot != "" {
		bs, err := computeFrontendVolumeSize(f.durableBlocks, f.durableBlockSize)
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: frontend volume size:", err)
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		}
		frontendBlockSize = uint32(f.durableBlockSize)
		frontendVolumeSize = bs
	}
	// frontendBlockSize / frontendVolumeSize are zero on memback —
	// HandlerConfig zero-value defaulting kicks in inside iscsi /
	// nvme construction (preserves T2 historical 1 MiB / 512 B
	// behavior).

	var iscsiTarget *iscsi.Target
	var frontendTargets []*control.FrontendTarget
	if f.iscsiListen != "" {
		prov := provider
		negotiation := iscsi.NegotiableConfig{}
		if f.iscsiCHAPSecret != "" {
			negotiation = iscsi.DefaultNegotiableConfig()
			negotiation.CHAP = iscsi.CHAPConfig{
				Username: f.iscsiCHAPUser,
				Secret:   f.iscsiCHAPSecret,
			}
		}
		iscsiTarget = iscsi.NewTarget(iscsi.TargetConfig{
			Listen:         f.iscsiListen,
			IQN:            f.iscsiIQN,
			PortalAddr:     f.iscsiPortalAddr,
			VolumeID:       f.volumeID,
			Provider:       prov,
			Negotiation:    negotiation,
			DataOutTimeout: f.iscsiDataOutTTL,
			Handler: iscsi.HandlerConfig{
				BlockSize:  frontendBlockSize,
				VolumeSize: frontendVolumeSize,
				ALUA:       newProjectionALUAProvider(h.ProjectionView(), f.volumeID, f.replicaID),
			},
		})
		iscsiAddr, err := iscsiTarget.Start()
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: iscsi target:", err)
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		}
		_ = json.NewEncoder(os.Stdout).Encode(iscsiReadyLine{
			Component: "blockvolume",
			Phase:     "iscsi-listening",
			IscsiAddr: iscsiAddr,
			IQN:       f.iscsiIQN,
		})
		frontendTargets = append(frontendTargets, &control.FrontendTarget{
			Protocol: "iscsi",
			Addr:     iscsiAddr,
			Iqn:      f.iscsiIQN,
			Lun:      uint32(f.iscsiLUN),
		})
		h.SetFrontendTargets(frontendTargets)
	}

	var nvmeTarget *nvme.Target
	if f.nvmeListen != "" {
		prov := provider
		nvmeTarget = nvme.NewTarget(nvme.TargetConfig{
			Listen:    f.nvmeListen,
			SubsysNQN: f.nvmeSubsysNQN,
			VolumeID:  f.volumeID,
			Provider:  prov,
			// Capacity from durable config (see iSCSI block above).
			// frontendBlockSize / frontendVolumeSize are 0 on memback
			// path; nvme HandlerConfig zero-value defaulting preserves
			// the historical 1 MiB / 512 B contract.
			Handler: nvme.HandlerConfig{
				NSID:       uint32(f.nvmeNS),
				BlockSize:  frontendBlockSize,
				VolumeSize: frontendVolumeSize,
			},
		})
		nvmeAddr, err := nvmeTarget.Start()
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: nvme target:", err)
			if iscsiTarget != nil {
				_ = iscsiTarget.Close()
			}
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		}
		_ = json.NewEncoder(os.Stdout).Encode(nvmeReadyLine{
			Component: "blockvolume",
			Phase:     "nvme-listening",
			NvmeAddr:  nvmeAddr,
			SubsysNQN: f.nvmeSubsysNQN,
		})
		frontendTargets = append(frontendTargets, &control.FrontendTarget{
			Protocol: "nvme",
			Addr:     nvmeAddr,
			Nqn:      f.nvmeSubsysNQN,
			Nsid:     uint32(f.nvmeNS),
		})
		h.SetFrontendTargets(frontendTargets)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	if nvmeTarget != nil {
		_ = nvmeTarget.Close()
	}
	if iscsiTarget != nil {
		_ = iscsiTarget.Close()
	}
	// Tear down replication BEFORE durable storage close (LIFO
	// listener lifecycle). Listener closes its accept loop +
	// in-flight handler conns; ReplicationVolume closes its outbound
	// peer connections. Both borrow LogicalStorage that
	// durableProv.Close will release; tearing them down first
	// prevents use-after-free on the storage handle.
	if replListen != nil {
		replListen.Stop()
	}
	if replVolume != nil {
		_ = replVolume.Close()
	}
	// T3b: close DurableProvider in correct order — Provider.Close
	// closes backends first (flags them closed) then storage files
	// (releases fd + final fsync).
	if durableProv != nil {
		_ = durableProv.Close()
	}
	if status != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = status.Close(shutCtx)
		shutCancel()
	}
	if err := h.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume: close:", err)
		return 1
	}
	return 0
}

// deriveDualLaneAddr maps "host:port" → "host:port+1". Per
// docs/recovery-wiring-plan.md §4 Option A: dual-lane traffic uses a
// separate port from the legacy data port via this fixed convention.
// Production may eventually carry a separate dual-lane field in
// AssignmentFact; until then, all daemons must agree on this offset.
func deriveDualLaneAddr(addr string) (string, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("split %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("port %q not numeric: %w", portStr, err)
	}
	if port+1 > 65535 {
		return "", fmt.Errorf("port %d+1 overflows", port)
	}
	return net.JoinHostPort(host, strconv.Itoa(port+1)), nil
}
