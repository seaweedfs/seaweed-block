// Command blockvolume is the block volume daemon — the product
// host for per-volume observation reporting and assignment
// subscription.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvmerdma"
	"github.com/seaweedfs/seaweed-block/core/frontend/ublk"
	"github.com/seaweedfs/seaweed-block/core/geometry"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storeident"
	"github.com/seaweedfs/seaweed-block/core/transport"
	"github.com/seaweedfs/seaweed-block/internal/buildinfo"
)

type flags struct {
	cpuProfileDir  string
	memoryLimitMiB int64
	masterAddr     string
	serverID       string
	volumeID       string
	layoutID       string
	replicaID      string
	dataAddr       string
	ctrlAddr       string
	hbInterval     time.Duration
	workspaceLease bool
	// recoverTimeout: #128 rule 6 recovery watchdog.
	recoverTimeout time.Duration
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
	// External status bind is off by default. Node-loss gates opt in so
	// blockmaster can probe surviving replicas across Kubernetes nodes.
	allowExternalStatusBind bool
	masterTLS               link.TLSFiles // the master link's mutual TLS (phase 0)
	runtimeRebuildEndpoint  bool

	// iSCSI frontend flags. iscsiListen is the TCP address the iSCSI
	// target binds on; empty disables the frontend. The safe default
	// rejects non-loopback binds. Node-loss gates must opt into
	// external bind and provide CHAP.
	iscsiListen            string
	iscsiIQN               string
	iscsiPortalAddr        string
	iscsiDataOutTTL        time.Duration
	iscsiLUN               uint
	iscsiCHAPUser          string
	iscsiCHAPSecret        string
	allowExternalISCSIBind bool

	// NVMe frontend flags. Symmetric with iSCSI flags: same
	// loopback-only safe-default rule, same auto-enable of
	// --t1-readiness so the symmetry stays clean.
	nvmeListen    string
	nvmeSubsysNQN string
	nvmeNS        uint
	nvmeTransport string
	nvmeMaxH2C    uint
	// External NVMe bind is explicit because the current target has no
	// authentication. Phase 106 uses it for controlled Kubernetes cross-node
	// gates; default installs remain loopback-only.
	allowExternalNVMeBind bool

	// ublk frontend: serve the volume as the local block device
	// /dev/ublkbN (Linux ublk_drv, root). -1 disables.
	ublkDevice     int
	ublkQueues     int
	ublkQueueDepth int

	// Durable-backend flags. When --durable-root is set, the iSCSI
	// and NVMe providers use DurableProvider instead of memback.
	// --durable-impl selects walstore, smartwal (experimental), or the
	// explicit parallel-walstore candidate. Empty (default) = auto: an
	// existing volume opens with its on-disk format, a new one is walstore.
	// --durable-blocks + --durable-blocksize are used on first-time
	// storage create.
	durableRoot string
	durableImpl string // "" (auto) | "walstore" | "smartwal" | "parallel-walstore" | "walstore-chunk"
	// --chunk-*: the SeaweedFS remote of --durable-impl=walstore-chunk.
	chunk                                chunkFlags
	durableBlocks                        uint
	durableBlockSize                     uint
	durableWALMultiBlockRecords          bool
	durableWALRecoveryTestDisableFlusher bool
	// volumeLockDirTestOnly replaces durable.HostVolumeLockDir (hidden,
	// test-only; refused under systemd).
	volumeLockDirTestOnly string

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
	version        bool
}

// volumeLockDirTestOnlyFlag is hidden from --help (printVisibleUsage).
const volumeLockDirTestOnlyFlag = "volume-lock-dir-test-only"

// printVisibleUsage is the default usage without the hidden test flag.
func printVisibleUsage(fs *flag.FlagSet) {
	visible := flag.NewFlagSet(fs.Name(), flag.ContinueOnError)
	visible.SetOutput(fs.Output())
	fs.VisitAll(func(fl *flag.Flag) {
		if fl.Name != volumeLockDirTestOnlyFlag {
			visible.Var(fl.Value, fl.Name, fl.Usage)
		}
	})
	fmt.Fprintf(fs.Output(), "Usage of %s:\n", fs.Name())
	visible.PrintDefaults()
}

func parseFlags(args []string) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("blockvolume", flag.ContinueOnError)
	fs.Int64Var(&f.memoryLimitMiB, "memory-limit-mib", 4*storage.DefaultCutPayloadMemoryBudget/(1<<20), "process Go soft memory limit in MiB (0 preserves GOMEMLIMIT); explicit GOMEMLIMIT is preserved unless this flag is set")
	fs.StringVar(&f.masterAddr, "master", "", "master gRPC address (required)")
	fs.StringVar(&f.serverID, "server-id", "", "this server identity (required)")
	fs.StringVar(&f.masterTLS.Cert, "master-tls-cert", "", "this node's certificate (PEM) for mutual TLS to blockmaster; its CN must equal --server-id")
	fs.StringVar(&f.masterTLS.Key, "master-tls-key", "", "the key of --master-tls-cert (PEM)")
	fs.StringVar(&f.masterTLS.CA, "master-ca", "", "CA (PEM) blockmaster's certificate must verify against")
	fs.StringVar(&f.volumeID, "volume-id", "", "served volume (required)")
	fs.StringVar(&f.layoutID, "layout-id", "", "master-minted layout identity; when set, durable-root is the base for <volume>-<id8>")
	fs.StringVar(&f.replicaID, "replica-id", "", "served replica (required)")
	fs.StringVar(&f.dataAddr, "data-addr", "", "data-path address (required)")
	fs.StringVar(&f.ctrlAddr, "ctrl-addr", "", "control-path address (required)")
	fs.DurationVar(&f.hbInterval, "heartbeat-interval", 2*time.Second, "heartbeat send interval")
	fs.DurationVar(&f.recoverTimeout, "recover-timeout", volume.DefaultRecoverTimeout, "how long a primary-assigned replica may stay not promotion-ready before it reports itself unhealthy (recovery_timeout) so the master can fail over")
	fs.BoolVar(&f.workspaceLease, "workspace-lease", false, "walstore-chunk workspace: require host-owned master lease; fail closed when unavailable")
	fs.BoolVar(&f.printReadyLine, "t0-print-ready", false, "internal test-only: emit one structured JSON line on stdout on first assignment")
	fs.BoolVar(&f.enableT1Readiness, "t1-readiness", false, "enable readiness bridge (HealthyPathExecutor) so adapter projection reaches Healthy")
	fs.StringVar(&f.statusAddr, "status-addr", "", "address for the status HTTP endpoint (e.g. 127.0.0.1:0); empty disables")
	fs.StringVar(&f.cpuProfileDir, "cpu-profile-dir", os.Getenv(cpuProfileEnv), "diagnostics: an existing directory; SIGUSR1 then toggles a CPU profile written there (default $"+cpuProfileEnv+"; empty disables)")
	fs.BoolVar(&f.statusRecovery, "status-recovery", false, "expose /status/recovery?volume=<id> with engine.ReplicaProjection (Mode/R/S/H/RecoveryDecision); off by default; loopback-only; intended for hardware test orchestration")
	fs.BoolVar(&f.allowExternalStatusBind, "allow-external-status-bind", false, "allow the unauthenticated status endpoint to bind and serve non-loopback addresses; intended only for explicit Kubernetes node-loss gates")
	fs.BoolVar(&f.runtimeRebuildEndpoint, "runtime-rebuild-endpoint", false, "expose POST /runtime/rebuild on the status HTTP listener; off by default; starts primary-side rebuild/catch-up traffic only after request lineage validation")
	fs.StringVar(&f.iscsiListen, "iscsi-listen", "", "iSCSI target bind address (e.g. 127.0.0.1:0); empty disables. Loopback-only unless paired with an operator-managed proxy")
	fs.StringVar(&f.iscsiIQN, "iscsi-iqn", "", "iSCSI target IQN (required if --iscsi-listen is set)")
	fs.StringVar(&f.iscsiPortalAddr, "iscsi-portal-addr", "", "iSCSI TargetAddress advertised in SendTargets responses (e.g. 203.0.113.10:3260,1). Defaults to the bound listen address. Does not change the loopback-only bind policy")
	fs.DurationVar(&f.iscsiDataOutTTL, "iscsi-dataout-timeout", 0, "iSCSI R2T/Data-Out wait timeout. 0 uses the target default. Bounds how long a session waits for solicited Data-Out from an initiator")
	fs.UintVar(&f.iscsiLUN, "iscsi-lun", 0, "iSCSI LUN id (default 0)")
	fs.StringVar(&f.iscsiCHAPUser, "iscsi-chap-username", "", "iSCSI target-side CHAP username. Requires --iscsi-chap-secret and --iscsi-listen")
	fs.StringVar(&f.iscsiCHAPSecret, "iscsi-chap-secret", "", "iSCSI target-side CHAP secret. Requires --iscsi-chap-username and --iscsi-listen")
	fs.BoolVar(&f.allowExternalISCSIBind, "allow-external-iscsi-bind", false, "allow iSCSI to bind a non-loopback address; requires CHAP and is intended for explicit Kubernetes node-loss gates")
	fs.StringVar(&f.nvmeListen, "nvme-listen", "", "NVMe/TCP target bind address (e.g. 127.0.0.1:0); empty disables. Loopback-only (no auth)")
	fs.StringVar(&f.nvmeSubsysNQN, "nvme-subsysnqn", "", "NVMe subsystem NQN (required if --nvme-listen is set)")
	fs.UintVar(&f.nvmeNS, "nvme-ns", 1, "NVMe namespace id (default 1)")
	fs.StringVar(&f.nvmeTransport, "nvme-transport", "tcp", "NVMe-oF transport: tcp or the explicit Linux-only standalone rdma target")
	fs.UintVar(&f.nvmeMaxH2C, "nvme-max-h2c-data-length", 0, "NVMe/TCP MaxH2CDataLength in bytes. 0 preserves the target default")
	fs.BoolVar(&f.allowExternalNVMeBind, "allow-external-nvme-bind", false, "allow NVMe to bind a non-loopback address; unauthenticated, intended only for explicit gates")
	fs.IntVar(&f.ublkDevice, "ublk-device", -1, "serve the volume as the local block device /dev/ublkbN (Linux ublk_drv, root); -1 disables. Same-host alternative to the NVMe/TCP loopback")
	fs.IntVar(&f.ublkQueues, "ublk-queues", 0, "ublk hardware queues (one server thread each); 0 = default 1. Requires --ublk-device")
	fs.IntVar(&f.ublkQueueDepth, "ublk-queue-depth", 0, "ublk in-flight requests per queue; 0 = default 64. Requires --ublk-device")
	fs.StringVar(&f.durableRoot, "durable-root", "", "directory for persistent storage files; empty = memback (non-durable)")
	fs.StringVar(&f.durableImpl, "durable-impl", "", "LogicalStorage impl: empty (default) = auto: an existing volume opens with its on-disk format (a walstore-chunk root needs the explicit walstore-chunk selector), a new volume is created as walstore; or walstore, smartwal (experimental), opt-in parallel-walstore, or opt-in walstore-chunk (walstore published to SeaweedFS; proof of concept); ignored unless --durable-root is set")
	registerHostLimitFlags(fs, &f.chunk.hostLimit)
	chunkstore.RegisterSWFSFlags(fs, &f.chunk.filer, &f.chunk.path, &f.chunk.collection, &f.chunk.replication)
	fs.Bool("chunk-detach-plain-supported", false, "capability: supports idle plain=true on /chunk/detach (no-op flag)")
	fs.BoolVar(&f.chunk.detachPrecopy, "chunk-detach-precopy", false, "walstore-chunk: not supported in this release; blockvolume refuses to start when it is set")
	fs.DurationVar(&f.chunk.captureInterval, "chunk-capture-interval", defaultChunkCaptureInterval, "walstore-chunk: periodic HEAD capture interval (default 0: off; when set, at least "+durable.MinCaptureInterval.String()+")")
	f.chunk.snapshotSchedule = defaultSnapshotSchedule
	fs.Var(&f.chunk.snapshotSchedule, "chunk-snapshot-schedule", "walstore-chunk: scheduled snapshots, daily:N or weekly:N (once per period at a per-volume offset, the newest N kept) or off")
	fs.DurationVar(&f.chunk.maintenanceInterval, "chunk-maintenance-interval", defaultChunkMaintenanceInterval, "walstore-chunk: interval of the holder's maintenance pass (retention, then the cleaning); 0 disables the whole pass")
	fs.BoolVar(&f.chunk.automaticGC, "chunk-maintenance-gc", defaultChunkMaintenanceGC, "walstore-chunk: the holder's maintenance pass removes chunks no root of the volume reaches (cleaning), every --chunk-maintenance-interval and after each snapshot delete; on by default, =false turns the cleaning off")
	fs.IntVar(&f.chunk.uploadConcurrency, "chunk-upload-concurrency", 8, "walstore-chunk: maximum concurrent dirty-chunk uploads (1..64)")
	fs.IntVar(&f.chunk.hydrationWorkers, "chunk-hydration-workers", 2, "walstore-chunk: concurrent lazy chunk fetches per volume (1..64)")
	fs.IntVar(&f.chunk.hydrationQueueDepth, "chunk-hydration-queue-depth", 64, "walstore-chunk: distinct queued/in-flight hydration chunks per volume (1..65536)")
	fs.BoolVar(&f.chunk.controlEndpoint, "chunk-control-endpoint", false, "walstore-chunk: expose the /chunk/* control routes (cut, snapshot, snapshots, snapshot/delete, detach) on the status HTTP listener; off by default; loopback-only like the listener")
	fs.UintVar(&f.durableBlocks, "durable-blocks", 2048, "number of blocks per volume on first create (ignored when opening existing)")
	fs.UintVar(&f.durableBlockSize, "durable-blocksize", 4096, "block size in bytes on first create")
	fs.BoolVar(&f.durableWALMultiBlockRecords, "durable-wal-multiblock-records", false, "enable experimental walstore multi-block WAL records; default false; requires --durable-impl=walstore")
	fs.BoolVar(&f.durableWALRecoveryTestDisableFlusher, "durable-wal-recovery-test-disable-flusher", false, "test-only: stop walstore's automatic WAL checkpoint flusher so recovery gates can prove WAL replay; default false; requires --durable-root and --durable-impl=walstore")
	fs.Uint64Var(&f.walRetentionLSNs, "wal-retention-lsns", defaultWALRetentionLSNs,
		"WAL retention window past checkpoint LSN (walstore only; default 10000, "+
			"capped at a quarter of the WAL in block-sized records). "+
			"Zero restores strict checkpoint-driven recycle. "+
			"Non-zero relaxes the recovery-scan recycle gate so a replica lagging "+
			"within the configured envelope still has a catch-up scan path; once a "+
			"replica falls past this window the engine escalates to rebuild. "+
			"Substrate semantics: walstore (append-only) honors this directly; "+
			"smartwal (fixed-size ring) bounds retention via --durable-blocks instead "+
			"and ignores this flag. Recommended starting value for sustained-write "+
			"workloads on walstore: 10000 LSNs (sized to expected replica lag).")
	fs.DurationVar(&f.degradedProbeInterval, "degraded-probe-interval", 5*time.Second,
		"primary-side degraded-peer probe loop interval; 0 = OFF. "+
			"On by default: a replica that fell out of the volume rejoins without an operator. Distinct from --degraded-probe-cooldown-base: this is how often "+
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
	fs.BoolVar(&f.version, "version", false, "print build provenance and exit")
	fs.StringVar(&f.volumeLockDirTestOnly, volumeLockDirTestOnlyFlag, "", "")
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { printVisibleUsage(fs) }
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	memoryFlagSet := false
	fs.Visit(func(fl *flag.Flag) {
		if fl.Name == "memory-limit-mib" {
			memoryFlagSet = true
		}
	})
	if !memoryFlagSet && os.Getenv("GOMEMLIMIT") != "" {
		f.memoryLimitMiB = 0
	}
	if f.memoryLimitMiB < 0 || f.memoryLimitMiB > math.MaxInt64>>20 {
		return flags{}, fmt.Errorf("--memory-limit-mib out of range")
	}
	if f.cpuProfileDir != "" { // before anything starts: run() has no side effect yet
		if !cpuProfileSupported {
			return flags{}, fmt.Errorf("--cpu-profile-dir needs SIGUSR1 (unix only)")
		}
		if err := checkCPUProfileDir(f.cpuProfileDir); err != nil {
			return flags{}, err
		}
	}
	if f.version {
		return f, nil
	}
	if err := checkMasterTLS(f); err != nil {
		return flags{}, err
	}
	if f.volumeLockDirTestOnly != "" && os.Getenv("INVOCATION_ID") != "" {
		return flags{}, fmt.Errorf("--%s is test-only and refused under systemd (INVOCATION_ID is set): the one-process-per-host guarantee needs %s", volumeLockDirTestOnlyFlag, durable.HostVolumeLockDir)
	}
	if f.recoveryMode != "legacy" && f.recoveryMode != "dual-lane" {
		return flags{}, fmt.Errorf("--recovery-mode=%q invalid; want \"legacy\" or \"dual-lane\"", f.recoveryMode)
	}
	if _, _, err := parseReplicationAckProfile(f.replicationAck); err != nil {
		return flags{}, err
	}
	switch f.durableImpl {
	case "", "walstore", "smartwal", "parallel-walstore", "walstore-chunk":
	default:
		return flags{}, fmt.Errorf("--durable-impl=%q invalid; want empty (auto), walstore, smartwal, parallel-walstore, or walstore-chunk", f.durableImpl)
	}
	if f.workspaceLease && (f.durableRoot == "" || f.durableImpl != "walstore-chunk") {
		return flags{}, fmt.Errorf("--workspace-lease requires --durable-root and --durable-impl=walstore-chunk")
	}
	if err := validateChunkFlags(f); err != nil {
		return flags{}, err
	}
	if f.durableWALMultiBlockRecords {
		if f.durableRoot == "" {
			return flags{}, fmt.Errorf("--durable-wal-multiblock-records requires --durable-root")
		}
		if f.durableImpl != "walstore" {
			return flags{}, fmt.Errorf("--durable-wal-multiblock-records requires --durable-impl=walstore")
		}
	}
	if f.durableWALRecoveryTestDisableFlusher {
		if f.durableRoot == "" {
			return flags{}, fmt.Errorf("--durable-wal-recovery-test-disable-flusher requires --durable-root")
		}
		if f.durableImpl != "walstore" {
			return flags{}, fmt.Errorf("--durable-wal-recovery-test-disable-flusher requires --durable-impl=walstore")
		}
	}
	if f.layoutID != "" {
		id, err := layout.ParseVolumeID(f.layoutID)
		if err != nil {
			return flags{}, fmt.Errorf("--layout-id: %w", err)
		}
		if f.durableRoot == "" {
			return flags{}, fmt.Errorf("--layout-id requires --durable-root")
		}
		if err := (layout.Volume{ID: id, Name: f.volumeID, Format: layout.VolumeFormat}).CheckIdentity(); err != nil {
			return flags{}, err
		}
		f.durableRoot = filepath.Join(f.durableRoot, layout.VolumeDir(f.volumeID, id))
	}
	if f.durableRoot != "" {
		if _, _, err := durableCreateGeometry(f); err != nil {
			return flags{}, err
		}
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
		if err := enforceISCSIBindPolicy(f); err != nil {
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
		if f.nvmeTransport != "tcp" && f.nvmeTransport != "rdma" {
			return flags{}, fmt.Errorf("--nvme-transport=%q unsupported; use \"tcp\" or \"rdma\"", f.nvmeTransport)
		}
		if f.nvmeSubsysNQN == "" {
			return flags{}, fmt.Errorf("--nvme-subsysnqn is required when --nvme-listen is set")
		}
		if err := enforceNVMeBindPolicy(f); err != nil {
			return flags{}, err
		}
		if f.nvmeMaxH2C > uint(^uint32(0)) {
			return flags{}, fmt.Errorf("--nvme-max-h2c-data-length=%d exceeds uint32", f.nvmeMaxH2C)
		}
		if f.nvmeTransport == "rdma" && f.nvmeMaxH2C != 0 {
			return flags{}, fmt.Errorf("--nvme-max-h2c-data-length applies only to --nvme-transport=tcp")
		}
		if err := nvme.ValidateMaxH2CDataLength(uint32(f.nvmeMaxH2C)); err != nil {
			return flags{}, fmt.Errorf("--nvme-max-h2c-data-length=%d invalid: %w", f.nvmeMaxH2C, err)
		}
		// Symmetric with iSCSI auto-enable per QA checkpoint-7
		// note: keep the two protocols' safe-default behavior
		// identical so the closure report doesn't have to
		// explain asymmetry.
		if !f.enableT1Readiness {
			fmt.Fprintln(os.Stderr, "blockvolume: nvme enabled: t1-readiness auto-enabled")
		}
		f.enableT1Readiness = true
	} else if f.nvmeMaxH2C != 0 {
		return flags{}, fmt.Errorf("--nvme-max-h2c-data-length requires --nvme-listen")
	}
	if f.ublkDevice >= 0 {
		if f.ublkQueues < 0 || f.ublkQueues > 64 {
			return flags{}, fmt.Errorf("--ublk-queues=%d, want 0..64", f.ublkQueues)
		}
		if f.ublkQueueDepth < 0 || f.ublkQueueDepth > 4096 {
			return flags{}, fmt.Errorf("--ublk-queue-depth=%d, want 0..4096", f.ublkQueueDepth)
		}
		// Like iSCSI and NVMe: the device serves only through a healthy
		// projection.
		if !f.enableT1Readiness {
			fmt.Fprintln(os.Stderr, "blockvolume: ublk enabled: t1-readiness auto-enabled")
		}
		f.enableT1Readiness = true
	} else if f.ublkDevice < -1 {
		return flags{}, fmt.Errorf("--ublk-device=%d, want -1 (off) or a device id >= 0", f.ublkDevice)
	} else if f.ublkQueues != 0 || f.ublkQueueDepth != 0 {
		return flags{}, fmt.Errorf("--ublk-queues/--ublk-queue-depth require --ublk-device")
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

func enforceISCSIBindPolicy(f flags) error {
	if !f.allowExternalISCSIBind {
		return enforceFrontendLoopbackBind("iscsi", f.iscsiListen)
	}
	if f.iscsiCHAPUser == "" || f.iscsiCHAPSecret == "" {
		return fmt.Errorf("--allow-external-iscsi-bind requires CHAP")
	}
	host, _, err := net.SplitHostPort(f.iscsiListen)
	if err != nil {
		return fmt.Errorf("--iscsi-listen %q not host:port: %w", f.iscsiListen, err)
	}
	if host == "" {
		return fmt.Errorf("--iscsi-listen %q has empty host; external iSCSI bind requires an explicit node address", f.iscsiListen)
	}
	if isLocalhostOrLoopbackHost(host) {
		return fmt.Errorf("--iscsi-listen %q is loopback; external iSCSI bind requires a non-loopback node address", f.iscsiListen)
	}
	return nil
}

func enforceNVMeBindPolicy(f flags) error {
	if !f.allowExternalNVMeBind {
		return enforceFrontendLoopbackBind("nvme", f.nvmeListen)
	}
	host, _, err := net.SplitHostPort(f.nvmeListen)
	if err != nil {
		return fmt.Errorf("--nvme-listen %q not host:port: %w", f.nvmeListen, err)
	}
	if host == "" {
		return fmt.Errorf("--nvme-listen %q has empty host; external NVMe/TCP bind requires an explicit node address", f.nvmeListen)
	}
	if isLocalhostOrLoopbackHost(host) {
		return fmt.Errorf("--nvme-listen %q is loopback; external NVMe/TCP bind requires a non-loopback node address", f.nvmeListen)
	}
	return nil
}

func isLocalhostOrLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func main() {
	f, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume:", err)
		os.Exit(2)
	}
	if f.version {
		fmt.Println(buildinfo.Version("blockvolume"))
		return
	}
	if f.memoryLimitMiB > 0 {
		debug.SetMemoryLimit(f.memoryLimitMiB << 20)
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

// ublkReadyLine reports the ublk block device once it serves.
type ublkReadyLine struct {
	Component string `json:"component"`
	Phase     string `json:"phase"`
	Device    string `json:"device"`
}

type nvmeReadyLine struct {
	Component string `json:"component"`
	Phase     string `json:"phase"`
	NvmeAddr  string `json:"nvme_addr"`
	SubsysNQN string `json:"subsys_nqn"`
}

// durableCreateGeometry leaves existing volumes in open-only mode. If the
// observed file disappears before Open, NumBlocks=0 prevents a replacement
// from being created using narrowed or otherwise ignored CLI values.
func durableCreateGeometry(f flags) (uint32, int, error) {
	path := filepath.Join(f.durableRoot, f.volumeID+".bin")
	if _, err := os.Stat(path); err == nil {
		return 0, 0, nil
	} else if !os.IsNotExist(err) {
		return 0, 0, fmt.Errorf("durable volume geometry: %w", err)
	}
	if err := geometry.ValidateBlockGeometry(uint64(f.durableBlocks), uint64(f.durableBlockSize)); err != nil {
		return 0, 0, fmt.Errorf("--durable-blocks/--durable-blocksize: %w", err)
	}
	return uint32(f.durableBlocks), int(f.durableBlockSize), nil
}

func runVolume(f flags, exits *processExit) int {
	// Diagnostics first: parseFlags validated the directory, and nothing
	// has been activated yet, so a failure here leaves nothing to clean up.
	stopProfile, err := startCPUProfileSignal(f.cpuProfileDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume:", err)
		return 1
	}
	defer stopProfile()
	readyCh := make(chan adapter.AssignmentInfo, 16)

	dialTLS, err := masterDialTLS(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume:", err)
		return 1
	}
	hc := hostConfig(f, readyCh)
	hc.MasterTLS = dialTLS
	h, err := volume.New(hc)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume:", err)
		return 1
	}
	if f.durableRoot != "" {
		h.BlockLocalReadiness("awaiting durable recovery")
	}
	h.Start()

	var status *volume.StatusServer
	if f.statusAddr != "" {
		status = volume.NewStatusServer(h.ProjectionView())
		status.SetMasterWait(h.MasterWaitStatus)
		if f.nvmeListen != "" {
			status.SetNVMeListenerReady(false)
		}
		if f.statusRecovery {
			status.EnableRecoveryEndpoint()
		}
		if f.runtimeRebuildEndpoint {
			status.EnableRuntimeEndpoint()
		}
		if f.chunk.controlEndpoint {
			status.EnableChunkControlEndpoint()
		}
		if f.allowExternalStatusBind {
			status.AllowExternalAccess()
		}
		status.SetFrontendCapabilities(nvmeFrontendCapabilities(f.nvmeListen, f.nvmeTransport, false))
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

	// T3b: pick Provider per --durable-root flag. When the flag is
	// unset, fall back to memback (non-durable, legacy behavior).
	// When set, DurableProvider opens (or creates) on-disk storage
	// per --durable-impl and runs Recovery before the frontends
	// accept connections.
	var provider frontend.Provider
	var durableProv *durable.DurableProvider
	var workspaceLayoutHead layout.HeadStore
	if f.durableRoot != "" {
		blocks, blockSize, geometryErr := durableCreateGeometry(f)
		var dp *durable.DurableProvider
		cfg := durable.ProviderConfig{
			Impl:                          durable.ImplName(f.durableImpl),
			StorageRoot:                   f.durableRoot,
			BlockSize:                     blockSize,
			NumBlocks:                     blocks,
			ChunkBlocks:                   durable.DefaultChunkBlocks,
			WALRetentionLSNs:              f.walRetentionLSNs,
			WALMultiBlockRecords:          f.durableWALMultiBlockRecords,
			WALRecoveryTestDisableFlusher: f.durableWALRecoveryTestDisableFlusher,
			// Host-lock rule: one live process per volume per host.
			HostVolumeLock:        true,
			VolumeLockDirTestOnly: f.volumeLockDirTestOnly,
		}
		if f.volumeLockDirTestOnly != "" {
			fmt.Fprintf(os.Stderr, "blockvolume: volume lock directory %s is a test override: the one-process-per-host guarantee does not hold\n", f.volumeLockDirTestOnly)
		}
		var chunkErr error
		var chunk chunkWiring
		if cfg.Impl == durable.ImplWALStoreChunk {
			chunk, chunkErr = newChunkWiring(f.chunk, dialTLS, f.masterAddr)
			cfg.ChunkCaptureInterval = f.chunk.captureInterval
			cfg.ChunkCutTimeout = defaultChunkCutTimeout
			cfg.ChunkMaintenanceInterval = f.chunk.maintenanceInterval
			cfg.ChunkMaintenanceTimeout = defaultChunkMaintenanceTimeout
			if f.workspaceLease {
				// The startup wait does not run out while the master gives no
				// reply; it ends 30 s after the first reply received after the
				// local open and recovery (or the master's barrier time), or at
				// once on a moved/deleted refusal.
				cfg.ChunkMasterWait = func(start time.Time) (time.Time, bool) {
					deadline, final := h.MasterWaitBound(start)
					return deadline, final != ""
				}
			}
			if chunk.bCatalog != nil && f.chunk.snapshotSchedule.period > 0 {
				cfg.ChunkSnapshotPeriod = f.chunk.snapshotSchedule.period
				cfg.ChunkScheduledSnapshot = volume.ScheduledSnapshot(func(ctx context.Context, name, snapshot string, meta layout.SnapshotMeta) (chunkstore.SnapshotInfo, error) {
					p, ok := any(dp).(interface {
						SnapshotVolume(context.Context, string, string, layout.SnapshotMeta) (layout.Head, error)
					})
					if !ok {
						return chunkstore.SnapshotInfo{}, errors.New("chunk snapshot data path is not configured")
					}
					h, err := p.SnapshotVolume(ctx, name, snapshot, meta)
					if err != nil {
						return chunkstore.SnapshotInfo{}, err
					}
					return chunkstore.SnapshotInfoFromHead(name, snapshot, h), nil
				}, f.chunk.snapshotSchedule.keep)
			}
			cfg.ChunkDetachPrecopy = f.chunk.detachPrecopy
			cfg.ChunkUploadConcurrency = f.chunk.uploadConcurrency
			cfg.ChunkHydrationWorkers = f.chunk.hydrationWorkers
			cfg.ChunkHydrationQueueDepth = f.chunk.hydrationQueueDepth
			cfg.ChunkHolder = chunkHolder(f)
			cfg.ChunkStagger = true
			if f.chunk.hostLimit.Directory != "" {
				limitErr := hostlimit.CheckLaunch(f.chunk.hostLimit)
				var limiter *hostlimit.Limiter
				if limitErr == nil {
					limiter, limitErr = hostlimit.New(f.chunk.hostLimit)
				}
				if limitErr != nil {
					chunkErr = errors.Join(chunkErr, limitErr)
				} else {
					defer limiter.Close()
					cfg.ChunkBudget = limiter
					cfg.ChunkPayloadMemoryBudget = f.chunk.hostLimit.PayloadBytes
					cfg.ChunkCOWSpillLimit = f.chunk.hostLimit.COWBytes
				}
			}
			// A detach is a graceful stop: the lease guard renews at a short
			// cadence for the revocation that follows instead of waiting out
			// its renewal delay, until the detach provably aborts and resumes.
			cfg.OnChunkDetach = h.WorkspaceStopPending
			cfg.OnChunkDetachResumed = h.WorkspaceStopResumed
			if chunkErr == nil {

				layoutID, _ := layout.ParseVolumeID(f.layoutID) // parseFlags validates a supplied id
				ref := chunkstore.VolumeRef{Name: f.volumeID, ID: layoutID}
				cfg.ChunkAttachCheck = chunkAttachCheck(chunk.bCatalog, ref)
				cfg.ChunkLayoutHeadFor = func(name string) (layout.StartStore, error) {
					if name != ref.Name {
						return nil, layout.ErrIdentity
					}
					return chunk.bCatalog.Store(ref)
				}
				cfg.ChunkLayoutRemoteFor = func(name string, me layout.HolderID) (storage.ChunkRemote, error) {
					if name != ref.Name {
						return nil, layout.ErrIdentity
					}
					return chunkstore.NewLayoutRemote(chunk.bCatalog, ref, me)
				}
				workspaceLayoutHead, chunkErr = chunk.bCatalog.Store(ref)
				// The holder identity comes from Start (until it is wired, the
				// pass retains and does not clean).
				cfg.ChunkMaintenance = newChunkMaintenance(chunk.bCatalog, ref, func(ctx context.Context) (layoutCleaner, error) {
					if dp == nil {
						return nil, errNoLayoutHolder
					}
					r, err := dp.LayoutRemote(ctx, f.volumeID)
					if err != nil {
						return nil, fmt.Errorf("%w: %w", errNoLayoutHolder, err)
					}
					c, ok := r.(layoutCleaner)
					if !ok {
						return nil, fmt.Errorf("%w: the attachment's remote (%T) cannot clean", errNoLayoutHolder, r)
					}
					return c, nil
				}, f.chunk.automaticGC, time.Now)
				defer chunk.close()
			}
		}
		// Only a store about to be created takes its geometry from the flags;
		// an existing store opens with its own (blocks == 0), and a mismatch
		// between it and the record is the attach check's.
		if chunkErr == nil && geometryErr == nil && blocks != 0 && chunk.bCatalog != nil {
			readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
			layoutID, _ := layout.ParseVolumeID(f.layoutID)
			refusal, code := geometryFlagsRefusal(readCtx, chunk.bCatalog, chunkstore.VolumeRef{Name: f.volumeID, ID: layoutID}, uint64(blocks), uint32(blockSize), cfg.ChunkBlocks)
			readCancel()
			if refusal != "" {
				fmt.Fprintln(os.Stderr, refusal)
				if status != nil {
					shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = status.Close(shutCtx)
					shutCancel()
				}
				_ = h.Close()
				return code
			}
		}
		err := geometryErr
		if err == nil {
			dp, err = durable.NewDurableProvider(cfg, h.ProjectionView())
			if err == nil {
				applyBuildTagHold(dp) // a no-op except in a kzhold (test-only) build
			}
		}
		if chunkErr != nil {
			err = chunkErr
		}
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
		watchCtx, stopWatch := context.WithCancel(exits.parent)
		defer stopWatch()
		go watchChunkStartup(watchCtx, exits, f, h, dp)
		go watchChunkRuntimeFence(watchCtx, exits, f, dp)
		provider = dp
		if status != nil {
			status.SetDurableStatusSource(dp)
			if f.chunk.controlEndpoint && chunk.bCatalog != nil {
				layoutID, _ := layout.ParseVolumeID(f.layoutID)
				status.SetChunkControl(volume.NewChunkControl(dp, chunk.bCatalog, chunkstore.VolumeRef{Name: f.volumeID, ID: layoutID}))
			}
		}

		// Open storage role-agnostically via EnsureStorage so replica
		// roles (assigned to SUPPORTING, never reach Healthy via
		// projection) can still expose LogicalStorage to
		// ReplicaListener. The Healthy-gated dp.Open path is reserved
		// for frontend consumers (iSCSI / NVMe targets); they call it
		// when they accept their first I/O. Primary role: dp.Open
		// will succeed at frontend bind. Replica role: frontends are
		// not enabled (operator doesn't pass --iscsi-listen /
		// --nvme-listen for replicas), so dp.Open is never called.
		// G1: a volume about to be created gets its replica floor record
		// (epoch 0) FIRST, so every later start can require the record and
		// an existing volume is never mistaken for a fresh one. A crash
		// between the two keeps the record; the volume is created next time.
		exists, xErr := dp.VolumeExists(f.volumeID)
		if xErr == nil && !exists {
			xErr = transport.InitReplicaFloor(replicaFloorPath(f), f.volumeID)
		}
		if xErr != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: replica floor init:", xErr)
			_ = dp.Close()
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		}
		// Open and replay (#133): a classifiable failure in either step
		// (wal_corrupt / io_error) faults fail-closed and is reported at
		// once by a primary assignment; see openAndRecoverDurable.
		outcome, evidence, sErr := openAndRecoverDurable(dp, h, f.volumeID, 10*time.Second)
		chooseChunkStartupExit(exits, f, h, dp)
		switch outcome {
		case durableStartupFatal:
			fmt.Fprintln(os.Stderr, "blockvolume: durable storage open:", sErr)
			_ = dp.Close()
			if status != nil {
				shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = status.Close(shutCtx)
				shutCancel()
			}
			_ = h.Close()
			return 1
		case durableStartupFaulted:
			fmt.Fprintln(os.Stderr, "blockvolume: durable recovery failed:", evidence)
			return waitFaultedDurableRecovery(exits.done, h, status, durableProv, evidence)
		default:
			fmt.Fprintln(os.Stderr, "blockvolume: durable recovered:", evidence)
		}
	} else {
		// memback fallback — wraps the volume's AdapterProjectionView
		// so per-op lineage fence + supersede fence still apply at
		// SCSI command granularity.
		provider = memback.NewProvider(h.ProjectionView())
	}
	startReadyAssignmentLoop(readyCh, f, durableProv, os.Stdout, os.Stderr)

	// Bind the replication stack — ReplicationVolume for outbound peer
	// fan-out (primary role) + ReplicaListener for incoming WAL traffic
	// (replica role). Incoming listeners are bound only when the master
	// assignment includes replica slots; RF=1 never opens either lane. Construction
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
		replListen *replicationIngress
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
		if status != nil {
			status.SetPeerStatusSource(replVolume)
			h.SetProbeRoleSource(status.ProbeRoles)
			status.SetRuntimeRecoverySource(replVolume)
		}
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
			peerRegistry.SetFrontier(store.Sync)
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
			probeFn := volume.ProductionProbeFnWithPrime(peerRegistry.AdapterForPeer, peerRegistry.PrimePendingFor)
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
			if err := sb.SetWriteObserver(replVolume); err != nil {
				// The backend now refuses all I/O; the volume cannot serve.
				fmt.Fprintln(os.Stderr, "blockvolume: write-observer wire:", err)
			}
			sb.SetWriteAckPolicy(writeAck)
		} else {
			fmt.Fprintln(os.Stderr, "blockvolume: write-observer wire: Backend(", f.volumeID, ") returned nil; replication fan-out disabled")
		}

		// No replication socket exists until a master assignment includes
		// replica slots. The role gate starts/fences both lanes before admission.
		replListen = newReplicationIngress(f, store)
		replVolume.SetEpochTable(replListen.EpochTable())
		if err := h.SetSelfFence(replListen); err != nil {
			fmt.Fprintln(os.Stderr, "blockvolume: replica listener role:", err)
			replListen.Stop()
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
	var nvmeTarget interface {
		Start() (string, error)
		Close() error
	}
	var ublkTarget *ublk.Target

	// One startup order (INV-FE-PRIMARY-READY-OPERATIONAL-001 with
	// INV-WS-STARTUP-RETRY-001): the workspace lease guard and the process's
	// one stop waiter (waitWorkspaceStop: signal, lease drain request, lease
	// end, or the latched startup outcome) run from here, before any frontend;
	// the frontends start only when the backend first admits I/O, and never
	// if a stop wins that race.
	shutdownCtx := exits.parent
	var leaseRequests <-chan workspaceStopRequest
	var leaseDone <-chan error
	if f.workspaceLease {
		leaseRequests, leaseDone = startWorkspaceLease(shutdownCtx, h)
	}
	stopWait := make(chan workspaceStopWait, 1)
	go func(requests <-chan workspaceStopRequest, done <-chan error) {
		ls, ld := waitWorkspaceStop(exits, f, h, requests, done)
		stopWait <- workspaceStopWait{leaseStop: ls, leaseDone: ld}
	}(leaseRequests, leaseDone)
	var serving <-chan struct{} // nil: no durable backend, start at once
	if durableProv != nil {
		if b := durableProv.Backend(f.volumeID); b != nil {
			serving = frontendStartSignal(f, b, exits.done)
		}
	}
	stopped, startFrontends := awaitFrontendStart(serving, stopWait)
	if startFrontends {
		if f.iscsiListen != "" {
			prov := provider
			var probeProvider iscsi.ProbeBackendProvider
			if durableProv != nil {
				probeProvider = &durableProbeProvider{provider: durableProv}
			}
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
				ProbeProvider:  probeProvider,
				Negotiation:    negotiation,
				DataOutTimeout: f.iscsiDataOutTTL,
				Handler: iscsi.HandlerConfig{
					BlockSize:  frontendBlockSize,
					VolumeSize: frontendVolumeSize,
					ALUA:       newProjectionALUAProvider(h.ProjectionView(), f.volumeID, f.replicaID, f.iscsiIQN),
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

		if f.nvmeListen != "" {
			if f.nvmeTransport == "rdma" {
				start := nvmeRDMAListenerStartDecision(true, nvmeRDMAPreflightFacts(f.nvmeListen))
				if !start.allowed {
					fmt.Fprintf(os.Stderr, "blockvolume: nvme rdma start refused: reason=%s\n", start.reason)
					if iscsiTarget != nil {
						_ = iscsiTarget.Close()
					}
					if replListen != nil {
						replListen.Stop()
					}
					if replVolume != nil {
						_ = replVolume.Close()
					}
					if durableProv != nil {
						_ = durableProv.Close()
					}
					if status != nil {
						shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
						_ = status.Close(shutCtx)
						shutCancel()
					}
					_ = h.Close()
					return 1
				}
				blockSize := frontendBlockSize
				volumeSize := frontendVolumeSize
				if blockSize == 0 {
					blockSize = nvme.DefaultBlockSize
				}
				if volumeSize == 0 {
					volumeSize = nvme.DefaultVolumeBlocks * uint64(blockSize)
				}
				nvmeTarget = nvmerdma.NewTarget(nvmerdma.TargetConfig{
					Listen:     f.nvmeListen,
					SubsysNQN:  f.nvmeSubsysNQN,
					VolumeID:   f.volumeID,
					NSID:       uint32(f.nvmeNS),
					BlockSize:  blockSize,
					VolumeSize: volumeSize,
					Provider:   provider,
				})
			} else {
				var probeProvider nvme.ProbeBackendProvider
				if durableProv != nil {
					probeProvider = &durableProbeProvider{provider: durableProv}
				}
				nvmeTarget = nvme.NewTarget(nvme.TargetConfig{
					Transport:        nvme.TransportTCP,
					Listen:           f.nvmeListen,
					SubsysNQN:        f.nvmeSubsysNQN,
					VolumeID:         f.volumeID,
					Provider:         provider,
					ProbeProvider:    probeProvider,
					ControllerID:     nvmeControllerIDFromReplicaID(f.replicaID),
					MaxH2CDataLength: uint32(f.nvmeMaxH2C),
					Handler: nvme.HandlerConfig{
						NSID:       uint32(f.nvmeNS),
						BlockSize:  frontendBlockSize,
						VolumeSize: frontendVolumeSize,
						ANA:        newProjectionANAProvider(h.ProjectionView(), f.volumeID, f.replicaID, f.nvmeSubsysNQN),
					},
				})
			}
			nvmeAddr, err := nvmeTarget.Start()
			if err != nil {
				if f.nvmeTransport == "rdma" {
					fmt.Fprintf(os.Stderr, "blockvolume: nvme rdma start refused: reason=%s error=%v\n", nvmeRDMATargetStartFailureReason(err), err)
				} else {
					fmt.Fprintln(os.Stderr, "blockvolume: nvme target:", err)
				}
				if iscsiTarget != nil {
					_ = iscsiTarget.Close()
				}
				if replListen != nil {
					replListen.Stop()
				}
				if replVolume != nil {
					_ = replVolume.Close()
				}
				if durableProv != nil {
					_ = durableProv.Close()
				}
				if status != nil {
					shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = status.Close(shutCtx)
					shutCancel()
				}
				_ = h.Close()
				return 1
			}
			if status != nil {
				status.SetNVMeListenerReady(true)
			}
			_ = json.NewEncoder(os.Stdout).Encode(nvmeReadyLine{
				Component: "blockvolume",
				Phase:     "nvme-listening",
				NvmeAddr:  nvmeAddr,
				SubsysNQN: f.nvmeSubsysNQN,
			})
			if shouldPublishNVMeFrontendTarget(f.nvmeTransport) {
				frontendTargets = append(frontendTargets, &control.FrontendTarget{
					Protocol:  "nvme",
					Transport: f.nvmeTransport,
					Addr:      nvmeAddr,
					Nqn:       f.nvmeSubsysNQN,
					Nsid:      uint32(f.nvmeNS),
				})
				h.SetFrontendTargets(frontendTargets)
			}
			if status != nil {
				status.SetFrontendCapabilities(nvmeFrontendCapabilities(f.nvmeListen, f.nvmeTransport, true))
				if source, ok := nvmeTarget.(interface{ Stats() nvme.Stats }); ok {
					status.SetNVMeStatusSource(source)
				}
			}
		}

		if f.ublkDevice >= 0 {
			blockSize := frontendBlockSize
			volumeSize := frontendVolumeSize
			if blockSize == 0 {
				blockSize = nvme.DefaultBlockSize
			}
			if volumeSize == 0 {
				volumeSize = nvme.DefaultVolumeBlocks * uint64(blockSize)
			}
			ublkTarget = ublk.NewTarget(ublk.TargetConfig{
				VolumeID:   f.volumeID,
				Provider:   provider,
				DevID:      f.ublkDevice,
				BlockSize:  blockSize,
				VolumeSize: volumeSize,
				Queues:     f.ublkQueues,
				QueueDepth: f.ublkQueueDepth,
			})
			ublkPath, err := ublkTarget.Start()
			if err != nil {
				fmt.Fprintln(os.Stderr, "blockvolume: ublk device:", err)
				if nvmeTarget != nil {
					_ = nvmeTarget.Close()
				}
				if iscsiTarget != nil {
					_ = iscsiTarget.Close()
				}
				if replListen != nil {
					replListen.Stop()
				}
				if replVolume != nil {
					_ = replVolume.Close()
				}
				if durableProv != nil {
					_ = durableProv.Close()
				}
				if status != nil {
					shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = status.Close(shutCtx)
					shutCancel()
				}
				_ = h.Close()
				return 1
			}
			if status != nil {
				status.SetUblkStatusSource(ublkTarget)
			}
			_ = json.NewEncoder(os.Stdout).Encode(ublkReadyLine{
				Component: "blockvolume",
				Phase:     "ublk-ready",
				Device:    ublkPath,
			})
		}
	} // startFrontends

	// The one stop waiter has already returned if a stop won the race above.
	if stopped == nil {
		w := <-stopWait
		stopped = &w
	}
	leaseStop, leaseDone := stopped.leaseStop, stopped.leaseDone
	// The resource owner performs shutdown. The lease callback waits for this
	// exact drain; it cannot ACK a merely requested or failed close.
	h.FenceWorkspaceLease()
	var drainErr error
	if leaseStop != nil && durableProv != nil {
		drainErr = durableProv.Backend(f.volumeID).StopIO(leaseStop.ctx, "workspace lease fenced")
	}
	h.SetFrontendTargets(nil)
	// The ublk device drains with the other frontends: its Close (STOP_DEV,
	// queue join, DEL_DEV) is part of the drain the lease ACK waits for.
	if ublkTarget != nil {
		drainErr = errors.Join(drainErr, ublkTarget.Close())
	}
	if nvmeTarget != nil {
		if status != nil {
			status.SetNVMeListenerReady(false)
		}
		drainErr = errors.Join(drainErr, nvmeTarget.Close())
	}
	if iscsiTarget != nil {
		drainErr = errors.Join(drainErr, iscsiTarget.Close())
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
		drainErr = errors.Join(drainErr, replVolume.Close())
	}
	// A detached HEAD is stable until a higher grant. Read it while the
	// attachment identity is still available, then ACK only after Close succeeds.
	var detachedGen uint64
	if leaseStop != nil && drainErr == nil && workspaceLayoutHead != nil {
		var me layout.HolderID
		me, drainErr = durableProv.LayoutHolder(leaseStop.ctx, leaseStop.id.VolumeId)
		if drainErr == nil {
			detachedGen, drainErr = workspaceLayoutDetachedGeneration(leaseStop.ctx, workspaceLayoutHead, leaseStop.id, me)
		}
	}
	// T3b: close DurableProvider in correct order — Provider.Close
	// closes backends first (flags them closed) then storage files
	// (releases fd + final fsync).
	if durableProv != nil {
		drainErr = errors.Join(drainErr, durableProv.Close())
	}
	if status != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = status.Close(shutCtx)
		shutCancel()
	}
	if leaseStop != nil {
		gen := detachedGen
		if drainErr == nil && workspaceLayoutHead == nil {
			drainErr = errors.New("workspace layout HEAD unavailable for detached proof")
		}
		leaseStop.reply <- workspaceStopResult{gen: gen, err: drainErr}
	}
	finishWorkspaceLease(exits, f, h, leaseDone)
	if err := h.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume: close:", err)
		return 1
	}
	return 0
}

func shouldPublishNVMeFrontendTarget(transport string) bool {
	return transport == "tcp" || transport == "rdma"
}

func nvmeRDMATargetStartFailureReason(err error) string {
	message := err.Error()
	switch {
	case strings.Contains(message, "port ID already exists"):
		return "rdma_port_conflict"
	case strings.Contains(message, "subsystem already exists"):
		return "rdma_subsystem_conflict"
	case strings.Contains(message, "cannot assign requested address"):
		return "rdma_bind_address_unassigned"
	case strings.Contains(message, "permission denied"), strings.Contains(message, "kernel target requires root"):
		return "rdma_target_permission_denied"
	default:
		return "rdma_target_start_failed"
	}
}

func nvmeFrontendCapabilities(nvmeListen, selectedTransport string, listenerStarted bool) []volume.FrontendTransportCapability {
	return nvmeFrontendCapabilitiesWithRDMAPreflight(nvmeListen, selectedTransport, listenerStarted, nvmeRDMAPreflightFacts(nvmeListen))
}

func nvmeFrontendCapabilitiesWithRDMAPreflight(nvmeListen, selectedTransport string, listenerStarted bool, rdmaFacts []volume.FrontendTransportPreflightFact) []volume.FrontendTransportCapability {
	tcpStarted := listenerStarted && nvmeListen != "" && selectedTransport == "tcp"
	rdmaSelected := selectedTransport == "rdma"
	rdmaStarted := listenerStarted && nvmeListen != "" && rdmaSelected && nvmerdma.Implemented()
	rdmaStart := nvmeRDMAListenerStartDecision(rdmaSelected, rdmaFacts)
	rdmaReason := "nvme_rdma_transport_unsupported"
	if rdmaStarted {
		rdmaReason = "implemented"
	}
	return []volume.FrontendTransportCapability{
		{
			Protocol:            "nvme",
			Transport:           "tcp",
			Supported:           true,
			ListenerImplemented: true,
			ListenerStarted:     tcpStarted,
			StartAllowed:        true,
			StartReason:         "implemented",
			Reason:              "implemented",
		},
		{
			Protocol:            "nvme",
			Transport:           "rdma",
			Supported:           rdmaStarted,
			ListenerImplemented: nvmerdma.Implemented(),
			ListenerStarted:     rdmaStarted,
			StartAllowed:        rdmaStart.allowed,
			StartReason:         rdmaStart.reason,
			Reason:              rdmaReason,
			Preflight:           append([]volume.FrontendTransportPreflightFact(nil), rdmaFacts...),
		},
	}
}

type nvmeRDMAListenerStart struct {
	allowed bool
	reason  string
}

func nvmeRDMAListenerStartDecision(enabled bool, facts []volume.FrontendTransportPreflightFact) nvmeRDMAListenerStart {
	if !enabled {
		return nvmeRDMAListenerStart{allowed: false, reason: "nvme_rdma_listener_disabled"}
	}
	for _, fact := range facts {
		// nvme-rdma is an initiator module. Keep it visible as a host fact, but
		// do not require it on a target that serves through nvmet-rdma.
		if fact.Name == "nvme_rdma_module" {
			continue
		}
		if !fact.Available {
			return nvmeRDMAListenerStart{allowed: false, reason: fact.Reason}
		}
	}
	if !nvmerdma.Implemented() {
		return nvmeRDMAListenerStart{allowed: false, reason: "nvme_rdma_transport_unsupported"}
	}
	return nvmeRDMAListenerStart{allowed: true, reason: "implemented"}
}

func nvmeRDMAPreflightFacts(nvmeListen string) []volume.FrontendTransportPreflightFact {
	return []volume.FrontendTransportPreflightFact{
		nvmeRDMAModuleFact(),
		kernelModuleFact("nvmet-rdma", "nvmet_rdma_module"),
		kernelModuleFact("nbd", "nbd_module"),
		rdmaDeviceFact(),
		rdmaBindAddressFact(nvmeListen),
		configFSFact(),
	}
}

func nvmeRDMAModuleFact() volume.FrontendTransportPreflightFact {
	return kernelModuleFact("nvme-rdma", "nvme_rdma_module")
}

func kernelModuleFact(module, factName string) volume.FrontendTransportPreflightFact {
	sysName := strings.ReplaceAll(module, "-", "_")
	if _, err := os.Stat("/sys/module/" + sysName); err == nil {
		return volume.FrontendTransportPreflightFact{Name: factName, Available: true, Reason: factName + "_loaded"}
	}
	if data, err := os.ReadFile("/proc/modules"); err == nil && strings.Contains(string(data), sysName+" ") {
		return volume.FrontendTransportPreflightFact{Name: factName, Available: true, Reason: factName + "_loaded"}
	}
	if err := exec.Command("modinfo", module).Run(); err == nil {
		return volume.FrontendTransportPreflightFact{Name: factName, Available: true, Reason: factName + "_available"}
	}
	return volume.FrontendTransportPreflightFact{Name: factName, Available: false, Reason: factName + "_missing"}
}

func configFSFact() volume.FrontendTransportPreflightFact {
	if _, err := os.Stat("/sys/kernel/config"); err == nil {
		return volume.FrontendTransportPreflightFact{Name: "configfs", Available: true, Reason: "configfs_available"}
	}
	return volume.FrontendTransportPreflightFact{Name: "configfs", Available: false, Reason: "configfs_missing"}
}

func rdmaDeviceFact() volume.FrontendTransportPreflightFact {
	entries, err := os.ReadDir("/sys/class/infiniband")
	if err != nil || len(entries) == 0 {
		return volume.FrontendTransportPreflightFact{Name: "rdma_device", Available: false, Reason: "rdma_device_missing"}
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return volume.FrontendTransportPreflightFact{Name: "rdma_device", Available: true, Reason: "rdma_device_present", Detail: strings.Join(names, ",")}
}

func rdmaBindAddressFact(listen string) volume.FrontendTransportPreflightFact {
	host := listen
	if h, _, err := net.SplitHostPort(listen); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() || ip.IsLoopback() {
		return volume.FrontendTransportPreflightFact{Name: "rdma_bind_address", Available: false, Reason: "rdma_bind_address_invalid", Detail: host}
	}
	return volume.FrontendTransportPreflightFact{Name: "rdma_bind_address", Available: true, Reason: "rdma_bind_address_candidate", Detail: host}
}

func waitFaultedDurableRecovery(stop <-chan struct{}, h *volume.Host, status *volume.StatusServer, durableProv *durable.DurableProvider, evidence string) int {
	fmt.Fprintln(os.Stderr, "blockvolume: durable recovery faulted; status endpoint remains available and local readiness remains blocked:", evidence)
	<-stop

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

// replicaFloorPath is where the replica's epoch floor record lives (G1):
// beside the volume's durable files, or "" for non-durable memback runs.
// hostConfig is the volume host's configuration from the flags.
func hostConfig(f flags, readyCh chan<- adapter.AssignmentInfo) volume.Config {
	var ident storeident.Identity
	if f.durableRoot != "" {
		ident = storeident.Of(f.durableRoot)
	}
	return volume.Config{
		MasterAddr:            f.masterAddr,
		AckProfile:            strings.ReplaceAll(f.replicationAck, "-", "_"),
		LinesNameTheirProcess: true,
		ServerID:              f.serverID,
		VolumeID:              f.volumeID,
		ReplicaID:             f.replicaID,
		DataAddr:              f.dataAddr,
		CtrlAddr:              f.ctrlAddr,
		HeartbeatInterval:     f.hbInterval,
		RecoverTimeout:        f.recoverTimeout,
		ReadyMarker:           readyCh,
		EnableT1Readiness:     f.enableT1Readiness,
		WorkspaceLease:        f.workspaceLease,
		RF1Only:               rf1Only(f),
		StorageIdentity:       ident,
	}
}

// epochTablePath is where the volume's epoch table lives, beside the floor
// record ("" for non-durable memback runs: in memory only).
func epochTablePath(f flags) string {
	if f.durableRoot == "" {
		return ""
	}
	return filepath.Join(f.durableRoot, f.volumeID+".epoch-table")
}

func replicaFloorPath(f flags) string {
	if f.durableRoot == "" {
		return ""
	}
	return filepath.Join(f.durableRoot, f.volumeID+".replica-floor")
}

// defaultWALRetentionLSNs keeps a replica a few thousand writes behind (a
// restart, a fence of a healthy peer) on the catch-up path instead of a
// rebuild. The store caps it to what its WAL can hold.
const defaultWALRetentionLSNs = 10000
