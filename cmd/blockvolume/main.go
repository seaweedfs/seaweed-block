// Command blockvolume is the block volume daemon — the P15 beta
// product host for per-volume observation reporting and
// assignment subscription. See sw-block/design/v3-phase-15-t0-sketch.md
// §4.1 for the product-daemon contract.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
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
	// statusRecovery — G5-5 opt-in: enable /status/recovery?volume=v1
	// endpoint exposing engine.ReplicaProjection (Mode/R/S/H/Decision).
	// Default off; production binaries do NOT enable. Used by the m01
	// orchestration script for catch-up verification (R/H polling).
	statusRecovery bool

	// T2 iSCSI frontend flags. iscsiListen is the TCP address
	// the iSCSI target binds on; empty disables the frontend.
	// The bind is rejected at startup if it is not a loopback
	// address — unauthenticated frontend on an external port
	// would be a T2-scope safety regression. Per T2 assignment
	// §3.2 "Defaults must be safe": no auth → loopback only.
	iscsiListen string
	iscsiIQN    string
	iscsiLUN    uint

	// T2 NVMe/TCP frontend flags. Symmetric with iSCSI flags
	// per QA note: same loopback-only safe-default rule, same
	// auto-enable of --t1-readiness so the symmetry stays clean.
	nvmeListen   string
	nvmeSubsysNQN string
	nvmeNS       uint

	// T3b durable-backend flags. When --durable-root is set, the
	// iSCSI and NVMe providers use DurableProvider instead of
	// memback. --durable-impl selects walstore or smartwal
	// (default smartwal per PM direction). --durable-blocks +
	// --durable-blocksize are used on first-time storage create.
	durableRoot      string
	durableImpl      string // "smartwal" | "walstore"
	durableBlocks    uint
	durableBlockSize uint

	// G5-5C degraded-peer probe loop (primary-side runtime recovery
	// trigger). Off by default; --degraded-probe-interval=0 keeps the
	// loop disabled so existing G5-4 deployments behave unchanged.
	// When enabled (interval > 0), the loop iterates over peers in
	// ReplicaDegraded at the given interval and dispatches an
	// engine-ingress probe per peer (with per-peer cooldown). See
	// architect mini-plan v0.5 §1.A binding 2026-04-27.
	degradedProbeInterval     time.Duration
	degradedProbeCooldownBase time.Duration
	degradedProbeCooldownCap  time.Duration
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
	fs.BoolVar(&f.enableT1Readiness, "t1-readiness", false, "enable T1 readiness bridge (HealthyPathExecutor) so adapter projection reaches Healthy")
	fs.StringVar(&f.statusAddr, "status-addr", "", "address for the status HTTP endpoint (e.g. 127.0.0.1:0); empty disables")
	fs.BoolVar(&f.statusRecovery, "status-recovery", false, "G5-5 opt-in: expose /status/recovery?volume=<id> with engine.ReplicaProjection (Mode/R/S/H/RecoveryDecision); off by default; loopback-only; intended for hardware test orchestration")
	fs.StringVar(&f.iscsiListen, "iscsi-listen", "", "iSCSI target bind address (e.g. 127.0.0.1:0); empty disables. Loopback-only in T2 scope (no auth)")
	fs.StringVar(&f.iscsiIQN, "iscsi-iqn", "", "iSCSI target IQN (required if --iscsi-listen is set)")
	fs.UintVar(&f.iscsiLUN, "iscsi-lun", 0, "iSCSI LUN id (default 0)")
	fs.StringVar(&f.nvmeListen, "nvme-listen", "", "NVMe/TCP target bind address (e.g. 127.0.0.1:0); empty disables. Loopback-only in T2 scope (no auth)")
	fs.StringVar(&f.nvmeSubsysNQN, "nvme-subsysnqn", "", "NVMe subsystem NQN (required if --nvme-listen is set)")
	fs.UintVar(&f.nvmeNS, "nvme-ns", 1, "NVMe namespace id (default 1)")
	fs.StringVar(&f.durableRoot, "durable-root", "", "directory for persistent storage files; empty = memback (non-durable)")
	fs.StringVar(&f.durableImpl, "durable-impl", "smartwal", "LogicalStorage impl: smartwal (default) or walstore; ignored unless --durable-root is set")
	fs.UintVar(&f.durableBlocks, "durable-blocks", 2048, "number of blocks per volume on first create (ignored when opening existing)")
	fs.UintVar(&f.durableBlockSize, "durable-blocksize", 4096, "block size in bytes on first create")
	fs.DurationVar(&f.degradedProbeInterval, "degraded-probe-interval", 0,
		"G5-5C primary-side degraded-peer probe loop interval; 0 = OFF (default; existing G5-4 behavior preserved). "+
			"Recommended production value: 5s. Distinct from --degraded-probe-cooldown-base: this is how often "+
			"the loop wakes up; cooldown-base is how long a single peer is gated AFTER a probe attempt.")
	fs.DurationVar(&f.degradedProbeCooldownBase, "degraded-probe-cooldown-base", 5*time.Second,
		"G5-5C per-peer cooldown after a probe attempt (also the reset value after a successful probe). "+
			"Doubled on consecutive failures up to --degraded-probe-cooldown-cap. "+
			"Ignored when --degraded-probe-interval=0.")
	fs.DurationVar(&f.degradedProbeCooldownCap, "degraded-probe-cooldown-cap", 60*time.Second,
		"G5-5C per-peer cooldown ceiling for the consecutive-failure backoff. "+
			"Must be >= --degraded-probe-cooldown-base; values below base are normalized up to base. "+
			"Ignored when --degraded-probe-interval=0.")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return flags{}, err
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

// enforceFrontendLoopbackBind mirrors volume.StatusServer's
// guard for any T2 frontend (iscsi / nvme): refuses to bind on
// anything other than 127.0.0.1 / ::1. T2 frontends are
// unauthenticated (sketch §6); real-network exposure lands
// with T8 (security).
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
		return fmt.Errorf("--%s-listen %q is not loopback; T2 %s target is unauthenticated and refuses external binds", kind, addr, kind)
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
			Impl:        durable.ImplName(f.durableImpl),
			StorageRoot: f.durableRoot,
			BlockSize:   int(f.durableBlockSize),
			NumBlocks:   uint32(f.durableBlocks),
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

		// G5-4: open storage role-agnostically via EnsureStorage so
		// replica roles (assigned to SUPPORTING, never reach Healthy
		// via projection) can still expose LogicalStorage to
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

	// G5-4: bind the T4 replication stack — ReplicationVolume for
	// outbound peer fan-out (primary role) + ReplicaListener for
	// incoming WAL traffic (replica role). Both code paths are bound
	// at startup; whichever role the master assignment selects is the
	// active one (INV-BIN-WIRING-ROLE-FROM-ASSIGNMENT). Construction
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
		h.SetReplicationVolume(replVolume)

		// G5-5C: configure + start the degraded-peer probe loop if
		// the operator opted in via --degraded-probe-interval > 0.
		// Off by default — preserves G5-4 behavior. Architect-bound
		// 2026-04-27 §1.A: probe loop owned by ReplicationVolume
		// lifecycle; volume.Close stops the loop before peer
		// teardown (already implemented in core/replication).
		//
		// Self-check items (architect Batch #5 review 2026-04-27):
		//   #1 interval=0 → don't Configure (loop stays absent)
		//   #2 cooldown-base/cap normalized in NewProbeLoop (cap < base
		//      is bumped up to base; 0 → defaults)
		//   #3 production probeFn from volume.ProductionProbeFn —
		//      releases peer.mu before transport call (loop discipline);
		//      transport failure → non-nil err (advances backoff);
		//      transport success → nil err (resets cooldown).
		//   #4 Configure→Start ordering — Configure here, Start
		//      ALSO here (peers may or may not exist yet; empty peer
		//      set is safely no-op per Batch 3 HappyPath).
		if f.degradedProbeInterval > 0 {
			// G5-5C Batch #7: per-peer adapter registry is the engine
			// state holder for each admitted peer (NOT for the host's
			// own slot, which already has h.Adapter()). The probe loop's
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
				MaxConcurrent: 1, // architect-bound v0.5 — only 1 supported
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
		// accessor (G5-4: same StorageBackend instance dp.Open returns
		// later for primary-role frontend bind). Without this hook,
		// primary writes would land locally but never ship to peers.
		if sb := durableProv.Backend(f.volumeID); sb != nil {
			sb.SetWriteObserver(replVolume)
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
	}

	var iscsiTarget *iscsi.Target
	if f.iscsiListen != "" {
		prov := provider
		iscsiTarget = iscsi.NewTarget(iscsi.TargetConfig{
			Listen:   f.iscsiListen,
			IQN:      f.iscsiIQN,
			VolumeID: f.volumeID,
			Provider: prov,
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
	}

	var nvmeTarget *nvme.Target
	if f.nvmeListen != "" {
		prov := provider
		nvmeTarget = nvme.NewTarget(nvme.TargetConfig{
			Listen:    f.nvmeListen,
			SubsysNQN: f.nvmeSubsysNQN,
			VolumeID:  f.volumeID,
			Provider:  prov,
			Handler:   nvme.HandlerConfig{NSID: uint32(f.nvmeNS)},
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
	// G5-4: tear down replication BEFORE durable storage close
	// (INV-BIN-WIRING-LISTENER-LIFECYCLE-LIFO). Listener closes its
	// accept loop + in-flight handler conns; ReplicationVolume closes
	// its outbound peer connections. Both borrow LogicalStorage that
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
