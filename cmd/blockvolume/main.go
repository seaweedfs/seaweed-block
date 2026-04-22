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
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
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
	fs.StringVar(&f.iscsiListen, "iscsi-listen", "", "iSCSI target bind address (e.g. 127.0.0.1:0); empty disables. Loopback-only in T2 scope (no auth)")
	fs.StringVar(&f.iscsiIQN, "iscsi-iqn", "", "iSCSI target IQN (required if --iscsi-listen is set)")
	fs.UintVar(&f.iscsiLUN, "iscsi-lun", 0, "iSCSI LUN id (default 0)")
	fs.StringVar(&f.nvmeListen, "nvme-listen", "", "NVMe/TCP target bind address (e.g. 127.0.0.1:0); empty disables. Loopback-only in T2 scope (no auth)")
	fs.StringVar(&f.nvmeSubsysNQN, "nvme-subsysnqn", "", "NVMe subsystem NQN (required if --nvme-listen is set)")
	fs.UintVar(&f.nvmeNS, "nvme-ns", 1, "NVMe namespace id (default 1)")
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

	var iscsiTarget *iscsi.Target
	if f.iscsiListen != "" {
		// memback provider wraps the volume's AdapterProjectionView
		// so per-op lineage fence + supersede fence still apply at
		// SCSI command granularity.
		prov := memback.NewProvider(h.ProjectionView())
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
		prov := memback.NewProvider(h.ProjectionView())
		nvmeTarget = nvme.NewTarget(nvme.TargetConfig{
			Listen:    f.nvmeListen,
			SubsysNQN: f.nvmeSubsysNQN,
			VolumeID:  f.volumeID,
			Provider:  prov,
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
