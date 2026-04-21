// Command blockvolume is the block volume daemon — the P15 beta
// product host for per-volume observation reporting and
// assignment subscription. See sw-block/design/v3-phase-15-t0-sketch.md
// §4.1 for the product-daemon contract.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
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
	return f, nil
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
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume:", err)
		return 1
	}
	h.Start()

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

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	if err := h.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "blockvolume: close:", err)
		return 1
	}
	return 0
}
