// Command blockcsi is the CSI daemon. Current scope is static: it
// consumes already-published frontend target facts from blockmaster
// and mounts them through the node path; it does not create volumes
// or mint authority.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	blockcsi "github.com/seaweedfs/seaweed-block/core/csi"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type flags struct {
	endpoint       string
	masterAddr     string
	nodeID         string
	iqnPrefix      string
	printReadyLine bool
}

func parseFlags(args []string) (flags, error) {
	var f flags
	fs := flag.NewFlagSet("blockcsi", flag.ContinueOnError)
	fs.StringVar(&f.endpoint, "endpoint", "unix:///var/lib/kubelet/plugins/block.csi.seaweedfs.com/csi.sock", "CSI endpoint, e.g. unix:///path/csi.sock or tcp://127.0.0.1:0")
	fs.StringVar(&f.masterAddr, "master", "", "blockmaster gRPC address for read-only publish-target lookup")
	fs.StringVar(&f.nodeID, "node-id", "", "CSI node ID; defaults to hostname")
	fs.StringVar(&f.iqnPrefix, "iqn-prefix", "iqn.2026-05.io.seaweedfs", "fallback IQN prefix for unstage after plugin restart")
	fs.BoolVar(&f.printReadyLine, "t0-print-ready", false, "internal test-only: emit one structured JSON ready line after listener bind")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return flags{}, err
	}
	if f.endpoint == "" {
		return flags{}, fmt.Errorf("--endpoint is required")
	}
	if f.nodeID == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			return flags{}, fmt.Errorf("--node-id is required when hostname is unavailable")
		}
		f.nodeID = host
	}
	return f, nil
}

func main() {
	f, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockcsi:", err)
		os.Exit(2)
	}
	os.Exit(run(f))
}

type readyLine struct {
	Component string `json:"component"`
	Phase     string `json:"phase"`
	Endpoint  string `json:"endpoint"`
	Network   string `json:"network"`
	Addr      string `json:"addr"`
}

func run(f flags) int {
	network, addr, err := blockcsi.ParseEndpoint(f.endpoint)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockcsi:", err)
		return 2
	}
	cleanup, err := prepareEndpoint(network, addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockcsi:", err)
		return 1
	}
	defer cleanup()

	ln, err := net.Listen(network, addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "blockcsi: listen:", err)
		return 1
	}
	defer ln.Close()

	var (
		masterConn  *grpc.ClientConn
		lookup      blockcsi.PublishTargetLookup
		provisioner blockcsi.VolumeProvisioner
	)
	if f.masterAddr != "" {
		masterConn, err = grpc.NewClient(f.masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, "blockcsi: master dial:", err)
			return 1
		}
		defer masterConn.Close()
		lookup = blockcsi.NewControlStatusLookup(control.NewEvidenceServiceClient(masterConn))
		provisioner = blockcsi.NewControlLifecycleProvisioner(control.NewLifecycleServiceClient(masterConn))
	}

	srv := grpc.NewServer()
	csipb.RegisterIdentityServer(srv, blockcsi.NewIdentityServer())
	csipb.RegisterControllerServer(srv, blockcsi.NewControllerServerWithProvisioner(lookup, provisioner))
	csipb.RegisterNodeServer(srv, blockcsi.NewDefaultNodeServer(f.nodeID, f.iqnPrefix))

	if f.printReadyLine {
		_ = json.NewEncoder(os.Stdout).Encode(readyLine{
			Component: "blockcsi",
			Phase:     "listening",
			Endpoint:  f.endpoint,
			Network:   network,
			Addr:      ln.Addr().String(),
		})
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil && err != grpc.ErrServerStopped {
			fmt.Fprintln(os.Stderr, "blockcsi: serve:", err)
			return 1
		}
	case <-sig:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			srv.Stop()
		}
	}
	return 0
}

func prepareEndpoint(network, addr string) (func(), error) {
	if network != "unix" {
		return func() {}, nil
	}
	if err := os.MkdirAll(filepath.Dir(addr), 0o755); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove stale socket: %w", err)
	}
	return func() { _ = os.Remove(addr) }, nil
}
