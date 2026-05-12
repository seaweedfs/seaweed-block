package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/internal/buildinfo"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

var opsStatusRunCommand = ops.DefaultRunCommand

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
	if args[0] == "--version" || args[0] == "version" {
		fmt.Fprintln(stdout, buildinfo.Version("sw-block"))
		return ops.VolumeStatusExitOK
	}
	if args[0] != "ops" {
		fmt.Fprintf(stderr, "sw-block: unknown command %q\n", args[0])
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
	if len(args) < 2 {
		fmt.Fprintln(stderr, "sw-block: expected subcommand ops status|inventory|list")
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
	switch args[1] {
	case "status":
		return runOpsStatus(args[2:], stdout, stderr)
	case "inventory", "list":
		return runOpsInventory(args[2:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "sw-block: unknown ops subcommand %q\n", args[1])
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
}

func runOpsStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		volumeID        string
		masterAddr      string
		statusAddr      string
		outDir          string
		productRevision string
		runnerRevision  string
		timeout         time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id to inspect")
	fs.StringVar(&masterAddr, "master", "", "blockmaster gRPC address for read-only QueryVolumeStatus")
	fs.StringVar(&statusAddr, "status-addr", "", "blockvolume loopback status address or URL")
	fs.StringVar(&outDir, "out", "", "directory for volume-status-report.json, volume-status-summary.txt, and ops-status-bundle.json")
	fs.StringVar(&productRevision, "product-revision", "", "product revision label to include in the report")
	fs.StringVar(&runnerRevision, "runner-revision", "", "runner revision label to include in the report")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "collection timeout")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if volumeID == "" || outDir == "" {
		fmt.Fprintln(stderr, "sw-block ops status: --volume and --out are required")
		return ops.VolumeStatusExitInvalid
	}
	if masterAddr == "" || statusAddr == "" {
		fmt.Fprintln(stderr, "sw-block ops status: --master and --status-addr are both required for a clean live report")
		return ops.VolumeStatusExitInvalid
	}
	if productRevision == "" {
		productRevision = buildinfo.Version("sw-block")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	collector := ops.NewLiveVolumeStatusReportCollector(ops.LiveVolumeStatusConfig{
		VolumeID:        volumeID,
		MasterAddr:      masterAddr,
		StatusAddr:      statusAddr,
		ProductRevision: productRevision,
		RunnerRevision:  runnerRevision,
		RunCommand:      opsStatusRunCommand,
	})
	report, code, err := ops.WriteVolumeStatusArtifacts(ctx, outDir, collector)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops status: %v\n", err)
	}
	fmt.Fprint(stdout, ops.RenderVolumeStatusSummary(report))
	if code != ops.VolumeStatusExitInvalid {
		fmt.Fprintf(stdout, "artifacts: %s %s %s\n", ops.VolumeStatusReportArtifact, ops.VolumeStatusSummaryArtifact, ops.OpsStatusBundleArtifact)
	}
	return code
}

func runOpsInventory(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops inventory", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		namespace       string
		outDir          string
		productRevision string
		runnerRevision  string
		timeout         time.Duration
	)
	fs.StringVar(&namespace, "namespace", "default", "Kubernetes namespace to inspect once live discovery is enabled")
	fs.StringVar(&outDir, "out", "", "directory for volume-inventory.json, volume-inventory-summary.txt, and ops-inventory-bundle.json")
	fs.StringVar(&productRevision, "product-revision", "", "product revision label to include in the inventory")
	fs.StringVar(&runnerRevision, "runner-revision", "", "runner revision label to include in the inventory")
	fs.DurationVar(&timeout, "timeout", 5*time.Second, "collection timeout")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if outDir == "" {
		fmt.Fprintln(stderr, "sw-block ops inventory: --out is required")
		return ops.VolumeStatusExitInvalid
	}
	if productRevision == "" {
		productRevision = buildinfo.Version("sw-block")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	collector := ops.StaticVolumeInventoryCollector(ops.VolumeInventoryInput{
		Source:          ops.ReportSource{Component: "sw-block ops inventory", Scenario: "namespace=" + namespace},
		ProductRevision: productRevision,
		RunnerRevision:  runnerRevision,
	})
	inventory, code, err := ops.WriteVolumeInventoryArtifacts(ctx, outDir, collector)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops inventory: %v\n", err)
	}
	fmt.Fprint(stdout, ops.RenderVolumeInventorySummary(inventory))
	if code != ops.VolumeStatusExitInvalid {
		fmt.Fprintf(stdout, "artifacts: %s %s %s\n", ops.VolumeInventoryArtifact, ops.VolumeInventorySummaryArtifact, ops.OpsInventoryBundleArtifact)
	}
	return code
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  sw-block --version")
	fmt.Fprintln(w, "  sw-block ops status --volume <id> --master <addr> --status-addr <addr|url> --out <dir>")
	fmt.Fprintln(w, "  sw-block ops inventory --namespace <ns> --out <dir>")
}
