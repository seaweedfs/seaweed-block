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
	if len(args) < 2 || args[1] != "status" {
		fmt.Fprintln(stderr, "sw-block: expected subcommand ops status")
		usage(stderr)
		return ops.VolumeStatusExitInvalid
	}
	return runOpsStatus(args[2:], stdout, stderr)
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
	fs.StringVar(&outDir, "out", "", "directory for volume-status-report.json and volume-status-summary.txt")
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
	})
	report, code, err := ops.WriteVolumeStatusArtifacts(ctx, outDir, collector)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops status: %v\n", err)
	}
	fmt.Fprint(stdout, ops.RenderVolumeStatusSummary(report))
	if code != ops.VolumeStatusExitInvalid {
		fmt.Fprintf(stdout, "artifacts: %s %s\n", ops.VolumeStatusReportArtifact, ops.VolumeStatusSummaryArtifact)
	}
	return code
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  sw-block --version")
	fmt.Fprintln(w, "  sw-block ops status --volume <id> --master <addr> --status-addr <addr|url> --out <dir>")
}
