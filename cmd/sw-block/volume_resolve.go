package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runOpsVolume exposes the same master name binding used by ops chunk.
func runOpsVolume(args []string, stdout, stderr io.Writer) int {
	const usage = "usage: sw-block ops volume resolve --master <addr> [--timeout 10s] <name>"
	if len(args) == 0 || args[0] != "resolve" {
		fmt.Fprintln(stderr, usage)
		return ops.VolumeStatusExitInvalid
	}
	fs := flag.NewFlagSet("sw-block ops volume resolve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprintln(stderr, usage) }
	master := fs.String("master", "", "master gRPC address")
	timeout := fs.Duration("timeout", 10*time.Second, "master query timeout")
	if err := fs.Parse(args[1:]); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if *master == "" || *timeout <= 0 || fs.NArg() != 1 || fs.Arg(0) == "" {
		fmt.Fprintln(stderr, usage)
		return ops.VolumeStatusExitInvalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	id, err := resolveVolumeID(ctx, *master, fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops volume resolve: %v\n", err)
		// Match blockvolume: an unavailable or unknown master answer is retryable;
		// a definite refusal or invalid local configuration needs correction.
		if s, ok := status.FromError(err); ok {
			switch s.Code() {
			case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown, codes.Internal:
				return 75
			}
		}
		return 78
	}
	fmt.Fprintln(stdout, id.String())
	return ops.VolumeStatusExitOK
}
