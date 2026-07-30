//go:build linux && amd64

package main

import "github.com/seaweedfs/seaweed-block/internal/iouring"

func runProbe(requestedDepth uint32) (probeReport, error) {
	return iouring.RunProbe(requestedDepth)
}
