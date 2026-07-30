//go:build !linux || !amd64

package main

import (
	"errors"
	"runtime"
)

func runProbe(uint32) (probeReport, error) {
	reason := "unsupported_platform"
	if runtime.GOOS == "linux" {
		reason = "unsupported_linux_arch"
	}
	return probeReport{
		Platform:      runtime.GOOS + "/" + runtime.GOARCH,
		KernelRelease: "-",
		RefusalReason: reason,
	}, errors.New(reason)
}
