//go:build !unix

package main

import "errors"

// cpuProfileSupported: no SIGUSR1 on this platform.
const cpuProfileSupported = false

func startCPUProfileSignal(dir string) (stop func(), err error) {
	if dir == "" {
		return func() {}, nil
	}
	return nil, errors.New("--cpu-profile-dir needs SIGUSR1 (unix only)")
}
