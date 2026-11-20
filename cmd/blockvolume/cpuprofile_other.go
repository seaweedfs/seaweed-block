//go:build !unix

package main

import (
	"context"
	"errors"
)

func startCPUProfileSignal(_ context.Context, dir string) (stop func(), err error) {
	if dir == "" {
		return func() {}, nil
	}
	return nil, errors.New("--cpu-profile-dir needs SIGUSR1 (unix only)")
}
