//go:build unix

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// startCPUProfileSignal makes SIGUSR1 toggle a CPU profile into dir until ctx
// ends; the returned stop ends a running profile and removes the handler.
// dir "" installs nothing.
func startCPUProfileSignal(ctx context.Context, dir string) (stop func(), err error) {
	if dir == "" {
		return func() {}, nil
	}
	if err := checkCPUProfileDir(dir); err != nil {
		return nil, err
	}
	p := &cpuProfiler{dir: dir}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGUSR1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-sig:
				path, running, err := p.toggle()
				switch {
				case err != nil:
					log.Printf("blockvolume: cpu profile %s: %v", path, err)
				case running:
					log.Printf("blockvolume: cpu profile started: %s (SIGUSR1 again stops it)", path)
				default:
					log.Printf("blockvolume: cpu profile written: %s", path)
				}
			}
		}
	}()
	log.Printf("blockvolume: SIGUSR1 toggles a CPU profile into %s", dir)
	return func() {
		signal.Stop(sig)
		p.stop()
	}, nil
}
