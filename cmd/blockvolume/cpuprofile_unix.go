//go:build unix

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

// cpuProfileSupported: SIGUSR1 exists here.
const cpuProfileSupported = true

// startCPUProfileSignal makes SIGUSR1 toggle a CPU profile into dir. The
// returned stop removes the handler, stops and joins the worker (so no
// buffered signal can start a profile after it), then writes out a running
// profile. dir "" installs nothing.
func startCPUProfileSignal(dir string) (stop func(), err error) {
	if dir == "" {
		return func() {}, nil
	}
	if err := checkCPUProfileDir(dir); err != nil {
		return nil, err
	}
	p := &cpuProfiler{dir: dir}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGUSR1)
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-quit:
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
		close(quit)
		<-done
		p.stop()
	}, nil
}
