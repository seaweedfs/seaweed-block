package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
)

// cpuProfileEnv names the directory for on-demand CPU profiles (diagnostics).
// Empty (the default) leaves blockvolume unchanged: no signal handler is
// installed and nothing is written. Set, SIGUSR1 starts a CPU profile into
// <dir>/blockvolume-<pid>-<n>.pprof and the next SIGUSR1 stops it, so a
// harness can profile a running volume without a port or a restart.
const cpuProfileEnv = "SWBLOCK_CPU_PROFILE_DIR"

// cpuProfiler toggles one CPU profile at a time.
type cpuProfiler struct {
	dir string
	mu  sync.Mutex
	f   *os.File
	n   int
}

// toggle starts a profile when none runs, else stops the running one. It
// returns the profile's path and whether a profile now runs.
func (p *cpuProfiler) toggle() (string, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.f != nil {
		path := p.f.Name()
		pprof.StopCPUProfile()
		err := p.f.Close()
		p.f = nil
		return path, false, err
	}
	p.n++
	path := filepath.Join(p.dir, fmt.Sprintf("blockvolume-%d-%d.pprof", os.Getpid(), p.n))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return path, false, err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return path, false, err
	}
	p.f = f
	return path, true, nil
}

// stop ends a running profile (at shutdown), keeping what it recorded.
func (p *cpuProfiler) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.f != nil {
		pprof.StopCPUProfile()
		_ = p.f.Close()
		p.f = nil
	}
}

// checkCPUProfileDir refuses a profile directory that is not an existing
// directory, at startup rather than at the first signal.
func checkCPUProfileDir(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("--cpu-profile-dir %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("--cpu-profile-dir %s: not a directory", dir)
	}
	return nil
}
