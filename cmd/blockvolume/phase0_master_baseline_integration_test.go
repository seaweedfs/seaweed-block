//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Phase 0 baseline: what blockmaster's loss costs the guests today, measured
// with real binaries and an in-test NVMe/TCP initiator, so that phase 0
// (mTLS on the master/holder link) can be gated against it. One write of 4
// KiB every 10 ms per volume, each with a 30 s command deadline.
//
//   (a) blockmaster SIGKILLed and restarted 5 s later under guest writes:
//       guest write errors (expected 0) and the write latency;
//   (b) blockmaster down 60 s: the time until each volume fences, then,
//       with the master back, until it serves again. The harness restarts
//       an exited blockvolume after the shipped RestartSec (5 s), as the unit
//       does, and counts the starts against the shipped start limit (10 in
//       600 s);
//   (c) as (b) with 3 volumes.
//
// The numbers are logged as PHASE0 lines; only (a)'s zero write errors and
// (b)/(c)'s serving again are asserted.

const (
	baselineEvery      = 10 * time.Millisecond
	baselineDeadline   = 30 * time.Second
	unitRestartSec     = 5 * time.Second
	unitStartLimit     = 10
	unitStartLimitSpan = 600 * time.Second
)

type baselineWriter struct {
	mu       sync.Mutex
	samples  []f1Sample
	errAt    time.Time
	err      error
	stopped  chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// startBaselineWriter writes to vol's frontend until stopped or until the
// first failed write (the fence, when the master is gone).
func startBaselineWriter(t *testing.T, env *handoffEnv, key string) *baselineWriter {
	t.Helper()
	b := env.hosts[key]
	c := dialNVMe(t, b.nvme, plannedHandoffNQN)
	w := &baselineWriter{stopped: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		defer c.close()
		for n := uint64(0); ; n++ {
			select {
			case <-w.stopped:
				return
			case <-time.After(baselineEvery):
			}
			lba := n % 1024
			s := time.Now()
			_ = c.io.SetDeadline(s.Add(baselineDeadline))
			err := c.tryWrite(lba, f1Block(b.volume, 0, "phase0", lba))
			w.mu.Lock()
			w.samples = append(w.samples, f1Sample{start: s, d: time.Since(s)})
			if err != nil {
				w.err, w.errAt = err, time.Now()
			}
			w.mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	return w
}

func (w *baselineWriter) stop() {
	w.stopOnce.Do(func() { close(w.stopped) })
	<-w.done
}

// maxLatency is the longest write that started in [from, to].
func (w *baselineWriter) maxLatency(from, to time.Time) (time.Duration, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var max time.Duration
	n := 0
	for _, s := range w.samples {
		if !s.start.Before(from) && !s.start.After(to) {
			n++
			if s.d > max {
				max = s.d
			}
		}
	}
	return max, n
}

func TestPhase0BaselineMasterKilledAndRestartedUnderWrites(t *testing.T) {
	env, _ := f1StartEnv(t, f1Config{Volumes: 1})
	vols := f1StartSources(t, env, 1)
	w := startBaselineWriter(t, env, vols[0].id)
	time.Sleep(10 * time.Second)
	killAt := time.Now()
	env.masterProcess.kill(t)
	time.Sleep(5 * time.Second)
	env.masterProcess = env.h.start(t, "blockmaster-restarted", env.h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	backAt := time.Now()
	time.Sleep(30 * time.Second)
	w.stop()
	all, n := w.maxLatency(time.Time{}, time.Now())
	around, m := w.maxLatency(killAt, backAt.Add(5*time.Second))
	t.Logf("PHASE0 (a) master SIGKILL + restart after 5 s: writes %d, write errors %d (%v), max write latency %v, max in the kill window (%d writes) %v, master back after %v",
		n, map[bool]int{true: 1, false: 0}[w.err != nil], w.err, all.Round(time.Microsecond), m, around.Round(time.Microsecond), backAt.Sub(killAt).Round(time.Millisecond))
	if w.err != nil {
		t.Fatalf("phase 0 baseline (a): a guest write failed while the master was down 5 s: %v", w.err)
	}
}

// unitEmulator restarts an exited blockvolume after unitRestartSec, as the
// shipped unit does, and records every exit.
type unitEmulator struct {
	mu     sync.Mutex
	d      *daemon
	starts []time.Time
	exits  []string
	stop   chan struct{}
	done   chan struct{}
}

func emulateUnit(t *testing.T, env *handoffEnv, key string, d *daemon) *unitEmulator {
	u := &unitEmulator{d: d, starts: []time.Time{time.Now()}, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(u.done)
		for {
			u.mu.Lock()
			cur := u.d
			u.mu.Unlock()
			select {
			case <-u.stop:
				return
			case <-cur.done:
			}
			u.mu.Lock()
			u.exits = append(u.exits, fmt.Sprintf("%s exit %d", time.Now().Format("15:04:05.000"), exitCode(cur.err)))
			u.mu.Unlock()
			select {
			case <-u.stop:
				return
			case <-time.After(unitRestartSec):
			}
			next := env.h.start(t, fmt.Sprintf("blockvolume-%s-unit-%d", key, len(u.starts)), env.h.volume, env.volArgsFor(env.hosts[key])...)
			u.mu.Lock()
			u.d, u.starts = next, append(u.starts, time.Now())
			u.mu.Unlock()
		}
	}()
	return u
}

// startLimitHit reports whether the unit's starts would have exceeded the
// shipped limit (more than unitStartLimit starts within unitStartLimitSpan).
func (u *unitEmulator) startLimitHit() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	for i := range u.starts {
		n := 0
		for _, s := range u.starts[i:] {
			if s.Sub(u.starts[i]) <= unitStartLimitSpan {
				n++
			}
		}
		if n > unitStartLimit {
			return true
		}
	}
	return false
}

func phase0MasterDown60s(t *testing.T, n int) {
	env, _ := f1StartEnv(t, f1Config{Volumes: n})
	vols := f1StartSources(t, env, n)
	writers := make([]*baselineWriter, n)
	units := make([]*unitEmulator, n)
	for i, v := range vols {
		writers[i] = startBaselineWriter(t, env, v.id)
		units[i] = emulateUnit(t, env, v.id, v.d)
	}
	time.Sleep(10 * time.Second)
	killAt := time.Now()
	env.masterProcess.kill(t)
	time.Sleep(60 * time.Second)
	env.masterProcess = env.h.start(t, "blockmaster-back", env.h.master, env.masterArgs...)
	env.masterProcess.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded=")
	backAt := time.Now()
	for i, v := range vols {
		b := env.hosts[v.id]
		poll(t, 5*time.Minute, v.id+" never served again after the master came back", func() bool { return volumeFrontendReady(b.status, b.volume) })
		servedAt := time.Now()
		probe := startBaselineWriter(t, env, v.id)
		time.Sleep(time.Second)
		probe.stop()
		if probe.err != nil {
			t.Fatalf("%s serves again but a write failed: %v", v.id, probe.err)
		}
		writers[i].stop()
		close(units[i].stop)
		<-units[i].done
		w, u := writers[i], units[i]
		u.mu.Lock()
		starts, exits := len(u.starts), append([]string(nil), u.exits...)
		u.mu.Unlock()
		fence := "none"
		if w.err != nil {
			fence = w.errAt.Sub(killAt).Round(time.Millisecond).String()
		}
		firstExit := "none"
		if len(exits) > 0 {
			firstExit = exits[0]
		}
		t.Logf("PHASE0 (%s) master down 60 s, volume %s: first failed write after %s (%v); exits %v (first %s); starts %d, start limit (%d in %v) hit %v; serving again %v after the master came back",
			map[int]string{1: "b", 3: "c"}[n], v.id, fence, w.err, exits, firstExit, starts, unitStartLimit, unitStartLimitSpan, u.startLimitHit(), servedAt.Sub(backAt).Round(time.Millisecond))
	}
}

func TestPhase0BaselineMasterDown60sOneVolume(t *testing.T)    { phase0MasterDown60s(t, 1) }
func TestPhase0BaselineMasterDown60sThreeVolumes(t *testing.T) { phase0MasterDown60s(t, 3) }
