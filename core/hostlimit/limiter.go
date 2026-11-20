// Package hostlimit coordinates background cuts across cooperating host processes.
// Guest I/O never acquires a slot. A missing pool degrades to static rate sharing.
package hostlimit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrBusy = errors.New("hostlimit: no background slot")
var ErrConfig = errors.New("hostlimit: incompatible host configuration")
var ErrRevoked = errors.New("hostlimit: cut budget revoked")

type Class uint8

const (
	Normal Class = iota
	Urgent
	BackgroundFill
	Idle
)

type classKey struct{}

func WithClass(ctx context.Context, c Class) context.Context {
	return context.WithValue(ctx, classKey{}, c)
}

type Config struct {
	Directory                string
	NormalSlots, UrgentSlots int
	FillSlots, IdleSlots     int
	PayloadBytes, COWBytes   int64
	BytesPerSecond           int64
	FallbackVolumes          int
	MaxHold                  time.Duration
}

func DefaultConfig(dir string) Config {
	return Config{Directory: dir, NormalSlots: 3, UrgentSlots: 1, FillSlots: 2, IdleSlots: 1, PayloadBytes: 64 << 20, COWBytes: 1 << 30, BytesPerSecond: 64 << 20, FallbackVolumes: 200, MaxHold: 5 * time.Minute}
}
func (c Config) Validate() error {
	if !filepath.IsAbs(c.Directory) || c.NormalSlots < 1 || c.UrgentSlots < 1 || c.FillSlots < 0 || c.IdleSlots < 0 || c.NormalSlots+c.UrgentSlots+c.FillSlots+c.IdleSlots > 256 || c.PayloadBytes <= 0 || c.COWBytes <= 0 || c.BytesPerSecond <= 0 || c.FallbackVolumes < 1 || c.FallbackVolumes > 65536 || c.MaxHold <= 0 {
		return errors.New("hostlimit: invalid configuration")
	}
	return nil
}

type Stats struct {
	Active                                         int
	PayloadReserved, COWReserved                   int64
	Admitted, Skipped, Fallback, Revoked, Released uint64
	FallbackActive                                 bool
	AdmissionNs, MaxHoldNs                         uint64
	SkippedByClass                                 [4]uint64
	LastPoolError                                  string
}
type lease struct {
	file         *os.File
	started      time.Time
	class        Class
	payload, cow int64
	abort        func(error)
	timer        *time.Timer
	releasing    bool
	revoked      bool
}
type Limiter struct {
	cfg        Config
	mu         sync.Mutex
	admitWG    sync.WaitGroup
	next       uint64
	live       map[uint64]*lease
	stats      Stats
	closed     bool
	wake       chan struct{}
	done       chan struct{}
	nextUpload time.Time
	fallback   bool
}

func New(c Config) (*Limiter, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	l := &Limiter{cfg: c, live: map[uint64]*lease{}, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go l.releaseLoop()
	return l, nil
}

// Phase is stable across process restarts; it never delays guest readiness.
func Phase(volume string, period time.Duration) time.Duration {
	if period <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(volume))
	return time.Duration(h.Sum64() % uint64(period))
}

// pool checks the immutable configuration under a separate OS lock. Slot files
// are never unlinked: replacing them could create a second set of live permits.
func (l *Limiter) pool(c Class) (*os.File, error) {
	info, err := os.Lstat(l.cfg.Directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("hostlimit: pool must be a local directory")
	}
	open := func(name string) (*os.File, error) {
		path := filepath.Join(l.cfg.Directory, name)
		if i, e := os.Lstat(path); e == nil && !i.Mode().IsRegular() {
			return nil, errors.New("hostlimit: nonregular pool file")
		}
		return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	}
	guard, err := open("configuration.lock")
	if err != nil {
		return nil, err
	}
	defer guard.Close()
	if err = tryLock(guard); err != nil {
		return nil, ErrBusy
	}
	expected, _ := json.Marshal(l.cfg)
	path := filepath.Join(l.cfg.Directory, "configuration.json")
	existing, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		err = os.WriteFile(path, expected, 0600)
		existing = expected
	}
	if err != nil {
		return nil, err
	}
	if string(existing) != string(expected) {
		return nil, ErrConfig
	}
	names := []string{}
	if c == BackgroundFill || c == Idle {
		n, prefix := l.cfg.FillSlots, "fill"
		if c == Idle {
			n, prefix = l.cfg.IdleSlots, "idle"
		}
		for i := 0; i < n; i++ {
			names = append(names, fmt.Sprintf("%s-%d", prefix, i))
		}
	} else {
		if c == Urgent {
			for i := 0; i < l.cfg.UrgentSlots; i++ {
				names = append(names, fmt.Sprintf("urgent-%d", i))
			}
		}
		for i := 0; i < l.cfg.NormalSlots; i++ {
			names = append(names, fmt.Sprintf("normal-%d", i))
		}
	}
	for _, name := range names {
		f, e := open(name)
		if e != nil {
			return nil, e
		}
		if e = tryLock(f); e == nil {
			return f, nil
		}
		_ = f.Close()
		if !errors.Is(e, ErrBusy) {
			return nil, e
		}
	}
	return nil, ErrBusy
}

// Acquire exposes background fill/idle permits to future background callers.
// Demand hydration must bypass this API; these permits carry no cut payload.
func (l *Limiter) Acquire(ctx context.Context, c Class, abort func(error)) (uint64, error) {
	return l.Admit(WithClass(ctx, c), 0, abort)
}

func (l *Limiter) Admit(ctx context.Context, payload int64, abort func(error)) (uint64, error) {
	start := time.Now()
	defer func() { l.mu.Lock(); l.stats.AdmissionNs += uint64(time.Since(start)); l.mu.Unlock() }()
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return 0, ErrRevoked
	}
	l.admitWG.Add(1)
	l.mu.Unlock()
	defer l.admitWG.Done()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if payload < 0 || payload > l.cfg.PayloadBytes {
		return 0, ErrBusy
	}
	c, _ := ctx.Value(classKey{}).(Class)
	if c > Idle || (c >= BackgroundFill && payload != 0) {
		return 0, ErrBusy
	}
	f, err := l.pool(c)
	if errors.Is(err, ErrBusy) {
		l.mu.Lock()
		l.stats.Skipped++
		l.stats.SkippedByClass[c]++
		l.mu.Unlock()
		return 0, err
	}
	if errors.Is(err, ErrConfig) {
		return 0, err
	}
	fallback := err != nil // A is deliberately degraded: no shared memory/spill cap.
	l.mu.Lock()
	if l.closed || ctx.Err() != nil || len(l.live) >= l.cfg.NormalSlots+l.cfg.UrgentSlots+l.cfg.FillSlots+l.cfg.IdleSlots {
		l.stats.Skipped++
		l.mu.Unlock()
		if f != nil {
			_ = f.Close()
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, ErrBusy
	}
	l.next++
	id := l.next
	x := &lease{file: f, started: time.Now(), class: c, payload: payload, abort: abort}
	l.live[id] = x
	l.stats.Admitted++
	l.stats.PayloadReserved += payload
	l.stats.Active++
	l.fallback = fallback
	l.stats.FallbackActive = fallback
	if fallback {
		l.stats.LastPoolError = err.Error()
		if l.stats.Fallback == 0 {
			log.Printf("hostlimit: shared cut pool unavailable (%v); using static fallback, shared memory/spill cap NOT enforced", err)
		}
		l.stats.Fallback++
		l.stats.FallbackActive = true
	}
	x.timer = time.AfterFunc(l.cfg.MaxHold, func() { l.revoke(id) })
	l.mu.Unlock()
	return id, nil
}
func (l *Limiter) revoke(id uint64) {
	l.mu.Lock()
	x := l.live[id]
	var abort func(error)
	if x != nil && !x.releasing && !x.revoked {
		x.revoked = true
		abort = x.abort
		l.stats.Revoked++
	}
	l.mu.Unlock()
	if abort != nil {
		abort(ErrRevoked)
	}
}

// ReserveCOW is guest-path accounting only: no OS calls, RPC or waiting for a slot.
func (l *Limiter) ReserveCOW(id uint64, n int64) error {
	l.mu.Lock()
	x := l.live[id]
	if x == nil || x.releasing || x.revoked || n < 0 || (x.class >= BackgroundFill && n != 0) {
		l.mu.Unlock()
		return ErrRevoked
	}
	if n > l.cfg.COWBytes-x.cow {
		x.revoked = true
		abort := x.abort
		l.stats.Revoked++
		l.mu.Unlock()
		if abort != nil {
			abort(ErrRevoked)
		}
		return ErrRevoked
	}
	x.cow += n
	l.stats.COWReserved += n
	l.mu.Unlock()
	return nil
}

// Release only marks settled work; one owned worker closes descriptors. Until
// then the kernel slot remains unavailable, conservatively retaining capacity.
func (l *Limiter) Release(id uint64) {
	l.mu.Lock()
	if x := l.live[id]; x != nil && !x.releasing {
		x.releasing = true
		x.timer.Stop()
	}
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
}
func (l *Limiter) releaseLoop() {
	defer close(l.done)
	for range l.wake {
		for {
			l.mu.Lock()
			var id uint64
			var x *lease
			for k, v := range l.live {
				if v.releasing {
					id, x = k, v
					break
				}
			}
			if x == nil {
				stop := l.closed && len(l.live) == 0
				l.mu.Unlock()
				if stop {
					return
				}
				break
			}
			l.mu.Unlock()
			if x.file != nil {
				_ = x.file.Close()
			}
			l.mu.Lock()
			delete(l.live, id)
			l.stats.Active--
			l.stats.PayloadReserved -= x.payload
			l.stats.COWReserved -= x.cow
			l.stats.Released++
			l.stats.MaxHoldNs = max(l.stats.MaxHoldNs, uint64(time.Since(x.started)))
			l.mu.Unlock()
		}
	}
}
func (l *Limiter) Snapshot() any { l.mu.Lock(); defer l.mu.Unlock(); return l.stats }

// WaitUpload paces background payload PUTs, including parallel uploader workers.
// Static shares trade utilization for a conservative aggregate rate. It never
// paces local WAL flushing, COW reads, or demand hydration.
func (l *Limiter) WaitUpload(ctx context.Context, n int) error {
	if n < 0 {
		return ErrBusy
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			return ErrRevoked
		}
		div := l.cfg.NormalSlots + l.cfg.UrgentSlots
		if l.fallback {
			div = l.cfg.FallbackVolumes
		}
		rate := l.cfg.BytesPerSecond / int64(div)
		if rate < 1 {
			rate = 1
		}
		now := time.Now()
		wait := time.Until(l.nextUpload)
		if wait <= 0 {
			l.nextUpload = now.Add(time.Duration(float64(n) / float64(rate) * float64(time.Second)))
			l.mu.Unlock()
			return ctx.Err()
		}
		l.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Close cancels cuts but cannot release their physical-I/O ownership. Callers
// must still settle their cuts and call Release; Close joins that settlement.
func (l *Limiter) Close() {
	l.mu.Lock()
	l.closed = true
	callbacks := []func(error){}
	for _, x := range l.live {
		if !x.releasing && x.abort != nil {
			callbacks = append(callbacks, x.abort)
		}
	}
	l.mu.Unlock()
	l.admitWG.Wait()
	for _, a := range callbacks {
		a(ErrRevoked)
	}
	select {
	case l.wake <- struct{}{}:
	default:
	}
	<-l.done
}
