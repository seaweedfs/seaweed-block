// Package hostlimit coordinates background cuts across cooperating host processes.
// Guest I/O never acquires a slot. An unavailable pool skips background work;
// urgent seals may use explicitly uncoordinated per-process limits.
package hostlimit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrBusy = errors.New("hostlimit: no background slot")

// ErrSlotsHeld is the ErrBusy of a host whose slots are all held by other
// work: retrying once one is released can succeed. Other ErrBusy results
// (an oversized payload, an unusable pool) do not change by waiting.
var ErrSlotsHeld = fmt.Errorf("%w: every slot is held", ErrBusy)

// ErrUrgentPending refuses a Normal admission while an urgent cut runs or
// a caller holds HoldUrgentPending on the host: new background cuts wait
// for it. It is an ErrSlotsHeld, so waiting callers treat it alike.
var ErrUrgentPending = fmt.Errorf("%w: an urgent cut is pending", ErrSlotsHeld)

// urgentPendingFile is the pool file urgent work holds shared; Normal
// admission probes it exclusively, without waiting. It has no content.
const urgentPendingFile = "urgent-pending"

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
	if !filepath.IsAbs(c.Directory) || c.NormalSlots < 1 || c.NormalSlots > 256 || c.UrgentSlots < 1 || c.UrgentSlots > 256 || c.FillSlots < 0 || c.FillSlots > 256 || c.IdleSlots < 0 || c.IdleSlots > 256 || c.NormalSlots+c.UrgentSlots+c.FillSlots+c.IdleSlots > 256 || c.PayloadBytes <= 0 || c.COWBytes <= 0 || c.BytesPerSecond <= 0 || c.FallbackVolumes < 1 || c.FallbackVolumes > 65536 || c.MaxHold <= 0 {
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
	LastPoolError                                  string // the last pool error; cleared by the next admission through the pool
	// UrgentPendingUnheld counts urgent admissions that went on without the
	// urgent-pending hold: it stayed held exclusively for 200 ms (a waiter
	// stalled in its probe).
	UrgentPendingUnheld uint64
	// SlotsHeldSinceNs maps each slot file seen held at the last snapshot to
	// the wall time it was first seen held, by any process. A slot held far
	// longer than MaxHold points to a stopped or hung holder, which only its
	// supervisor can remove.
	SlotsHeldSinceNs map[string]int64 `json:",omitempty"`
	// UrgentPendingHeldSinceNs is the wall time urgent-pending was first seen
	// held at consecutive snapshots (0 when free). A stop or other urgent
	// work that hangs keeps Normal admissions refused for as long as it
	// lives; this shows since when.
	UrgentPendingHeldSinceNs int64 `json:",omitempty"`
}
type lease struct {
	file         *os.File
	pending      *os.File // urgent only: the shared hold on urgentPendingFile
	started      time.Time
	class        Class
	fallback     bool
	payload, cow int64
	abort        func(error)
	timer        *time.Timer
	releasing    bool
	revoked      bool
}
type Limiter struct {
	testBeforeConfigWrite func()         // test-only crash boundary; immutable before first admission
	testBeforeConfigLock  func(*os.File) // test-only fault boundary; immutable before first admission
	testAfterTicketCreate func(string)   // test-only: between a ticket's create and its lock
	testSkipAfter         time.Duration  // test-only TicketSkipAfter override
	cfg                   Config
	mu                    sync.Mutex
	admitWG               sync.WaitGroup
	next                  uint64
	live                  map[uint64]*lease
	stats                 Stats
	closed                bool
	wake                  chan struct{}
	done                  chan struct{}
	nextUpload            time.Time
	fallback              bool
	heldSlots             int   // normal and urgent slots held on the host at the last count
	bytesSinceCount       int64 // payload bytes paced since that count
	heldSince             map[string]time.Time
	urgentHeldSince       time.Time

	// skipMu guards stalled: the earlier tickets this process judged stalled
	// by the skip rule. It is never taken with mu held, nor mu under it.
	skipMu  sync.Mutex
	stalled map[string]bool
}

func (l *Limiter) skipAfter() time.Duration {
	if l.testSkipAfter > 0 {
		return l.testSkipAfter
	}
	return TicketSkipAfter
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
func (l *Limiter) pool(c Class, t *Ticket) (*os.File, error) {
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
	// A ticketed Normal admission first checks its place in the queue. The
	// ticket files synchronise through their own locks, so this runs outside
	// the configuration lock and does not lengthen it.
	if c == Normal && t != nil {
		if err := t.headCheck(l.normalSlotFree(), l.urgentFree()); err != nil {
			return nil, err
		}
	}
	guard, err := open("configuration.lock")
	if err != nil {
		return nil, err
	}
	defer guard.Close()
	if l.testBeforeConfigLock != nil {
		l.testBeforeConfigLock(guard)
	}
	if err = lockGuard(guard); err != nil {
		return nil, err
	}
	expected, _ := json.Marshal(l.cfg)
	path := filepath.Join(l.cfg.Directory, "configuration.json")
	existing, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		err = l.publishConfig(path, expected)
		existing = expected
	}
	if err != nil {
		return nil, err
	}
	if string(existing) != string(expected) {
		return nil, ErrConfig
	}
	if c == Normal {
		// The probe is held until the slot try-lock below has been made, so
		// no Normal admission completes after urgent work became pending.
		probe, err := open(urgentPendingFile)
		if err != nil {
			return nil, err
		}
		defer probe.Close()
		err = tryLock(probe)
		if errors.Is(err, ErrBusy) {
			return nil, ErrUrgentPending
		}
		if err != nil {
			return nil, err
		}
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
	return nil, ErrSlotsHeld
}

// guardWait bounds how long an admission waits for configuration.lock.
// Another admission holds it only while it checks the configuration and
// tries its slots.
const guardWait = 50 * time.Millisecond

// lockGuard takes configuration.lock, retrying for up to guardWait. Still
// held after that, the answer is ErrSlotsHeld: a wait, like every held slot,
// never a refusal of urgent work.
func lockGuard(f *os.File) error {
	deadline := time.Now().Add(guardWait)
	for {
		err := tryLock(f)
		if !errors.Is(err, ErrBusy) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: configuration lock held for %s", ErrSlotsHeld, guardWait)
		}
		time.Sleep(time.Millisecond)
	}
}

// publishConfig runs under the configuration OS lock. A killed initializer
// leaves an ignored temporary file, never a partially visible configuration.
func (l *Limiter) publishConfig(path string, data []byte) error {
	f, err := os.CreateTemp(l.cfg.Directory, ".configuration-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if l.testBeforeConfigWrite != nil {
		l.testBeforeConfigWrite()
	}
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
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
	var t *Ticket
	if c == Normal {
		t = ticketFrom(ctx)
	}
	f, err := l.pool(c, t)
	var pending *os.File
	if err == nil && c == Urgent {
		// Bounded: a waiter stalled in its probe must not block urgent work.
		var held bool
		if pending, held, _ = holdSharedBounded(filepath.Join(l.cfg.Directory, urgentPendingFile)); !held {
			l.mu.Lock()
			l.stats.UrgentPendingUnheld++
			l.mu.Unlock()
		}
	}
	if errors.Is(err, ErrBusy) {
		l.mu.Lock()
		l.stats.Skipped++
		l.stats.SkippedByClass[c]++
		l.mu.Unlock()
		return 0, err
	}
	if errors.Is(err, ErrConfig) {
		l.mu.Lock()
		warn := l.stats.LastPoolError != err.Error()
		l.stats.LastPoolError = err.Error()
		l.stats.Skipped++
		l.stats.SkippedByClass[c]++
		l.mu.Unlock()
		if warn {
			log.Printf("hostlimit: shared pool configuration refused; background protection may lag: %v", err)
		}
		return 0, err
	}
	if err != nil && c != Urgent {
		l.mu.Lock()
		warn := l.stats.LastPoolError != err.Error()
		l.stats.LastPoolError = err.Error()
		l.stats.Skipped++
		l.stats.SkippedByClass[c]++
		l.mu.Unlock()
		if warn {
			log.Printf("hostlimit: shared pool unavailable; background work skipped: %v", err)
		}
		return 0, errors.Join(ErrBusy, err)
	}
	fallback := err != nil // Urgent fallback has no coordinated host resource/rate bound.
	l.mu.Lock()
	if !fallback {
		// The pool answered: a pool error reported earlier is over.
		l.stats.LastPoolError = ""
	}
	if l.closed || ctx.Err() != nil || len(l.live) >= l.cfg.NormalSlots+l.cfg.UrgentSlots+l.cfg.FillSlots+l.cfg.IdleSlots {
		closed := l.closed
		l.stats.Skipped++
		l.mu.Unlock()
		if f != nil {
			_ = f.Close()
		}
		switch {
		case closed:
			return 0, ErrRevoked
		case ctx.Err() != nil:
			return 0, ctx.Err()
		}
		return 0, ErrSlotsHeld
	}
	l.next++
	id := l.next
	x := &lease{file: f, pending: pending, started: time.Now(), class: c, fallback: fallback, payload: payload, abort: abort}
	t.Close() // admitted: the ticket's place is used up
	l.live[id] = x
	l.stats.Admitted++
	l.stats.PayloadReserved += payload
	l.stats.Active++
	l.fallback = fallback
	for _, held := range l.live {
		l.fallback = l.fallback || held.fallback
	}
	l.stats.FallbackActive = l.fallback
	warnFallback := fallback && l.stats.Fallback == 0
	if fallback {
		l.stats.LastPoolError = err.Error()
		l.stats.Fallback++
		l.stats.FallbackActive = true
	}
	x.timer = time.AfterFunc(l.cfg.MaxHold, func() { l.revoke(id) })
	l.mu.Unlock()
	if warnFallback {
		log.Printf("hostlimit: shared cut pool unavailable (%v); using static fallback, shared memory/spill cap NOT enforced", err)
	}
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
			if x.pending != nil {
				_ = x.pending.Close()
			}
			l.mu.Lock()
			delete(l.live, id)
			l.fallback = false
			for _, held := range l.live {
				l.fallback = l.fallback || held.fallback
			}
			l.stats.FallbackActive = l.fallback
			l.stats.Active--
			l.stats.PayloadReserved -= x.payload
			l.stats.COWReserved -= x.cow
			l.stats.Released++
			l.stats.MaxHoldNs = max(l.stats.MaxHoldNs, uint64(time.Since(x.started)))
			l.mu.Unlock()
		}
	}
}
func (l *Limiter) Snapshot() any {
	held := l.slotFilesHeld()
	urgentHeld := held != nil && !l.urgentFree()
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.stats
	switch {
	case !urgentHeld:
		l.urgentHeldSince = time.Time{}
	case l.urgentHeldSince.IsZero():
		l.urgentHeldSince = time.Now()
	}
	if !l.urgentHeldSince.IsZero() {
		st.UrgentPendingHeldSinceNs = l.urgentHeldSince.UnixNano()
	}
	if held != nil {
		now := time.Now()
		if l.heldSince == nil {
			l.heldSince = map[string]time.Time{}
		}
		st.SlotsHeldSinceNs = map[string]int64{}
		for name := range l.heldSince {
			if !held[name] {
				delete(l.heldSince, name)
			}
		}
		for name := range held {
			if _, ok := l.heldSince[name]; !ok {
				l.heldSince[name] = now
			}
			st.SlotsHeldSinceNs[name] = l.heldSince[name].UnixNano()
		}
	}
	return st
}

// slotNames lists the slot files of the given classes.
func (l *Limiter) slotNames(urgent, normal, others bool) []string {
	var names []string
	add := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			names = append(names, fmt.Sprintf("%s-%d", prefix, i))
		}
	}
	if urgent {
		add("urgent", l.cfg.UrgentSlots)
	}
	if normal {
		add("normal", l.cfg.NormalSlots)
	}
	if others {
		add("fill", l.cfg.FillSlots)
		add("idle", l.cfg.IdleSlots)
	}
	return names
}

// probeHeld reports whether the slot file is held by any process. The probe
// is a shared non-blocking lock, released at once; it can make another
// process's exclusive admission attempt fail once (a wait, by rule).
func (l *Limiter) probeHeld(name string) (held, ok bool) {
	path := filepath.Join(l.cfg.Directory, name)
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return false, errors.Is(err, os.ErrNotExist)
	}
	defer f.Close()
	err = tryLockShared(f)
	if errors.Is(err, ErrBusy) {
		return true, true
	}
	return false, err == nil
}

// slotFilesHeld maps every held slot file to true (nil if the pool cannot
// be read).
func (l *Limiter) slotFilesHeld() map[string]bool {
	if _, err := os.Stat(l.cfg.Directory); err != nil {
		return nil
	}
	held := map[string]bool{}
	for _, name := range l.slotNames(true, true, true) {
		if h, _ := l.probeHeld(name); h {
			held[name] = true
		}
	}
	return held
}

// normalSlotFree reports whether some normal slot file is free now.
func (l *Limiter) normalSlotFree() bool {
	for _, name := range l.slotNames(false, true, false) {
		if h, ok := l.probeHeld(name); ok && !h {
			return true
		}
	}
	return false
}

// urgentFree reports whether no urgent work holds urgent-pending now.
func (l *Limiter) urgentFree() bool {
	f, err := os.OpenFile(filepath.Join(l.cfg.Directory, urgentPendingFile), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false
	}
	defer f.Close()
	return tryLock(f) == nil
}

// heldCodedSlots counts the normal and urgent slots held on the host, the
// classes that pace on the upload budget.
func (l *Limiter) heldCodedSlots() int {
	n := 0
	for _, name := range l.slotNames(true, true, false) {
		if h, _ := l.probeHeld(name); h {
			n++
		}
	}
	return n
}

// WaitUpload paces background payload PUTs, including parallel uploader workers.
// The rate is the budget divided by the normal and urgent slots held on the
// host (counted by probing their files), re-counted once per PayloadBytes of
// this process's uploads: a lone transfer gets the whole budget. After the
// held count changes, each process may pace up to one payload at its old
// rate. One process spaces reservations by the previous payload/rate; its
// first payload is immediate; debt survives its leases but resets on a new
// process. Uncoordinated fallback keeps a per-process divisor and has no
// host-rate bound. Wire overhead is excluded. Local WAL flushing, COW reads
// and demand hydration bypass it.
func (l *Limiter) WaitUpload(ctx context.Context, n int) error {
	if n < 0 {
		return ErrBusy
	}
	l.mu.Lock()
	recount := l.heldSlots == 0 || l.bytesSinceCount >= l.cfg.PayloadBytes
	l.bytesSinceCount += int64(n)
	l.mu.Unlock()
	if recount {
		h := l.heldCodedSlots()
		l.mu.Lock()
		l.heldSlots, l.bytesSinceCount = h, int64(n)
		l.mu.Unlock()
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
		div := max(1, l.heldSlots)
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
