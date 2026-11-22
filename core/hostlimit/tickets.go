package hostlimit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNotHead refuses a Normal admission whose ticket is not the earliest
// live, unskipped ticket on the host. It is an ErrSlotsHeld: a wait.
var ErrNotHead = fmt.Errorf("%w: an earlier ticket is waiting", ErrSlotsHeld)

const ticketsDir = "tickets"

// TicketSkipAfter is how long a waiter must observe the same earlier ticket
// ahead of it, with a normal slot free and no urgent work pending, before
// it treats that ticket's holder as stalled and passes it. The observation
// can be wrong about a slow holder; ticket order is fairness only, the slot
// lock is the exclusion.
const TicketSkipAfter = 2 * time.Second

// HostNow is the host's monotonic clock in nanoseconds (shared by every
// process on the host).
func HostNow() int64 { return hostMonoNs() }

// HostTime converts an in-process time to the host monotonic clock.
func HostTime(t time.Time) int64 { return hostMonoNs() - time.Since(t).Nanoseconds() }

// A Ticket is a timer cut's place in the host queue: a file in the pool's
// tickets directory, held with an exclusive lock while the cut waits, named
// by its due time on the host monotonic clock. Admission consumes it; Close
// gives it up. The kernel drops the lock of an exiting process, and other
// waiters then remove the file.
type Ticket struct {
	l    *Limiter
	due  int64
	name string
	path string

	mu       sync.Mutex
	f        *os.File
	done     bool
	observed map[string]int64 // earlier ticket -> host time it was first seen stalled-looking
	skipped  uint64
}

type ticketKey struct{}

// WithTicket carries t to the Normal admission made under ctx.
func WithTicket(ctx context.Context, t *Ticket) context.Context {
	return context.WithValue(ctx, ticketKey{}, t)
}

func ticketFrom(ctx context.Context) *Ticket {
	t, _ := ctx.Value(ticketKey{}).(*Ticket)
	return t
}

var ticketSeq struct {
	sync.Mutex
	n uint64
}

// TakeTicket queues a timer cut due at dueHostNs (host monotonic clock).
// The file is created, locked, and kept only if its path still names the
// locked file (a prober may have removed it before the lock); otherwise it
// is created again.
func (l *Limiter) TakeTicket(dueHostNs int64) (*Ticket, error) {
	dir := filepath.Join(l.cfg.Directory, ticketsDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 8; attempt++ {
		ticketSeq.Lock()
		ticketSeq.n++
		seq := ticketSeq.n
		ticketSeq.Unlock()
		name := fmt.Sprintf("%020d-%d-%d", max(dueHostNs, 0), os.Getpid(), seq)
		path := filepath.Join(dir, name)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return nil, err
		}
		if hook := l.testAfterTicketCreate; hook != nil {
			hook(path)
		}
		if err = lockRetry(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		if samePath(path, f) {
			return &Ticket{l: l, due: dueHostNs, name: name, path: path, f: f, observed: map[string]int64{}}, nil
		}
		_ = f.Close() // removed before our lock: take a new one
	}
	return nil, errors.New("hostlimit: ticket kept disappearing before it was locked")
}

// lockRetry takes an exclusive lock that a prober may hold for an instant.
func lockRetry(f *os.File) error {
	for i := 0; ; i++ {
		err := tryLock(f)
		if !errors.Is(err, ErrBusy) || i >= 200 {
			return err
		}
		time.Sleep(time.Millisecond)
	}
}

// samePath reports whether path still names the file open as f.
func samePath(path string, f *os.File) bool {
	pi, err := os.Stat(path)
	if err != nil {
		return false
	}
	fi, err := f.Stat()
	return err == nil && os.SameFile(pi, fi)
}

// Due is the ticket's due time on the host monotonic clock.
func (t *Ticket) Due() int64 { return t.due }

// Skipped counts the earlier tickets this ticket passed by the skip rule.
func (t *Ticket) Skipped() uint64 { t.mu.Lock(); defer t.mu.Unlock(); return t.skipped }

// Close gives the place up: the file is removed if its path still names
// it, then unlocked. Close is idempotent.
func (t *Ticket) Close() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return
	}
	t.done = true
	if samePath(t.path, t.f) {
		_ = os.Remove(t.path)
	}
	_ = t.f.Close()
}

// headCheck runs under the configuration lock, before the urgent-pending
// probe. Earlier tickets whose lock can be taken belong to exited
// processes and are removed (lock, re-check the path, unlink, unlock).
// A live earlier ticket blocks t unless t has observed it, for
// TicketSkipAfter, staying ahead while a normal slot was free and no
// urgent work was pending.
func (t *Ticket) headCheck(slotFree, urgentFree bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return errors.New("hostlimit: the ticket was closed")
	}
	dir := filepath.Dir(t.path)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	type tk struct {
		name string
		due  int64
	}
	var earlier []tk
	for _, e := range ents {
		n := e.Name()
		if n == t.name {
			continue
		}
		due, ok := ticketDue(n)
		if !ok || due > t.due || (due == t.due && n > t.name) {
			continue
		}
		earlier = append(earlier, tk{n, due})
	}
	sort.Slice(earlier, func(i, j int) bool { return earlier[i].due < earlier[j].due })
	now := hostMonoNs()
	seen := map[string]bool{}
	blocked := false
	var passed []string
	for _, e := range earlier {
		path := filepath.Join(dir, e.name)
		if reap(path) {
			continue
		}
		seen[e.name] = true
		if !slotFree || !urgentFree {
			delete(t.observed, e.name)
			blocked = true
			continue
		}
		first, ok := t.observed[e.name]
		if !ok {
			t.observed[e.name] = now
			blocked = true
			continue
		}
		if time.Duration(now-first) < t.l.skipAfter() {
			blocked = true
			continue
		}
		passed = append(passed, e.name) // observed long enough: the skip rule
	}
	for n := range t.observed {
		if !seen[n] {
			delete(t.observed, n)
		}
	}
	if blocked {
		return ErrNotHead
	}
	t.skipped += uint64(len(passed))
	return nil
}

// reap removes the ticket at path if its holder has exited: it takes the
// lock, re-checks that the path still names the locked file, unlinks it,
// then unlocks. It reports whether the ticket was dead (or already gone).
func reap(path string) bool {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return errors.Is(err, os.ErrNotExist)
	}
	defer f.Close()
	if tryLock(f) != nil {
		return false
	}
	if samePath(path, f) {
		_ = os.Remove(path)
	}
	return true
}

func ticketDue(name string) (int64, bool) {
	i := strings.IndexByte(name, '-')
	if i <= 0 {
		return 0, false
	}
	d, err := strconv.ParseInt(name[:i], 10, 64)
	return d, err == nil
}
