package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/status"
)

// AuditEvent is one line of the append-only audit log (JSON).
type AuditEvent struct {
	Time        time.Time `json:"time"`
	Event       string    `json:"event"` // "call" or "reload"
	Method      string    `json:"method,omitempty"`
	Role        Role      `json:"role,omitempty"`
	ID          string    `json:"id,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	Result      string    `json:"result"` // "ok", or the refusal's gRPC code
	Reason      string    `json:"reason,omitempty"`
}

// auditQueue bounds the lines waiting for the writer. A full queue drops a
// best-effort line and counts it (WriteErrors).
const auditQueue = 1024

// AuditAttemptBound is how long a strict (mutating) call waits for its
// attempt line to be written before it is refused as "audit unavailable".
var AuditAttemptBound = 2 * time.Second

// ErrAuditUnavailable: a strict call's attempt line could not be written in
// time; the call is refused and nothing is done.
var ErrAuditUnavailable = errors.New("audit unavailable")

type auditLine struct {
	b    []byte
	done chan error // nil for a best-effort line
}

// AuditLog appends events to a writer, one JSON object per line, from one
// writer goroutine. What waits on it depends on what the line is for:
//   - node calls (renewals, ACKs, heartbeats, streams): accepted ones are not
//     recorded and return before any lock; nothing on the lease path waits;
//   - reads and refused calls: best effort, queued (bounded), dropped and
//     counted when the queue is full (one log line per overflow burst);
//   - mutating operator and admin calls (Rule.Strict): an ATTEMPT line is
//     written before the handler runs, within AuditAttemptBound, or the call
//     is refused (ErrAuditUnavailable) and nothing is done; the outcome line
//     after the handler is best effort.
//
// A refused node call is recorded at most once per minute per (fingerprint,
// result, method); every person's call and every role-file reload is recorded.
type AuditLog struct {
	mu       sync.Mutex // the refusal dedup only; never held across I/O
	now      func() time.Time
	refused  map[string]time.Time
	q        chan auditLine
	pending  sync.WaitGroup
	errs     atomic.Int64 // lines dropped (queue full) or not written
	dropping atomic.Bool  // inside an overflow burst (logged once)
}

// NewAuditLog writes to w; now nil is time.Now.
func NewAuditLog(w io.Writer, now func() time.Time) *AuditLog {
	if now == nil {
		now = time.Now
	}
	l := &AuditLog{now: now, refused: map[string]time.Time{}, q: make(chan auditLine, auditQueue)}
	go l.run(w)
	return l
}

// OpenAuditLog appends to the file at path (created 0600 when missing).
func OpenAuditLog(path string) (*AuditLog, *os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("audit log: %w", err)
	}
	return NewAuditLog(f, nil), f, nil
}

func (l *AuditLog) run(w io.Writer) {
	for line := range l.q {
		_, err := w.Write(line.b)
		if err != nil {
			l.errs.Add(1)
		} else {
			l.dropping.Store(false)
		}
		if line.done != nil {
			line.done <- err
		}
		l.pending.Done()
	}
}

// enqueue queues e for the writer, or drops and counts it when the queue is
// full (logged once per overflow burst).
func (l *AuditLog) enqueue(e AuditEvent) {
	raw, err := json.Marshal(e)
	if err != nil {
		l.errs.Add(1)
		return
	}
	l.pending.Add(1)
	select {
	case l.q <- auditLine{b: append(raw, '\n')}:
	default:
		l.pending.Done()
		l.errs.Add(1)
		if !l.dropping.Swap(true) {
			log.Printf("blockmaster: audit log queue full: best-effort audit lines are being dropped (counted in /status/audit)")
		}
	}
}

// attempt writes a strict call's attempt line before its handler runs, and
// waits for the writer within AuditAttemptBound. Any failure (queue full,
// write error, timeout) is ErrAuditUnavailable: the caller refuses the call.
func (l *AuditLog) attempt(method string, p Principal) error {
	if l == nil {
		return nil // no audit log configured
	}
	raw, err := json.Marshal(AuditEvent{Time: l.now(), Event: "attempt", Method: method, Role: p.Role, ID: p.ID, Fingerprint: p.Fingerprint, Result: "pending"})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAuditUnavailable, err)
	}
	done := make(chan error, 1)
	l.pending.Add(1)
	select {
	case l.q <- auditLine{b: append(raw, '\n'), done: done}:
	default:
		l.pending.Done()
		return fmt.Errorf("%w: the audit queue is full", ErrAuditUnavailable)
	}
	timer := time.NewTimer(AuditAttemptBound)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%w: %v", ErrAuditUnavailable, err)
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("%w: the attempt line was not written within %v", ErrAuditUnavailable, AuditAttemptBound)
	}
}

// Flush waits until every queued line has been written (or failed).
func (l *AuditLog) Flush() { l.pending.Wait() }

// call records one call's outcome under the rules above. p is zero when
// the caller could not be authenticated.
func (l *AuditLog) call(method string, p Principal, err error) {
	if l == nil || (err == nil && p.Role == RoleNode) {
		return // accepted node calls are not audited, and wait on nothing
	}
	now := l.now()
	e := AuditEvent{Time: now, Event: "call", Method: method, Role: p.Role, ID: p.ID, Fingerprint: p.Fingerprint, Result: "ok"}
	if err != nil {
		st, _ := status.FromError(err)
		e.Result, e.Reason = st.Code().String(), st.Message()
	}
	if p.Role == RoleNode || (p.Role == "" && err != nil) {
		key := p.Fingerprint + "|" + e.Result + "|" + method
		l.mu.Lock()
		last, seen := l.refused[key]
		if !seen || now.Sub(last) >= time.Minute {
			l.refused[key] = now
		}
		l.mu.Unlock()
		if seen && now.Sub(last) < time.Minute {
			return
		}
	}
	l.enqueue(e)
}

// Reload records a role-file reload and its outcome.
func (l *AuditLog) Reload(path string, err error) {
	if l == nil {
		return
	}
	e := AuditEvent{Time: l.now(), Event: "reload", Result: "ok", Reason: path}
	if err != nil {
		e.Result, e.Reason = "failed", path+": "+err.Error()
	}
	l.enqueue(e)
}

// WriteErrors is how many audit lines were dropped (queue full) or could not
// be written.
func (l *AuditLog) WriteErrors() int { return int(l.errs.Load()) }
