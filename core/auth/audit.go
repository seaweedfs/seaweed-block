package auth

import (
	"encoding/json"
	"fmt"
	"io"
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

// auditQueue bounds the lines waiting for the writer. A full queue drops the
// line and counts it (WriteErrors): a call never waits on audit I/O.
const auditQueue = 1024

// AuditLog appends events to a writer, one JSON object per line, from one
// writer goroutine. Nothing reads it to decide anything, and no call waits on
// it: accepted node calls (renewals, ACKs, heartbeats, streams) are not
// recorded and return before any lock; every other line is queued (bounded)
// and written in the background. A refused node call is recorded at most once
// per minute per (fingerprint, result, method); every person's call and every
// role-file reload is recorded.
type AuditLog struct {
	mu      sync.Mutex // the refusal dedup only; never held across I/O
	now     func() time.Time
	refused map[string]time.Time
	q       chan []byte
	pending sync.WaitGroup
	errs    atomic.Int64 // lines dropped (queue full) or not written
}

// NewAuditLog writes to w; now nil is time.Now.
func NewAuditLog(w io.Writer, now func() time.Time) *AuditLog {
	if now == nil {
		now = time.Now
	}
	l := &AuditLog{now: now, refused: map[string]time.Time{}, q: make(chan []byte, auditQueue)}
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
		if _, err := w.Write(line); err != nil {
			l.errs.Add(1)
		}
		l.pending.Done()
	}
}

// enqueue queues e for the writer, or drops and counts it when the queue is
// full.
func (l *AuditLog) enqueue(e AuditEvent) {
	raw, err := json.Marshal(e)
	if err != nil {
		l.errs.Add(1)
		return
	}
	l.pending.Add(1)
	select {
	case l.q <- append(raw, '\n'):
	default:
		l.pending.Done()
		l.errs.Add(1)
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
