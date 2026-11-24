package auth

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
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

// AuditLog appends events to a writer, one JSON object per line. Nothing
// reads it to decide anything. Accepted node calls (renewals, ACKs,
// heartbeats, streams) are not recorded; a refused node call is recorded at
// most once per minute per (fingerprint, reason); every person's call and
// every role-file reload is recorded.
type AuditLog struct {
	mu      sync.Mutex
	w       io.Writer
	now     func() time.Time
	refused map[string]time.Time
	errs    int // writes that failed (the log never fails a call)
}

// NewAuditLog writes to w; now nil is time.Now.
func NewAuditLog(w io.Writer, now func() time.Time) *AuditLog {
	if now == nil {
		now = time.Now
	}
	return &AuditLog{w: w, now: now, refused: map[string]time.Time{}}
}

// OpenAuditLog appends to the file at path (created 0600 when missing).
func OpenAuditLog(path string) (*AuditLog, *os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("audit log: %w", err)
	}
	return NewAuditLog(f, nil), f, nil
}

func (l *AuditLog) write(e AuditEvent) {
	raw, err := json.Marshal(e)
	if err == nil {
		_, err = l.w.Write(append(raw, '\n'))
	}
	if err != nil {
		l.errs++
	}
}

// call records one call's outcome under the rules above. p is zero when
// the caller could not be authenticated.
func (l *AuditLog) call(method string, p Principal, err error) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	e := AuditEvent{Time: now, Event: "call", Method: method, Role: p.Role, ID: p.ID, Fingerprint: p.Fingerprint, Result: "ok"}
	if err != nil {
		st, _ := status.FromError(err)
		e.Result, e.Reason = st.Code().String(), st.Message()
	}
	if p.Role == RoleNode || (p.Role == "" && err != nil) {
		if err == nil {
			return // accepted node calls are not audited
		}
		key := p.Fingerprint + "|" + e.Result + "|" + method
		if last, seen := l.refused[key]; seen && now.Sub(last) < time.Minute {
			return
		}
		l.refused[key] = now
	}
	l.write(e)
}

// Reload records a role-file reload and its outcome.
func (l *AuditLog) Reload(path string, err error) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e := AuditEvent{Time: l.now(), Event: "reload", Result: "ok", Reason: path}
	if err != nil {
		e.Result, e.Reason = "failed", path+": "+err.Error()
	}
	l.write(e)
}

// WriteErrors is how many audit lines could not be written.
func (l *AuditLog) WriteErrors() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.errs
}
