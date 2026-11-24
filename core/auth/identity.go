// Package auth decides who calls blockmaster (phase 0): the caller is the
// leaf certificate of the mutual-TLS connection, verified to the operator's
// CA, pinned by its SHA-256 fingerprint and bound to a role and an id by the
// role file (TERM-CALLER-001). No request field ever names the caller.
package auth

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Role is what a principal may do. viewer < operator < admin (each includes
// the one before) are people and tools; node (a volume host) stands apart.
type Role string

const (
	RoleViewer   Role = "viewer"
	RoleOperator Role = "operator"
	RoleAdmin    Role = "admin"
	RoleNode     Role = "node"
)

func parseRole(s string) (Role, bool) {
	switch r := Role(s); r {
	case RoleViewer, RoleOperator, RoleAdmin, RoleNode:
		return r, true
	}
	return "", false
}

// Principal is who a call comes from.
type Principal struct {
	Role        Role
	ID          string // a node's server id; a person's or tool's name
	Fingerprint string // "sha256:<hex>" of the leaf DER
	NotAfter    time.Time
}

type entry struct {
	role Role
	id   string
}

// Table is a parsed role file: fingerprint -> (role, id). Configuration
// written only by the operator; never written by the master.
type Table struct {
	byFingerprint map[string]entry
}

// Len is the number of principals in the table.
func (t *Table) Len() int { return len(t.byFingerprint) }

// ParseRoleFile reads lines "sha256:<64 hex> <role> <id>"; blank lines and
// lines starting with # are ignored. Any malformed line, unknown role or
// repeated fingerprint makes the whole file an error.
func ParseRoleFile(r io.Reader) (*Table, error) {
	t := &Table{byFingerprint: map[string]entry{}}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 3 {
			return nil, fmt.Errorf("role file line %d: want \"sha256:<hex> <role> <id>\", got %d fields", n, len(f))
		}
		fp := strings.ToLower(f[0])
		hexPart, ok := strings.CutPrefix(fp, "sha256:")
		if raw, err := hex.DecodeString(hexPart); !ok || err != nil || len(raw) != sha256.Size {
			return nil, fmt.Errorf("role file line %d: %q is not a sha256:<64 hex> fingerprint", n, f[0])
		}
		role, ok := parseRole(f[1])
		if !ok {
			return nil, fmt.Errorf("role file line %d: unknown role %q (viewer, operator, admin, node)", n, f[1])
		}
		if _, dup := t.byFingerprint[fp]; dup {
			return nil, fmt.Errorf("role file line %d: fingerprint %s appears twice", n, fp)
		}
		t.byFingerprint[fp] = entry{role: role, id: f[2]}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("role file: %w", err)
	}
	return t, nil
}

// LoadRoleFile parses the role file at path.
func LoadRoleFile(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("role file: %w", err)
	}
	defer f.Close()
	return ParseRoleFile(f)
}

// Authenticator holds the current table (swapped atomically: a call never
// blocks on a reload) and the master's clock.
type Authenticator struct {
	table atomic.Pointer[Table]
	now   func() time.Time

	mu      sync.Mutex
	changed chan struct{} // closed (and replaced) by every Swap
	audit   *AuditLog     // nil: no audit log
}

// SetAudit sends every call's authorization outcome and every reload to l.
// Set before serving.
func (a *Authenticator) SetAudit(l *AuditLog) { a.audit = l }

// NewAuthenticator starts with t; now nil is time.Now.
func NewAuthenticator(t *Table, now func() time.Time) *Authenticator {
	if now == nil {
		now = time.Now
	}
	a := &Authenticator{now: now, changed: make(chan struct{})}
	a.table.Store(t)
	return a
}

// Swap installs t for every later call, and wakes every open stream to
// check its caller against it.
func (a *Authenticator) Swap(t *Table) {
	a.table.Store(t)
	a.mu.Lock()
	close(a.changed)
	a.changed = make(chan struct{})
	a.mu.Unlock()
}

// Changed is closed by the next Swap.
func (a *Authenticator) Changed() <-chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.changed
}

// Reload parses path and installs it; on any error the current table stays.
// The validated table is published before the reload is logged: a revocation
// never waits on the audit writer.
func (a *Authenticator) Reload(path string) error {
	t, err := LoadRoleFile(path)
	if err == nil {
		a.Swap(t)
	}
	a.audit.Reload(path, err)
	return err
}

// Fingerprint is the role-file form of a DER certificate's fingerprint.
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func unauthenticated(format string, args ...any) error {
	return status.Errorf(codes.Unauthenticated, format, args...)
}

// PeerIdentity is the only decider of who calls the master. It reads the
// leaf certificate the TLS handshake verified to the client CA (never a
// request field) and runs on every call. Every failure is UNAUTHENTICATED,
// never retryable: no verified client certificate; a fingerprint not in the
// current table; a CN that differs from the table's id; now at or past the
// certificate's NotAfter on the master's clock.
func (a *Authenticator) PeerIdentity(ctx context.Context) (Principal, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return Principal{}, unauthenticated("no client certificate: the call did not come over mutual TLS")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return Principal{}, unauthenticated("no client certificate verified by the client CA")
	}
	leaf := info.State.VerifiedChains[0][0]
	fp := Fingerprint(leaf.Raw)
	e, ok := a.table.Load().byFingerprint[fp]
	if !ok {
		return Principal{}, unauthenticated("certificate %s (CN=%s) is not in the role file", fp, leaf.Subject.CommonName)
	}
	if leaf.Subject.CommonName != e.id {
		return Principal{}, unauthenticated("certificate CN=%s does not match role-file id %s for fingerprint %s", leaf.Subject.CommonName, e.id, fp)
	}
	if !a.now().Before(leaf.NotAfter) {
		return Principal{}, unauthenticated("certificate %s (CN=%s) expired at %s", fp, e.id, leaf.NotAfter.UTC().Format(time.RFC3339))
	}
	return Principal{Role: e.role, ID: e.id, Fingerprint: fp, NotAfter: leaf.NotAfter}, nil
}

// AuditDropped is how many audit lines were dropped or not written, and
// whether an audit log is configured.
func (a *Authenticator) AuditDropped() (int, bool) {
	if a == nil || a.audit == nil {
		return 0, false
	}
	return a.audit.WriteErrors(), true
}
