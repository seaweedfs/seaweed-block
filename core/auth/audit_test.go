package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"google.golang.org/grpc"
)

// The audit log: every person's call (accepted or refused) and every reload
// is a line; an accepted node call is not; a refused node call is a line at
// most once a minute per (fingerprint, reason, method).
func TestAuditLog(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	node := ca.Issue(t, "s1", time.Now().Add(time.Hour))
	op := ca.Issue(t, "alice", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, node.Fingerprint+" node s1", op.Fingerprint+" operator alice"), nil)
	now := time.Unix(2000, 0)
	var buf bytes.Buffer
	log := auth.NewAuditLog(&buf, func() time.Time { return now })
	a.SetAudit(log)
	rules := map[string]auth.Rule{"/renew": {Node: true}, "/move": {Min: auth.RoleOperator}, "/delete": {Min: auth.RoleAdmin}}
	call := func(leaf *authtest.Leaf, method string) {
		_, _ = a.UnaryInterceptor(rules)(peerCtx(leaf, ca), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { return nil, nil })
	}
	lines := func() []auth.AuditEvent {
		log.Flush() // the writer is asynchronous
		var out []auth.AuditEvent
		for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if l == "" {
				continue
			}
			var e auth.AuditEvent
			if err := json.Unmarshal([]byte(l), &e); err != nil {
				t.Fatalf("audit line %q: %v", l, err)
			}
			out = append(out, e)
		}
		return out
	}
	call(node, "/renew")  // accepted node call: not audited
	call(op, "/move")     // person, accepted
	call(op, "/delete")   // person, refused
	call(node, "/delete") // node, refused
	call(node, "/delete") // the same refusal within the minute: not again
	now = now.Add(61 * time.Second)
	call(node, "/delete") // a minute later: again
	got := lines()
	if len(got) != 4 {
		t.Fatalf("%d audit lines; want 4: %s", len(got), buf.String())
	}
	if got[0].ID != "alice" || got[0].Method != "/move" || got[0].Result != "ok" ||
		got[1].ID != "alice" || got[1].Result != "PermissionDenied" ||
		got[2].ID != "s1" || got[2].Result != "PermissionDenied" || got[3].ID != "s1" {
		t.Fatalf("audit lines %+v", got)
	}
	path := filepath.Join(t.TempDir(), "roles")
	if err := os.WriteFile(path, []byte("not a role file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = a.Reload(path)
	if err := os.WriteFile(path, []byte(op.Fingerprint+" operator alice\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = a.Reload(path)
	got = lines()
	if len(got) != 6 || got[4].Event != "reload" || got[4].Result != "failed" || got[5].Event != "reload" || got[5].Result != "ok" {
		t.Fatalf("reload audit lines %+v", got[4:])
	}
}
