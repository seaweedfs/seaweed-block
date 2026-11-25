package auth_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// peerCtx is a call context whose connection presented leaf, verified to ca
// (as the TLS handshake with a client CA leaves it); leaf nil = no client
// certificate.
func peerCtx(leaf *authtest.Leaf, ca *authtest.CA) context.Context {
	st := tls.ConnectionState{HandshakeComplete: true}
	if leaf != nil {
		st.PeerCertificates = []*x509.Certificate{leaf.Cert}
		st.VerifiedChains = [][]*x509.Certificate{{leaf.Cert, ca.Cert}}
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: st}})
}

func table(t *testing.T, lines ...string) *auth.Table {
	t.Helper()
	tab, err := auth.ParseRoleFile(strings.NewReader(strings.Join(lines, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	return tab
}

// T1: PeerIdentity is the one decider of who is calling: the CA-verified
// leaf's fingerprint, bound to a role and id by the role file, with CN equal
// to the id and the certificate not expired on the master's clock. Every
// failure is UNAUTHENTICATED.
func TestPeerIdentity(t *testing.T) {
	ca := authtest.NewCA(t, "test-ca")
	expiry := time.Now().Add(time.Hour)
	node := ca.Issue(t, "s1", expiry)
	op := ca.Issue(t, "alice", expiry)
	admin := ca.Issue(t, "ops-admin", expiry)
	wrongCN := ca.Issue(t, "s2", expiry)
	unlisted := ca.Issue(t, "s3", expiry)
	tab := table(t,
		"# fingerprint role id",
		node.Fingerprint+" node s1",
		op.Fingerprint+" operator alice",
		admin.Fingerprint+"   admin   ops-admin",
		wrongCN.Fingerprint+" node s1", // pasted on the wrong line
	)
	now := time.Now()
	a := auth.NewAuthenticator(tab, func() time.Time { return now })

	for _, c := range []struct {
		name string
		ctx  context.Context
		role auth.Role
		id   string
	}{
		{"node", peerCtx(node, ca), auth.RoleNode, "s1"},
		{"operator", peerCtx(op, ca), auth.RoleOperator, "alice"},
		{"admin", peerCtx(admin, ca), auth.RoleAdmin, "ops-admin"},
	} {
		p, err := a.PeerIdentity(c.ctx)
		if err != nil || p.Role != c.role || p.ID != c.id || p.NotAfter.IsZero() || !strings.HasPrefix(p.Fingerprint, "sha256:") {
			t.Fatalf("%s: %+v %v; want role %s id %s", c.name, p, err, c.role, c.id)
		}
	}
	refused := func(name string, ctx context.Context, want string) {
		t.Helper()
		_, err := a.PeerIdentity(ctx)
		if status.Code(err) != codes.Unauthenticated || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: %v; want UNAUTHENTICATED naming %q", name, err, want)
		}
	}
	refused("no peer", context.Background(), "no client certificate")
	refused("no client certificate", peerCtx(nil, ca), "no client certificate")
	other := authtest.NewCA(t, "other-ca")
	stray := other.Issue(t, "s1", expiry)
	strayCtx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
		HandshakeComplete: true, PeerCertificates: []*x509.Certificate{stray.Cert}}}}) // not verified to the CA
	refused("a certificate the CA did not verify", strayCtx, "no client certificate")
	refused("fingerprint not in the table", peerCtx(unlisted, ca), "not in the role file")
	refused("CN differs from the id", peerCtx(wrongCN, ca), "does not match role-file id")
	now = node.Cert.NotAfter.Add(-time.Nanosecond)
	if _, err := a.PeerIdentity(peerCtx(node, ca)); err != nil {
		t.Fatalf("1 ns before NotAfter: %v", err)
	}
	now = node.Cert.NotAfter
	refused("at NotAfter", peerCtx(node, ca), "expired")
	now = time.Now()

	// A table swapped between two calls on one connection: the next call sees it.
	ctx := peerCtx(node, ca)
	if _, err := a.PeerIdentity(ctx); err != nil {
		t.Fatal(err)
	}
	a.Swap(table(t, op.Fingerprint+" operator alice"))
	refused("after a reload that removed the node", ctx, "not in the role file")
}

// T3: a good role file swaps the table; a malformed one keeps the previous
// table and reports the error; an unreadable file at start is an error.
func TestRoleFileReload(t *testing.T) {
	ca := authtest.NewCA(t, "test-ca")
	node := ca.Issue(t, "s1", time.Now().Add(time.Hour))
	dir := t.TempDir()
	path := filepath.Join(dir, "roles")
	if _, err := auth.LoadRoleFile(path); err == nil {
		t.Fatal("an unreadable role file at start must be an error")
	}
	if err := os.WriteFile(path, []byte(node.Fingerprint+" node s1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tab, err := auth.LoadRoleFile(path)
	if err != nil {
		t.Fatal(err)
	}
	a := auth.NewAuthenticator(tab, nil)
	if _, err := a.PeerIdentity(peerCtx(node, ca)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		"sha256:zz node s1",                // not hex
		node.Fingerprint + " root s1",      // unknown role
		node.Fingerprint + " node",         // no id
		node.Fingerprint + " node s1 more", // extra field
		node.Fingerprint + " node s1\n" + node.Fingerprint + " admin x", // fingerprint twice
	} {
		if err := os.WriteFile(path, []byte(bad+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := a.Reload(path); err == nil {
			t.Fatalf("malformed role file %q reloaded", bad)
		}
		if _, err := a.PeerIdentity(peerCtx(node, ca)); err != nil {
			t.Fatalf("a failed reload (%q) dropped the previous table: %v", bad, err)
		}
	}
	if err := os.WriteFile(path, []byte("# empty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := a.Reload(path); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PeerIdentity(peerCtx(node, ca)); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("after a reload without the node: %v; want UNAUTHENTICATED", err)
	}
}
