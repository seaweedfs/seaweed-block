package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// The per-call check never blocks on the role file: while a reload holds the
// authenticator's only lock (the stream wake-up channel), PeerIdentity still
// answers, from the table installed last.
func TestPeerIdentityNeverWaitsForAReload(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	leaf := ca.Issue(t, "s1", time.Now().Add(time.Hour))
	table, err := ParseRoleFile(strings.NewReader(leaf.Fingerprint + " node s1\n"))
	if err != nil {
		t.Fatal(err)
	}
	a := NewAuthenticator(table, nil)
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			VerifiedChains: [][]*x509.Certificate{{leaf.Cert, ca.Cert}},
		}},
	})

	a.mu.Lock() // a reload in progress
	done := make(chan error, 1)
	go func() {
		p, err := a.PeerIdentity(ctx)
		if err == nil && (p.Role != RoleNode || p.ID != "s1") {
			err = context.Canceled
		}
		done <- err
	}()
	select {
	case err := <-done:
		a.mu.Unlock()
		if err != nil {
			t.Fatalf("PeerIdentity during a reload: %v", err)
		}
	case <-time.After(2 * time.Second):
		a.mu.Unlock()
		t.Fatal("PeerIdentity blocked while a reload held the authenticator's lock")
	}
}
