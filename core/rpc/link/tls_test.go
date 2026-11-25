package link_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
)

// The master link's mutual TLS: a client with a certificate from the CA is
// served, and the server sees its verified leaf (what PeerIdentity reads); a
// client without a certificate, or with one from another CA, is refused at
// the handshake; partial or wrong files are refused at start.
func TestMasterLinkMutualTLS(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	dir := t.TempDir()
	caPath := ca.WriteCA(t, dir)
	srvCert, srvKey := ca.Issue(t, "blockmaster", time.Now().Add(time.Hour)).WriteFiles(t, dir, "master")
	nodeCert, nodeKey := ca.Issue(t, "s1", time.Now().Add(time.Hour)).WriteFiles(t, dir, "s1")

	if _, err := link.ServerCredentials(link.TLSFiles{Cert: srvCert, Key: srvKey}); err == nil {
		t.Fatal("a server without a CA must be refused at start")
	}
	if _, err := link.ServerCredentials(link.TLSFiles{Cert: srvCert, Key: nodeKey, CA: caPath}); err == nil {
		t.Fatal("a key that does not match its certificate must be refused at start")
	}
	creds, err := link.ServerCredentials(link.TLSFiles{Cert: srvCert, Key: srvKey, CA: caPath})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan string, 4)
	srv := link.NewServer(link.ServerConfig{Unary: []grpc.UnaryServerInterceptor{func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if p, ok := peer.FromContext(ctx); ok {
			if info, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(info.State.VerifiedChains) > 0 {
				seen <- info.State.VerifiedChains[0][0].Subject.CommonName
			}
		}
		return h(ctx, req)
	}}}, creds, link.ConnectionAge(time.Hour))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	check := func(dial grpc.DialOption) error {
		conn, err := link.DialMaster(ln.Addr().String(), dial)
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		return err
	}
	client, err := link.ClientCredentials(link.TLSFiles{Cert: nodeCert, Key: nodeKey, CA: caPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := check(client); err != nil {
		t.Fatalf("a client with the CA's certificate: %v", err)
	}
	if cn := <-seen; cn != "s1" {
		t.Fatalf("server saw verified leaf CN %q; want s1", cn)
	}
	if err := check(nil); err == nil {
		t.Fatal("a plaintext client was served by the TLS master")
	}
	other := authtest.NewCA(t, "other")
	odir := t.TempDir()
	oc, ok := other.Issue(t, "s1", time.Now().Add(time.Hour)).WriteFiles(t, odir, "s1")
	stray, err := link.ClientCredentials(link.TLSFiles{Cert: oc, Key: ok, CA: caPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := check(stray); err == nil {
		t.Fatal("a client certificate from another CA was served")
	}
}
