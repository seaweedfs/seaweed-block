package scripts_test

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// The alpha certificate helper issues what blockmaster accepts: a role file
// that parses, a node certificate whose CN is its server id and which
// authenticates as that node over a real mutual-TLS link (the in-cluster
// clients' certificates: TestClientSecretsCarryTheLeastRole).
func TestK8sAlphaCertsAreAcceptedByBlockmaster(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash and openssl script")
	}
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not installed")
	}
	out := t.TempDir()
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "scripts", "k8s-alpha-certs.sh"), "kube-system", out, "node-a", "node-b")
	cmd.Env = append(os.Environ(), "SW_BLOCK_TLS_APPLY=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("k8s-alpha-certs.sh: %v\n%s", err, b)
	}
	table, err := auth.LoadRoleFile(filepath.Join(out, "roles"))
	if err != nil {
		t.Fatal(err)
	}
	a := auth.NewAuthenticator(table, nil)
	master := link.TLSFiles{Cert: filepath.Join(out, "master.crt"), Key: filepath.Join(out, "master.key"), CA: filepath.Join(out, "ca.crt")}
	for _, c := range []struct {
		name, role, id string
	}{{"node-node-a", "node", "node-a"}, {"node-node-b", "node", "node-b"}} {
		client := link.TLSFiles{Cert: filepath.Join(out, c.name+".crt"), Key: filepath.Join(out, c.name+".key"), CA: filepath.Join(out, "ca.crt")}
		p, err := handshake(t, master, client, a)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if string(p.Role) != c.role || p.ID != c.id {
			t.Fatalf("%s authenticated as %s %s, want %s %s", c.name, p.Role, p.ID, c.role, c.id)
		}
	}
	raw, err := os.ReadFile(filepath.Join(out, "master.crt"))
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(raw)
	leaf, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := leaf.VerifyHostname("blockmaster.kube-system.svc.cluster.local"); err != nil {
		t.Fatalf("master certificate does not name the Service the clients dial: %v", err)
	}
	cmd = exec.Command("bash", filepath.Join(repoRoot(t), "scripts", "k8s-alpha-certs.sh"), "kube-system", t.TempDir(), "Node_A")
	cmd.Env = append(os.Environ(), "SW_BLOCK_TLS_APPLY=0")
	if b, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(b), "cannot name a Kubernetes Secret") {
		t.Fatalf("server id Node_A: %v\n%s, want refused", err, b)
	}
}

// handshake runs one mutual-TLS gRPC call from client to a server with the
// master's credentials and returns who the server says is calling.
func handshake(t *testing.T, master, client link.TLSFiles, a *auth.Authenticator) (auth.Principal, error) {
	t.Helper()
	srvCreds, err := link.ServerCredentials(master)
	if err != nil {
		return auth.Principal{}, err
	}
	got := make(chan auth.Principal, 1)
	srv := grpc.NewServer(srvCreds, grpc.UnknownServiceHandler(func(_ any, ss grpc.ServerStream) error {
		p, err := a.PeerIdentity(ss.Context())
		if err != nil {
			return err
		}
		got <- p
		return nil
	}))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return auth.Principal{}, err
	}
	go srv.Serve(lis)
	defer srv.Stop()
	dial, err := link.ClientCredentials(client, "blockmaster.kube-system.svc.cluster.local")
	if err != nil {
		return auth.Principal{}, err
	}
	conn, err := link.DialMaster(lis.Addr().String(), dial)
	if err != nil {
		return auth.Principal{}, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	callErr := conn.Invoke(ctx, "/probe.Probe/Who", &emptypb.Empty{}, &emptypb.Empty{})
	select {
	case p := <-got:
		return p, nil
	default:
		return auth.Principal{}, callErr
	}
}
