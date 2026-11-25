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
	for _, id := range []string{"Node_A", "a..b", "a.-b", "a-.b", "a.", strings.Repeat("a", 64)} {
		if b, err := runCerts(t, t.TempDir(), id); err == nil || !strings.Contains(string(b), "cannot name a Kubernetes Secret") {
			t.Fatalf("server id %q: %v\n%s, want refused", id, err, b)
		}
	}

	// a later run with one new id keeps the earlier ids in the role file
	if b, err := runCerts(t, out, "node-c"); err != nil {
		t.Fatalf("second run: %v\n%s", err, b)
	}
	roles, err := os.ReadFile(filepath.Join(out, "roles"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		if !strings.Contains(string(roles), " node "+id+"\n") {
			t.Fatalf("role file after adding node-c lacks %s:\n%s", id, roles)
		}
	}

	// a regenerated CA reissues every leaf: node-a's certificate is replaced
	// by one the new CA signed, and the role file names the new one
	before, err := os.ReadFile(filepath.Join(out, "node-node-a.crt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"ca.crt", "ca.key"} {
		if err := os.Remove(filepath.Join(out, f)); err != nil {
			t.Fatal(err)
		}
	}
	if b, err := runCerts(t, out, "node-a"); err != nil {
		t.Fatalf("run with a new CA: %v\n%s", err, b)
	}
	after, err := os.ReadFile(filepath.Join(out, "node-node-a.crt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) == string(before) {
		t.Fatal("node-a's certificate signed by the old CA was kept after the CA was regenerated")
	}
	table, err = auth.LoadRoleFile(filepath.Join(out, "roles"))
	if err != nil {
		t.Fatal(err)
	}
	a = auth.NewAuthenticator(table, nil)
	for _, c := range []struct{ name, role, id string }{{"node-node-a", "node", "node-a"}, {"node-node-b", "node", "node-b"}, {"csi-node", "viewer", "sw-block-csi-node"}} {
		client := link.TLSFiles{Cert: filepath.Join(out, c.name+".crt"), Key: filepath.Join(out, c.name+".key"), CA: filepath.Join(out, "ca.crt")}
		p, err := handshake(t, master, client, a)
		if err != nil {
			t.Fatalf("%s after the CA was regenerated: %v", c.name, err)
		}
		if string(p.Role) != c.role || p.ID != c.id {
			t.Fatalf("%s authenticated as %s %s, want %s %s", c.name, p.Role, p.ID, c.role, c.id)
		}
	}
}

func runCerts(t *testing.T, out string, ids ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command("bash", append([]string{filepath.Join(repoRoot(t), "scripts", "k8s-alpha-certs.sh"), "kube-system", out}, ids...)...)
	cmd.Env = append(os.Environ(), "SW_BLOCK_TLS_APPLY=0")
	return cmd.CombinedOutput()
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
