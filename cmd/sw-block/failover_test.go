package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeFailoverRuntime struct {
	calls []ops.FailbackRuntimeRequest
	res   ops.FailbackRuntimeResult
	err   error
}

func (f *fakeFailoverRuntime) ExecuteFailback(_ context.Context, req ops.FailbackRuntimeRequest) (ops.FailbackRuntimeResult, error) {
	f.calls = append(f.calls, req)
	return f.res, f.err
}

func withFailoverRuntime(t *testing.T, f *fakeFailoverRuntime) *string {
	t.Helper()
	withClientIdentity(t)
	var dialed string
	prev := opsFailoverRuntimeFactory
	opsFailoverRuntimeFactory = func(master string) failoverRuntime { dialed = master; return f }
	t.Cleanup(func() { opsFailoverRuntimeFactory = prev })
	return &dialed
}

func failoverArgs(extra ...string) []string {
	args := []string{"ops", "failover", "--master", "m:9333", "--volume", "v1", "--target", "r2",
		"--target-data", "d2:1", "--target-ctrl", "c2:1", "--expect-current", "r1@4", "--confirm-old-node-off", "v1"}
	return append(args, extra...)
}

// The operator's failover sends one FailbackService call carrying the
// expected current line it was given; a move is reported and exits 0.
func TestOpsFailoverSendsTheExpectedLineAndReportsTheMove(t *testing.T) {
	f := &fakeFailoverRuntime{res: ops.FailbackRuntimeResult{FailbackStarted: true, AuthorityEpochAdvanced: true, SinglePrimaryAfterFailback: true, PublishTargetSwappedAfterFailback: true}}
	dialed := withFailoverRuntime(t, f)
	var out, errb bytes.Buffer
	if rc := run(failoverArgs(), &out, &errb); rc != 0 {
		t.Fatalf("rc=%d stderr=%s", rc, errb.String())
	}
	if *dialed != "m:9333" || len(f.calls) != 1 {
		t.Fatalf("dialed %q calls %d; want one call to m:9333", *dialed, len(f.calls))
	}
	r := f.calls[0]
	if r.VolumeID != "v1" || r.ReplicaID != "r2" || r.TargetDataAddr != "d2:1" || r.TargetCtrlAddr != "c2:1" ||
		r.ExpectedCurrentReplicaID != "r1" || r.ExpectedCurrentEpoch != 4 || !r.FrontendFencedBeforeFailback {
		t.Fatalf("request %+v", r)
	}
	if !strings.Contains(out.String(), "result=moved") {
		t.Fatalf("stdout %q; want result=moved", out.String())
	}
}

// A refusal by blockmaster (for example a fencing round that does not allow
// the target) is reported and exits 1; a call that returns without moving
// the line is not a move either.
func TestOpsFailoverReportsARefusal(t *testing.T) {
	for _, f := range []*fakeFailoverRuntime{
		{err: status.Error(codes.FailedPrecondition, "execute failback: fenced move: the fencing round at epoch 5 does not allow r2")},
		{res: ops.FailbackRuntimeResult{FailbackStarted: true}},
	} {
		withFailoverRuntime(t, f)
		var out, errb bytes.Buffer
		if rc := run(failoverArgs(), &out, &errb); rc != 1 {
			t.Fatalf("rc=%d; want 1 (stdout %q stderr %q)", rc, out.String(), errb.String())
		}
		if strings.Contains(out.String(), "result=moved") {
			t.Fatalf("stdout %q reports a move", out.String())
		}
	}
}

// Every argument guard refuses locally, before any call: the old node must
// be confirmed off for exactly this volume, the expected line must be
// <replica>@<epoch> with a nonzero epoch, and the target must not be the
// current primary.
func TestOpsFailoverRefusesBadArgumentsWithoutACall(t *testing.T) {
	for name, args := range map[string][]string{
		"no confirmation":             {"ops", "failover", "--master", "m:9333", "--volume", "v1", "--target", "r2", "--target-data", "d2:1", "--target-ctrl", "c2:1", "--expect-current", "r1@4"},
		"confirmation for another":    failoverArgs("--confirm-old-node-off", "v2"),
		"expected line without epoch": failoverArgs("--expect-current", "r1"),
		"expected epoch zero":         failoverArgs("--expect-current", "r1@0"),
		"expected epoch not a number": failoverArgs("--expect-current", "r1@x"),
		"target is current":           failoverArgs("--expect-current", "r2@4"),
		"no master":                   {"ops", "failover", "--volume", "v1", "--target", "r2", "--target-data", "d2:1", "--target-ctrl", "c2:1", "--expect-current", "r1@4", "--confirm-old-node-off", "v1"},
		"stray argument":              append(failoverArgs(), "extra"),
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeFailoverRuntime{}
			withFailoverRuntime(t, f)
			var out, errb bytes.Buffer
			if rc := run(args, &out, &errb); rc != 2 || len(f.calls) != 0 {
				t.Fatalf("rc=%d calls=%d; want 2 and no call (stderr %q)", rc, len(f.calls), errb.String())
			}
		})
	}
}

// The alpha stack deploys the manual failover its notes describe: blockmaster
// serves the admin failback RPC, the executor runs with the admin client
// Secret and execution on, the CRD is applied, and uninstall removes both.
func TestAlphaStackDeploysTheManualFailover(t *testing.T) {
	read := func(p string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join("..", "..", p))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	const crd = "charts/seaweed-block/crds/swblockreplicafailbacks.block.seaweedfs.com.yaml"
	read(crd)
	for file, wants := range map[string][]string{
		"deploy/k8s/alpha/block-stack.yaml":       {`"--failback-runtime-rpc"`},
		"deploy/k8s/alpha/failback-executor.yaml": {"secretName: sw-block-failback-executor-tls", `"--enable-execution"`, `"--execution-policy"`, `"--failback-runtime-grpc-addr=blockmaster.kube-system.svc.cluster.local:9333"`},
		"scripts/install-k8s-alpha.sh":            {crd, `render-k8s-alpha-image.sh" "$IMAGE" "$ROOT/deploy/k8s/alpha/failback-executor.yaml"`, `kubectl apply -f "$ARTIFACT_DIR/failback-executor.rendered.yaml"`},
		"scripts/run-alpha-app-demo.sh":           {crd, `render-k8s-alpha-image.sh" "$IMAGE" "$ROOT/deploy/k8s/alpha/failback-executor.yaml"`, `kubectl apply -f "$ARTIFACT_DIR/failback-executor.rendered.yaml"`},
		"scripts/uninstall-k8s-alpha.sh":          {crd, "deploy/k8s/alpha/failback-executor.yaml"},
		"scripts/k8s-alpha-certs.sh":              {"failback-executor:admin"},
	} {
		s := read(file)
		for _, w := range wants {
			if !strings.Contains(s, w) {
				t.Errorf("%s: missing %s", file, w)
			}
		}
	}
}

// A call that does not get blockmaster's decision back (the connection or
// the deadline failed) is reported as unknown, exit 3, never as a refusal:
// the line may have moved.
func TestOpsFailoverReportsAnUnknownOutcome(t *testing.T) {
	for name, err := range map[string]error{
		"unavailable":      status.Error(codes.Unavailable, "connection reset"),
		"deadline status":  status.Error(codes.DeadlineExceeded, "deadline"),
		"context deadline": context.DeadlineExceeded,
		"not a status":     errors.New("dial tcp: i/o timeout"),
	} {
		t.Run(name, func(t *testing.T) {
			withFailoverRuntime(t, &fakeFailoverRuntime{err: err})
			var out, errb bytes.Buffer
			if rc := run(failoverArgs(), &out, &errb); rc != 3 || !strings.Contains(out.String(), "result=unknown") {
				t.Fatalf("rc=%d stdout=%q; want 3 and result=unknown", rc, out.String())
			}
		})
	}
}

// The admin command never dials without a complete, valid client identity:
// unset, partial or unreadable TLS settings are refused locally (exit 2, no
// call); a valid identity makes the call.
func TestOpsFailoverRequiresAClientIdentity(t *testing.T) {
	dir := t.TempDir()
	cert, key, ca := writeClientIdentity(t, dir)
	junk := filepath.Join(dir, "junk.pem")
	if err := os.WriteFile(junk, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		cert, key, ca string
		calls         int
	}{
		"unset":      {"", "", "", 0},
		"cert only":  {cert, "", "", 0},
		"no CA":      {cert, key, "", 0},
		"unreadable": {junk, junk, junk, 0},
		"valid":      {cert, key, ca, 1},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeFailoverRuntime{res: ops.FailbackRuntimeResult{FailbackStarted: true, AuthorityEpochAdvanced: true, SinglePrimaryAfterFailback: true}}
			prev := opsFailoverRuntimeFactory
			opsFailoverRuntimeFactory = func(string) failoverRuntime { return f }
			t.Cleanup(func() { opsFailoverRuntimeFactory = prev })
			t.Setenv(link.EnvTLSCert, c.cert)
			t.Setenv(link.EnvTLSKey, c.key)
			t.Setenv(link.EnvTLSCA, c.ca)
			var out, errb bytes.Buffer
			rc := run(failoverArgs(), &out, &errb)
			if len(f.calls) != c.calls {
				t.Fatalf("calls=%d; want %d (rc=%d stderr %q)", len(f.calls), c.calls, rc, errb.String())
			}
			if c.calls == 0 && rc != 2 {
				t.Fatalf("rc=%d; want 2", rc)
			}
			if c.calls == 1 && rc != 0 {
				t.Fatalf("rc=%d stderr %q; want 0", rc, errb.String())
			}
		})
	}
}

// withClientIdentity sets a valid admin client identity for the test.
func withClientIdentity(t *testing.T) {
	t.Helper()
	cert, key, ca := writeClientIdentity(t, t.TempDir())
	t.Setenv(link.EnvTLSCert, cert)
	t.Setenv(link.EnvTLSKey, key)
	t.Setenv(link.EnvTLSCA, ca)
}

// writeClientIdentity writes a CA and a client certificate it signed.
func writeClientIdentity(t *testing.T, dir string) (certPath, keyPath, caPath string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cliKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cliTmpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "sw-block-admin"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTmpl, caCert, &cliKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(cliKey)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	return write("client.crt", "CERTIFICATE", cliDER), write("client.key", "EC PRIVATE KEY", keyDER), write("ca.crt", "CERTIFICATE", caDER)
}

// The executor runs the image the install selected, like the rest of the
// stack: rendering it with a registry image replaces the local image and its
// never-pull policy.
func TestAlphaFailbackExecutorRendersTheSelectedImage(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("no bash")
	}
	root := filepath.Join("..", "..")
	out := filepath.Join(t.TempDir(), "rendered.yaml")
	const image = "registry.example.com/seaweedfs/seaweed-block:v9.9"
	cmd := exec.Command(bash, filepath.Join(root, "scripts", "render-k8s-alpha-image.sh"), image, filepath.Join(root, "deploy", "k8s", "alpha", "failback-executor.yaml"), out)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("render: %v: %s", err, b)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	r := string(b)
	if !strings.Contains(r, "image: "+image) || !strings.Contains(r, "imagePullPolicy: IfNotPresent") || strings.Contains(r, "sw-block:local") || strings.Contains(r, "imagePullPolicy: Never") {
		t.Fatalf("rendered executor does not run the selected image: %s", r)
	}
}
