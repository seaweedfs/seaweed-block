package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/ops"
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
		{err: errors.New("fenced move: the fencing round at epoch 5 does not allow r2")},
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
		"scripts/install-k8s-alpha.sh":            {crd, "deploy/k8s/alpha/failback-executor.yaml"},
		"scripts/run-alpha-app-demo.sh":           {crd, "deploy/k8s/alpha/failback-executor.yaml"},
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
