package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/master"
)

type lockedLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// A failure AFTER the forced move is issued (here the binding
// reconciliation, made to fail by moving the binding store's directory away)
// is reported through the RPC, the client and the CLI as an unknown outcome
// with the same-request-id recovery, never as a refusal ("nothing moved")
// and never with a fresh request id; the master's audit log records the
// issue; a rerun with the same id then returns the stored grant.
func TestWorkspaceForceMovePostIssueFailureKeepsTheOutcome(t *testing.T) {
	logs := &lockedLog{}
	lifecycleDir := t.TempDir()
	cfg := master.Config{WorkspaceMaxClockSkew: 100 * time.Millisecond, Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: lifecycleDir, WorkspaceRPC: true,
		Logger: log.New(logs, "", 0),
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
			return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42}, nil
		}}
	h, stop := startDeleteMaster(t, cfg)
	defer stop()
	seedForcedVolume(t, h)
	st := h.Publisher().WorkspaceState("vol-f")
	skew := time.Duration(st.Lease.MaxClockSkewNS)
	if skew < cfg.WorkspaceMaxClockSkew {
		skew = cfg.WorkspaceMaxClockSkew
	}
	time.Sleep(time.Until(time.Unix(0, st.Lease.ExpiresAtNS).Add(skew)) + 100*time.Millisecond)

	bindings := filepath.Join(lifecycleDir, "workspace-bindings")
	if err := os.Rename(bindings, bindings+".aside"); err != nil {
		t.Fatal(err)
	}
	args := []string{"ops", "workspace", "force-move", "--master", h.Addr(), "--volume", "vol-f", "--target-host", "node-b", "--target-data", "127.0.0.1:29200",
		"--target-ctrl", "127.0.0.1:29100", "--request-id", "force-1", "--confirm-forced", "vol-f@1"}
	code, out, errOut := runCLI(args)
	if line, ok := h.Publisher().VolumeAuthorityLine("vol-f"); !ok || line.Epoch != 2 {
		t.Fatalf("fixture: the forced move was not issued before the failure: %+v", line)
	}
	if code != wsExitUnknown || strings.Contains(errOut, "nothing moved") || strings.Contains(out, "nothing moved") ||
		!strings.Contains(errOut, "rerun with the SAME --request-id force-1") || strings.Contains(strings.ToLower(errOut), "new request id") {
		t.Fatalf("post-issue failure: exit %d stdout %q stderr %s; want exit 3 with the same-id recovery and no refusal", code, out, errOut)
	}
	if !strings.Contains(logs.String(), `forced move issued volume="vol-f" request="force-1"`) {
		t.Fatalf("issued forced move missing from the audit log:\n%s", logs.String())
	}
	if err := os.Rename(bindings+".aside", bindings); err != nil {
		t.Fatal(err)
	}
	code, out, errOut = runCLI(args)
	if code != wsExitOK || !strings.HasPrefix(out, "forced move issued: volume vol-f request force-1: r1@1 on node-a -> r1@2 on node-b") {
		t.Fatalf("rerun with the same id: exit %d stdout %q stderr %s", code, out, errOut)
	}
}
