package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/master"
)

// The issuing write of a forced move reaches the authority store (renamed
// into place) but the store reports an error (directory sync): the master
// cannot know whether the move is issued, and memory stays unissued. The
// CLI must report an unknown outcome with the same-request-id recovery,
// never "nothing moved", also on a rerun before the master restarts; after a
// restart the master reloads the issued record and a rerun with the same id
// gets the grant.
func TestWorkspaceForceMoveUncertainIssueWriteIsUnknown(t *testing.T) {
	logs := &lockedLog{}
	cfg := master.Config{WorkspaceMaxClockSkew: 100 * time.Millisecond, Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		Logger: log.New(logs, "", 0),
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
			return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42}, nil
		}}
	h, stop := startDeleteMaster(t, cfg)
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	seedForcedVolume(t, h)
	st := h.Publisher().WorkspaceState("vol-f")
	skew := time.Duration(st.Lease.MaxClockSkewNS)
	if skew < cfg.WorkspaceMaxClockSkew {
		skew = cfg.WorkspaceMaxClockSkew
	}
	time.Sleep(time.Until(time.Unix(0, st.Lease.ExpiresAtNS).Add(skew)) + 100*time.Millisecond)

	var faults atomic.Int32
	authority.SetPutAfterRenameFaultForTest(func(r authority.DurableRecord) error {
		if r.Workspace == nil {
			return nil
		}
		for _, m := range r.Workspace.Moves {
			if m.RequestID == "force-1" && m.Issued && faults.Add(1) == 1 {
				return errors.New("injected directory fsync failure after the rename")
			}
		}
		return nil
	})
	t.Cleanup(func() { authority.SetPutAfterRenameFaultForTest(nil) })

	args := func(addr string) []string {
		return []string{"ops", "workspace", "force-move", "--master", addr, "--volume", "vol-f", "--target-host", "node-b", "--target-data", "127.0.0.1:29200",
			"--target-ctrl", "127.0.0.1:29100", "--request-id", "force-1", "--confirm-forced", "vol-f@1"}
	}
	for _, attempt := range []string{"first", "rerun before restart"} {
		code, out, errOut := runCLI(args(h.Addr()))
		if code != wsExitUnknown || strings.Contains(errOut+out, "nothing moved") || !strings.Contains(errOut, "rerun with the SAME --request-id force-1") {
			t.Fatalf("%s: exit %d stdout %q stderr %s; want exit 3 with the same-id recovery and no refusal", attempt, code, out, errOut)
		}
	}
	if faults.Load() != 1 {
		t.Fatalf("fixture: the issuing write failed %d times; want once", faults.Load())
	}
	if line, _ := h.Publisher().VolumeAuthorityLine("vol-f"); line.Epoch != 1 {
		t.Fatalf("fixture: memory published the issue (epoch %d); this control needs it unpublished", line.Epoch)
	}
	if !strings.Contains(logs.String(), `MoveWorkspace outcome unknown volume="vol-f" request="force-1"`) || strings.Contains(logs.String(), "forced move issued") {
		t.Fatalf("master log must record an unknown outcome, not an issue:\n%s", logs.String())
	}

	stop()
	stopped = true
	h2, stop2 := startDeleteMaster(t, cfg)
	defer stop2()
	if line, _ := h2.Publisher().VolumeAuthorityLine("vol-f"); line.Epoch != 2 {
		t.Fatalf("fixture: the reloaded store does not hold the issued move (epoch %d)", line.Epoch)
	}
	code, out, errOut := runCLI(args(h2.Addr()))
	if code != wsExitOK || !strings.HasPrefix(out, "forced move issued: volume vol-f request force-1: r1@1 on node-a -> r1@2 on node-b") || !strings.Contains(out, "restored to the last published image") {
		t.Fatalf("rerun after restart: exit %d stdout %q stderr %s", code, out, errOut)
	}
}
