package main

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// seedForcedVolume makes r1 on node-a the holder of vol-f at epoch 1 (lease
// renewed once) with a prepared target binding on node-b.
func seedForcedVolume(t *testing.T, h *master.Host) {
	t.Helper()
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ws := control.NewWorkspaceServiceClient(conn)
	for _, server := range []string{"node-a", "node-b"} {
		if _, err := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: server, DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "vol-f", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	for _, b := range []*control.WorkspaceBinding{
		{VolumeId: "vol-f", ServerId: "node-a", DataAddr: "127.0.0.1:19200", CtrlAddr: "127.0.0.1:19100"},
		{VolumeId: "vol-f", ServerId: "node-b", DataAddr: "127.0.0.1:29200", CtrlAddr: "127.0.0.1:29100"},
	} {
		if _, err := ws.RegisterWorkspaceBinding(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ws.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "vol-f", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{VolumeId: "vol-f", ReplicaId: "r1", DataAddr: "127.0.0.1:19200", CtrlAddr: "127.0.0.1:19100"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if line, ok := h.Publisher().VolumeAuthorityLine("vol-f"); ok && line.Epoch == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no authority line for vol-f")
		}
	}
	if _, err := ws.RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "vol-f", ReplicaId: "r1", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
}

// The CLI against a real master: before the horizon the move is refused
// (exit 1) with when it becomes admissible, and the dry run writes nothing;
// after it the move is issued with the readout of the stored range; a rerun
// with the same request id prints the same; lost-range prints that range;
// after a restart with the range file damaged a rerun exits 4 (issued,
// readout unproven) and lost-range exits 4. A confirmation at another epoch
// is refused by the master's state (exit 1); a malformed one never leaves
// the CLI (exit 2).
func TestWorkspaceForceMoveAgainstRealMaster(t *testing.T) {
	headTime := time.Date(2026, 11, 21, 1, 2, 3, 0, time.UTC)
	authDir := t.TempDir()
	cfg := master.Config{WorkspaceMaxClockSkew: 100 * time.Millisecond, Listen: "127.0.0.1:0", AuthorityStoreDir: authDir, LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
			return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, FilerMtimeNS: headTime.UnixNano()}, nil
		}}
	h, stop := startDeleteMaster(t, cfg)
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	seedForcedVolume(t, h)
	args := func(extra ...string) []string {
		return append([]string{"ops", "workspace", "force-move", "--master", h.Addr(), "--volume", "vol-f", "--target-host", "node-b", "--target-data", "127.0.0.1:29200",
			"--target-ctrl", "127.0.0.1:29100", "--request-id", "force-1"}, extra...)
	}

	if code, _, errOut := runCLI(args("--confirm-forced", "vol-f@1x")); code != wsExitRefusedLocally {
		t.Fatalf("malformed confirmation: exit %d %s", code, errOut)
	}
	if code, _, errOut := runCLI(args("--confirm-forced", "vol-f@2")); code != wsExitRefused || !strings.Contains(errOut, "no holder of volume") {
		t.Fatalf("confirmation at another epoch: exit %d %s", code, errOut)
	}
	code, _, errOut := runCLI(args("--confirm-forced", "vol-f@1"))
	if code != wsExitRefused || !strings.Contains(errOut, "has not elapsed") || !strings.Contains(errOut, "admissible at ") {
		t.Fatalf("before the horizon: exit %d %s", code, errOut)
	}
	code, out, _ := runCLI(args("--confirm-forced", "vol-f@1", "--dry-run"))
	if code != wsExitOK || !strings.Contains(out, "forced move not admitted") || !strings.Contains(out, "admissible at ") {
		t.Fatalf("dry run before the horizon: exit %d\n%s", code, out)
	}
	if st := h.Publisher().WorkspaceState("vol-f"); len(st.Moves) != 0 {
		t.Fatalf("refusal or dry run wrote a journal record: %+v", st.Moves)
	}

	st := h.Publisher().WorkspaceState("vol-f")
	skew := time.Duration(st.Lease.MaxClockSkewNS)
	if skew < cfg.WorkspaceMaxClockSkew {
		skew = cfg.WorkspaceMaxClockSkew
	}
	time.Sleep(time.Until(time.Unix(0, st.Lease.ExpiresAtNS).Add(skew)) + 100*time.Millisecond)

	code, out, errOut = runCLI(args("--confirm-forced", "vol-f@1"))
	if code != wsExitOK || !strings.HasPrefix(out, "forced move issued: volume vol-f request force-1: r1@1 on node-a -> r1@2 on node-b (kept HEAD gen 9, remote LSN 42)\n") ||
		!strings.Contains(out, "volume vol-f restored to the last published image at 2026-11-21T01:02:03Z, filer clock (gen 9, LSN 42): writes with LSN <= 42 are preserved; writes acknowledged by r1@1 after that and before ") {
		t.Fatalf("forced move: exit %d stderr %s\n%s", code, errOut, out)
	}
	code2, out2, _ := runCLI(args("--confirm-forced", "vol-f@1"))
	if code2 != wsExitOK || out2 != out {
		t.Fatalf("rerun with the same request id: exit %d\n%s\nwant\n%s", code2, out2, out)
	}
	readout := strings.Split(out, "\n")[1]
	code, lr, _ := runCLI([]string{"ops", "workspace", "lost-range", "--master", h.Addr(), "--volume", "vol-f"})
	if code != wsExitOK || !strings.HasPrefix(lr, "range 1 of 1: request force-1, r1@1 -> epoch 2, lost after LSN 42") || !strings.Contains(lr, "  "+readout+"\n") {
		t.Fatalf("lost-range: exit %d\n%s\nwant readout %s", code, lr, readout)
	}

	stop()
	stopped = true
	var files []string
	_ = filepath.WalkDir(authDir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".lostrange.json") {
			files = append(files, p)
		}
		return err
	})
	if len(files) != 1 {
		t.Fatalf("lost range files %v", files)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files[0], []byte(strings.Replace(string(raw), `"LostAfterLSN":42`, `"LostAfterLSN":41`, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	h2, stop2 := startDeleteMaster(t, cfg)
	defer stop2()
	args2 := func(extra ...string) []string {
		a := args(extra...)
		a[4] = h2.Addr()
		return a
	}
	code, out, errOut = runCLI(args2("--confirm-forced", "vol-f@1"))
	if code != wsExitReadoutUnproven || !strings.HasPrefix(out, "forced move issued: volume vol-f request force-1") || strings.Contains(out, "restored to") || !strings.Contains(errOut, "not proven") {
		t.Fatalf("rerun with a damaged range: exit %d stdout %q stderr %s", code, out, errOut)
	}
	if code, _, errOut = runCLI([]string{"ops", "workspace", "lost-range", "--master", h2.Addr(), "--volume", "vol-f"}); code != wsExitReadoutUnproven {
		t.Fatalf("lost-range with a damaged range: exit %d %s", code, errOut)
	}
}
