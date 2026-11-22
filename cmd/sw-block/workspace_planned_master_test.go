package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Existing M2 Granted -> Revoking -> Acked -> Moved (clean),
// INV-WS-CLEAN-MOVE-001: real RPC admission still requires the host ACK.
// HEAD reading is supplied by the existing master seam; no lab is involved.
func TestWorkspacePlannedHandoffAgainstRealMaster(t *testing.T) {
	h, stop := startDeleteMaster(t, master.Config{
		Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
			return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, Detached: true}, nil
		},
	})
	defer stop()
	seedForcedVolume(t, h)
	show := []string{"ops", "workspace", "show", "--master", h.Addr(), "--volume", "vol-f"}
	if code, out, errOut := runCLI(show); code != 0 || !strings.Contains(out, "holder: r1@1") || !strings.Contains(out, "recorded ACK: none") {
		t.Fatalf("initial show: exit=%d out=%q err=%q", code, out, errOut)
	}
	revoke := []string{"ops", "workspace", "revoke", "--master", h.Addr(), "--volume", "vol-f", "--replica", "r1", "--epoch", "1", "--sealed-gen", "9"}
	if code, out, errOut := runCLI(revoke); code != 0 {
		t.Fatalf("revoke: exit=%d out=%q err=%q", code, out, errOut)
	}
	move := []string{"ops", "workspace", "move", "--clean", "--master", h.Addr(), "--volume", "vol-f", "--replica", "r1", "--epoch", "1", "--request-id", "clean-actual", "--target-host", "node-b", "--target-data", "127.0.0.1:29200", "--target-ctrl", "127.0.0.1:29100"}
	if code, _, errOut := runCLI(move); code != wsExitRefused {
		t.Fatalf("move without host ACK: exit=%d err=%q", code, errOut)
	}
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// This is the separate host event, never synthesized by the CLI.
	_, err = control.NewWorkspaceServiceClient(conn).AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "vol-f", ReplicaId: "r1", Epoch: 1}, HeadGen: 9})
	if err != nil {
		t.Fatal(err)
	}
	if code, out, errOut := runCLI(show); code != 0 || !strings.Contains(out, "recorded ACK: r1@1 HEAD generation 9") {
		t.Fatalf("ACK show: exit=%d out=%q err=%q", code, out, errOut)
	}
	var first string
	for i := 0; i < 2; i++ {
		code, out, errOut := runCLI(move)
		if code != 0 || !strings.Contains(out, "clean move issued") || !strings.Contains(out, "r1@1 -> r1@2 on node-b") {
			t.Fatalf("clean move: exit=%d out=%q err=%q", code, out, errOut)
		}
		if i == 0 {
			first = out
		} else if out != first {
			t.Fatalf("same-id replay changed grant: %q versus %q", out, first)
		}
	}
}
