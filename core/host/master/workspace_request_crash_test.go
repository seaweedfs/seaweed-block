package master

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func pendingPreparationConfig(root string) Config {
	return Config{Listen: "127.0.0.1:0", AuthorityStoreDir: filepath.Join(root, "authority"), LifecycleStoreDir: filepath.Join(root, "lifecycle"), WorkspaceRPC: true, WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
		return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r2", RemoteLSN: 42, Detached: true}, nil
	}}
}
func pendingTargetBinding() *control.WorkspaceBinding {
	return &control.WorkspaceBinding{RequestId: "prepare-target", VolumeId: "vol-a", ServerId: "node-b", DataAddr: "127.0.0.1:19201", CtrlAddr: "127.0.0.1:19101"}
}

// This subprocess makes the real RPC persist its intent and then blocks at the
// handler's existing mutation lock. The parent kills it at that observed durable
// boundary. No fabricated receipt and no production fault hook are used.
func TestWorkspacePendingPreparationCrashHelper(t *testing.T) {
	root := os.Getenv("WORKSPACE_PENDING_CRASH_ROOT")
	if root == "" {
		t.Skip("subprocess helper")
	}
	h, e := New(pendingPreparationConfig(root))
	if e != nil {
		t.Fatal(e)
	}
	h.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, e = h.Lifecycle().Volumes.CreateVolume(lifecycle.VolumeSpec{VolumeID: "vol-a", SizeBytes: 1 << 20, ReplicationFactor: 1}); e != nil {
		t.Fatal(e)
	}
	if _, e = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "node-b", DataAddr: "127.0.0.1:9201", CtrlAddr: "127.0.0.1:9101"}); e != nil {
		t.Fatal(e)
	}
	seedVerifiedExistingReplicaPlacement(t, h)
	if _, e = h.RunLifecycleProductTick(); e != nil {
		t.Fatal(e)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-a")
	if line.Epoch != 1 || line.ReplicaID != "r2" {
		t.Fatal(line)
	}
	c, e := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	w := control.NewWorkspaceServiceClient(c)
	if _, e = w.RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "vol-a", ReplicaId: "r2", Epoch: 1}); e != nil {
		t.Fatal(e)
	}
	h.workspaceMu.Lock() // force the real request to stop after durable intent
	done := make(chan error, 1)
	go func() { _, err := w.RegisterWorkspaceBinding(ctx, pendingTargetBinding()); done <- err }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(filepath.Join(root, "authority", "workspace-requests.json"))
		var records map[string]workspaceReceipt
		if err == nil && json.Unmarshal(data, &records) == nil {
			for _, r := range records {
				if r.Pending && r.Volume == "vol-a" && r.Method == control.WorkspaceService_RegisterWorkspaceBinding_FullMethodName {
					// persistWorkspaceRequests holds its mutex through directory fsync;
					// wait for it before announcing the crash boundary.
					h.workspaceRequests.mu.Lock()
					h.workspaceRequests.mu.Unlock()
					if e = os.WriteFile(filepath.Join(root, "ready"), []byte("pending target bind durable"), 0600); e != nil {
						t.Fatal(e)
					}
					<-ctx.Done()
					t.Fatal("parent did not crash child at pending boundary")
				}
			}
		}
		select {
		case e := <-done:
			t.Fatalf("bind escaped held mutation lock: %v", e)
		case <-time.After(5 * time.Millisecond):
		}
	}
	t.Fatal("no durable pending bind observed")
}
func TestWorkspacePendingBindCrashDoesNotBlockHolderLease(t *testing.T) {
	root := t.TempDir()
	logPath := filepath.Join(root, "child.log")
	log, e := os.Create(logPath)
	if e != nil {
		t.Fatal(e)
	}
	defer log.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestWorkspacePendingPreparationCrashHelper$", "-test.v")
	child.Env = append(os.Environ(), "WORKSPACE_PENDING_CRASH_ROOT="+root)
	child.Stdout = log
	child.Stderr = log
	if e = child.Start(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		_ = child.Process.Kill()
		select {
		case <-done:
			stopped = true
		case <-time.After(5 * time.Second):
			t.Fatal("crashed master did not reap")
		}
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, e = os.Stat(filepath.Join(root, "ready")); e == nil {
			break
		}
		select {
		case e = <-done:
			stopped = true
			b, _ := os.ReadFile(logPath)
			t.Fatalf("child exited before crash: %v %s", e, b)
		case <-time.After(5 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(logPath)
			t.Fatalf("pending boundary timeout: %s", b)
		}
	}
	stop() // actual crash after the durable pending target-bind record
	h, e := New(pendingPreparationConfig(root))
	if e != nil {
		t.Fatal(e)
	}
	h.Start()
	defer closeTestMaster(t, h)
	c, e := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	w := control.NewWorkspaceServiceClient(c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := &control.WorkspaceIdentity{VolumeId: "vol-a", ReplicaId: "r2", Epoch: 1}
	lease, e := w.RenewWorkspaceLease(ctx, id)
	if e != nil || lease.Epoch != 1 || lease.Revoking || lease.ValidForNs <= 0 {
		t.Fatalf("pending endpoint preparation blocked unchanged holder renewal after crash: lease=%v err=%v", lease, e)
	}
	if _, e = w.RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "vol-a", ReplicaId: "r2", Epoch: 2}); status.Code(e) != codes.FailedPrecondition {
		t.Fatalf("epoch guard bypassed: %v", e)
	}
	revoked, e := w.RevokeWorkspace(ctx, id)
	if e != nil || !revoked.Revoking {
		t.Fatalf("pending preparation blocked revocation: %v %v", revoked, e)
	}
	if _, e = w.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); e != nil {
		t.Fatalf("pending preparation blocked drained ACK: %v", e)
	}
	first, e := w.RegisterWorkspaceBinding(ctx, pendingTargetBinding())
	if e != nil {
		t.Fatal("resume crashed preparation", e)
	}
	again, e := w.RegisterWorkspaceBinding(ctx, pendingTargetBinding())
	if e != nil || !proto.Equal(first, again) {
		t.Fatalf("receipt retry not idempotent: %v %v", again, e)
	}
	if first.DataAddr != "127.0.0.1:19201" || first.CtrlAddr != "127.0.0.1:19101" {
		t.Fatal(first)
	}
	changed := proto.Clone(pendingTargetBinding()).(*control.WorkspaceBinding)
	changed.CtrlAddr = "127.0.0.1:19102"
	if _, e = w.RegisterWorkspaceBinding(ctx, changed); status.Code(e) != codes.AlreadyExists {
		t.Fatalf("changed receipt body accepted: %v", e)
	}
	state, e := w.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "vol-a"})
	if e != nil || state.GetRevocationAck().GetHeadGen() != 9 || len(state.Moves) != 0 {
		t.Fatal(fmt.Sprint("lease maintenance changed grant state: ", state, e))
	}
}
