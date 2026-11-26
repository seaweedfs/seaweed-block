//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	master "github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func leaseBinaryAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}
func leaseBinaryPoll(t *testing.T, what string, fn func() bool) {
	t.Helper()
	end := time.Now().Add(30 * time.Second)
	for !fn() {
		if time.Now().After(end) {
			t.Fatal(what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
func leaseBinaryPost(addr, op string) (int, []byte, error) {
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Post("http://"+addr+"/chunk/"+op, "application/json", bytes.NewBufferString(`{"volumeID":"vol-a","timeoutMs":3000}`))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 65536))
	return resp.StatusCode, b, err
}

// blockvolumeUnderTest is the blockvolume a test starts: a gate run's
// prebuilt, hashed binary (SWBLOCK_TEST_BINS), or a race-enabled build for a
// local run.
func blockvolumeUnderTest(t *testing.T) string {
	t.Helper()
	if d := os.Getenv("SWBLOCK_TEST_BINS"); d != "" {
		exe := filepath.Join(d, "blockvolume")
		if _, err := os.Stat(exe); err != nil {
			t.Fatalf("SWBLOCK_TEST_BINS has no blockvolume: %v", err)
		}
		return exe
	}
	exe := filepath.Join(t.TempDir(), "blockvolume")
	if out, err := exec.Command("go", "build", "-race", "-p", "4", "-o", exe, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	return exe
}

// runProductLoop runs h's lifecycle product loop for the rest of the test,
// as blockmaster runs it (--lifecycle-product-loop-interval): a line is
// issued for the process at its endpoints, and a line minted before a
// process reported itself (a hand-published initial line, a move's target)
// is re-issued once it does. SWBLOCK_TEST_NO_PRODUCT_LOOP=1 leaves it off:
// the holder then refuses a line that names no process (the control).
func runProductLoop(t *testing.T, h *master.Host) {
	t.Helper()
	if os.Getenv("SWBLOCK_TEST_NO_PRODUCT_LOOP") == "1" {
		return
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_, _ = h.RunLifecycleProductTick()
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
}

func TestBinaryWorkspaceLeaseOptInOwnsCLEANShutdown(t *testing.T) {
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Skip("CHUNKSTORE_WEED not set")
	}
	exe := blockvolumeUnderTest(t)
	cluster := startWorkspaceWeed(t)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("workspace-rpc-%v", enabled), func(t *testing.T) {
			base := fmt.Sprintf("/workspace-lease-%d", time.Now().UnixNano())
			cl := cluster.client(t, base)
			headCat, err := chunkstore.NewBCatalog(cl, cl, base)
			if err != nil {
				t.Fatal(err)
			}
			var h *master.Host
			reader, err := master.NewLayoutWorkspaceHEADReader(headCat, func(ctx context.Context, name string) (chunkstore.VolumeRef, error) {
				return h.WorkspaceLayoutVolume(ctx, name)
			})
			if err != nil {
				t.Fatal(err)
			}
			h, err = master.New(master.Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0", WorkspaceRPC: enabled, WorkspaceHeadReader: reader,
				LayoutCreateVolume: layoutMasterWriter(t, cluster, base)})
			if err != nil {
				t.Fatal(err)
			}
			h.Start()
			t.Cleanup(func() {
				ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
				defer c()
				_ = h.Close(ctx)
			})
			runProductLoop(t, h)
			dataAddr, ctrlAddr, statusAddr, nvmeAddr := leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t)
			// The master's create writes VOLUME and the unclaimed HEAD, then
			// marks the record store-ready; only a store-ready volume gets a placement.
			layoutID := createStoreReadyVolume(t, h, "vol-a", 64*4096)
			if _, err = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "node-a", DataAddr: dataAddr, CtrlAddr: ctrlAddr}); err != nil {
				t.Fatal(err)
			}
			if _, err = h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "vol-a", DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{VolumeID: "vol-a", ServerID: "node-a", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
				t.Fatal(err)
			}
			if err = h.ObservationHost().Ingest(authority.Observation{ServerID: "node-a", ObservedAt: time.Now().UTC(), Slots: []authority.SlotFact{{VolumeID: "vol-a", ReplicaID: "r1", DataAddr: dataAddr, CtrlAddr: ctrlAddr}}}); err != nil {
				t.Fatal(err)
			}
			if _, err = h.RunLifecycleProductTick(); err != nil {
				t.Fatal(err)
			}
			leaseBinaryPoll(t, "no initial authority", func() bool { line, ok := h.Publisher().VolumeAuthorityLine("vol-a"); return ok && line.Epoch > 0 })
			line, _ := h.Publisher().VolumeAuthorityLine("vol-a")
			logPath := filepath.Join(t.TempDir(), "volume.log")
			log, err := os.Create(logPath)
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(exe, "--master", h.Addr(), "--server-id", "node-a", "--volume-id", "vol-a", "--replica-id", "r1", "--data-addr", dataAddr, "--ctrl-addr", ctrlAddr, "--t1-readiness", "--status-addr", statusAddr, "--durable-root", t.TempDir(), "--layout-id", layoutID.String(), "--volume-lock-dir-test-only", t.TempDir(), "--durable-impl", "walstore-chunk", "--durable-blocks", "64", "--chunk-filer", cluster.filerGRPC, "--chunk-path", base, "--chunk-control-endpoint", "--workspace-lease", "--nvme-listen", nvmeAddr, "--nvme-subsysnqn", "nqn.2026-11.test:workspace-lease")
			stripSystemdForTestLockDir(cmd)
			cmd.Stdout, cmd.Stderr = log, log
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			exited := false
			t.Cleanup(func() {
				if !exited {
					_ = cmd.Process.Kill()
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("binary did not exit after kill")
					}
				}
				log.Close()
				if t.Failed() {
					b, _ := os.ReadFile(logPath)
					if len(b) > 14000 {
						b = b[len(b)-14000:]
					}
					t.Logf("volume log: %s", b)
				}
			})
			client := &http.Client{Timeout: time.Second}
			projection := func() (frontend.Projection, bool) {
				resp, e := client.Get("http://" + statusAddr + "/status?volume=vol-a")
				if e != nil {
					return frontend.Projection{}, false
				}
				defer resp.Body.Close()
				var p frontend.Projection
				e = json.NewDecoder(resp.Body).Decode(&p)
				return p, e == nil
			}
			remote := cluster.remote(t, base, chunkstore.VolumeRef{Name: "vol-a", ID: layoutID})
			if !enabled {
				leaseBinaryPoll(t, "unleased host did not adopt assignment", func() bool { p, ok := projection(); return ok && p.Epoch == line.Epoch })
				// Exercise an actual cut request, not just the status presentation.
				code, b, e := leaseBinaryPost(statusAddr, "cut")
				if e != nil || code >= 200 && code < 300 {
					t.Fatalf("unleased cut: %d %s %v", code, b, e)
				}
				if p, ok := projection(); !ok || p.Healthy {
					t.Fatal("disabled WorkspaceRPC served", p, ok)
				}
				if state := h.Publisher().WorkspaceState("vol-a"); state != nil {
					t.Fatal("disabled RPC enrolled a lease", state)
				}
				_ = cmd.Process.Signal(syscall.SIGTERM)
			} else {
				leaseBinaryPoll(t, "valid lease did not admit binary", func() bool { p, ok := projection(); return ok && p.Healthy })
				// Actual provider/filer cut and canonical Detach happen before Revoke.
				leaseBinaryPoll(t, "provider was not operational", func() bool { code, _, e := leaseBinaryPost(statusAddr, "cut"); return e == nil && code == 200 })
				code, b, e := leaseBinaryPost(statusAddr, "detach")
				if e != nil || code != 200 {
					t.Fatalf("detach: %d %s %v", code, b, e)
				}
				detached, e := remote.GetHead(context.Background())
				if e != nil || detached.State != "detached" || detached.DetachedBy == nil {
					t.Fatal(detached, e)
				}
				conn, e := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if e != nil {
					t.Fatal(e)
				}
				defer conn.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, e = control.NewWorkspaceServiceClient(conn).RevokeWorkspace(ctx, &control.WorkspaceIdentity{VolumeId: "vol-a", ReplicaId: "r1", Epoch: line.Epoch})
				cancel()
				if e != nil {
					t.Fatal(e)
				}
				leaseBinaryPoll(t, "binary did not ACK its own drained shutdown", func() bool { s := h.Publisher().WorkspaceState("vol-a"); return s != nil && s.Ack != nil })
				state := h.Publisher().WorkspaceState("vol-a")
				if state.Ack.HeadGen != detached.Gen {
					t.Fatal("ACK differs from stored detached HEAD", state, detached)
				}
				current, e := remote.GetHead(context.Background())
				if e != nil || current.Gen != detached.Gen {
					t.Fatal("revoke performed a forbidden extra cut", current, e)
				}
			}
			select {
			case err := <-done:
				exited = true
				if err != nil {
					t.Fatal("binary exit", err)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("binary did not finish shutdown")
			}
			if c, e := net.DialTimeout("tcp", nvmeAddr, 200*time.Millisecond); e == nil {
				c.Close()
				t.Fatal("NVMe frontend still accepts after shutdown")
			}
			if !enabled {
				// No claim was ever made: HEAD is absent, or the unclaimed
				// HEAD the create wrote (no holder, epoch 0).
				cl := cluster.client(t, base)
				cat, e := chunkstore.NewBCatalog(cl, cl, base)
				if e != nil {
					t.Fatal(e)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				head, found, e := cat.ReadHead(ctx, chunkstore.VolumeRef{Name: "vol-a", ID: layoutID})
				cancel()
				if e != nil || found && (head.Holder != "" || head.Epoch != 0 || head.ClaimID != "") {
					t.Fatal("unleased binary changed HEAD", head, found, e)
				}
			}
		})
	}
}
