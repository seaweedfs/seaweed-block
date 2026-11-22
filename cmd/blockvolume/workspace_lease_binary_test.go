//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
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

func TestBinaryWorkspaceLeaseOptInOwnsCLEANShutdown(t *testing.T) {
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Skip("CHUNKSTORE_WEED not set")
	}
	exe := filepath.Join(t.TempDir(), "blockvolume")
	build := exec.Command("go", "build", "-race", "-p", "4", "-o", exe, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	cluster := startWorkspaceWeed(t)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("workspace-rpc-%v", enabled), func(t *testing.T) {
			base := fmt.Sprintf("/workspace-lease-%d", time.Now().UnixNano())
			reader, err := master.NewWorkspaceHEADReader("http://" + cluster.filerHTTP + base)
			if err != nil {
				t.Fatal(err)
			}
			h, err := master.New(master.Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0", WorkspaceRPC: enabled, WorkspaceHeadReader: reader})
			if err != nil {
				t.Fatal(err)
			}
			h.Start()
			t.Cleanup(func() {
				ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
				defer c()
				_ = h.Close(ctx)
			})
			dataAddr, ctrlAddr, statusAddr, nvmeAddr := leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t), leaseBinaryAddress(t)
			if _, err = h.Lifecycle().Volumes.CreateVolume(lifecycle.VolumeSpec{VolumeID: "vol-a", SizeBytes: 64 * 4096, ReplicationFactor: 1}); err != nil {
				t.Fatal(err)
			}
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
			cmd := exec.Command(exe, "--master", h.Addr(), "--server-id", "node-a", "--volume-id", "vol-a", "--replica-id", "r1", "--data-addr", dataAddr, "--ctrl-addr", ctrlAddr, "--t1-readiness", "--status-addr", statusAddr, "--durable-root", t.TempDir(), "--volume-lock-dir-test-only", t.TempDir(), "--durable-impl", "walstore-chunk", "--durable-blocks", "64", "--chunk-filer", cluster.filerGRPC, "--chunk-path", base, "--chunk-control-endpoint", "--workspace-lease", "--nvme-listen", nvmeAddr, "--nvme-subsysnqn", "nqn.2026-11.test:workspace-lease")
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
			remote := cluster.remote(t, base+"/vol-a")
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
				head, e := remote.GetHead(context.Background())
				if e != nil || head.Gen != 0 {
					t.Fatal("unleased binary changed HEAD", head, e)
				}
			}
		})
	}
}
