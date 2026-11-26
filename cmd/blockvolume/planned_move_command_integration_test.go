//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// stopBoundary holds an applied operation's reply, so killing the real CLI
// cannot be confused with cancelling the operation before it reached its owner.
type stopBoundary struct {
	mu      sync.Mutex
	phase   string
	hit     chan struct{}
	release chan struct{}
}

func (g *stopBoundary) arm(phase string) (<-chan struct{}, func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.phase = phase
	g.hit = make(chan struct{})
	g.release = make(chan struct{})
	release := g.release
	var once sync.Once
	return g.hit, func() { once.Do(func() { close(release) }) }
}
func (g *stopBoundary) observed(phase string) {
	// Negative rehearsal control: without the causal boundary the command
	// exits before the requested kill; the test must fail, never count it.
	if os.Getenv("PLANNED_MOVE_SKIP_BOUNDARY") == phase {
		return
	}
	g.mu.Lock()
	if g.phase != phase {
		g.mu.Unlock()
		return
	}
	hit, release := g.hit, g.release
	g.phase = ""
	g.mu.Unlock()
	close(hit)
	<-release
}

type plannedMoveProxy struct {
	control.UnimplementedWorkspaceServiceServer
	control.UnimplementedLifecycleServiceServer
	ws   control.WorkspaceServiceClient
	lc   control.LifecycleServiceClient
	gate *stopBoundary
}

func (p *plannedMoveProxy) QueryWorkspace(ctx context.Context, q *control.WorkspaceVolume) (*control.WorkspaceStatus, error) {
	st, e := p.ws.QueryWorkspace(ctx, q)
	if e == nil {
		p.gate.observed("precondition")
		if st.GetRevocationAck() != nil {
			p.gate.observed("ack")
		}
	}
	return st, e
}
func (p *plannedMoveProxy) GetVolume(ctx context.Context, q *control.GetLifecycleVolumeRequest) (*control.CreateVolumeResponse, error) {
	return p.lc.GetVolume(ctx, q)
}
func (p *plannedMoveProxy) RevokeWorkspace(ctx context.Context, q *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	v, e := p.ws.RevokeWorkspace(ctx, q)
	if e == nil {
		p.gate.observed("revoke")
	}
	return v, e
}
func (p *plannedMoveProxy) MoveWorkspace(ctx context.Context, q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
	v, e := p.ws.MoveWorkspace(ctx, q)
	if e == nil {
		p.gate.observed("move")
	}
	return v, e
}

func TestPlannedMoveCommandInterruptEveryStep(t *testing.T) {
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Skip("CHUNKSTORE_WEED not set")
	}
	for _, key := range []string{"SW_BLOCK_TLS_CERT", "SW_BLOCK_TLS_KEY", "SW_BLOCK_TLS_CA"} {
		t.Setenv(key, "")
	}
	env := startHandoffEnv(t, 2, "--lifecycle-product-loop-interval", "100ms")
	source := env.start(t, "planned-source", "s1")
	epoch := env.adopt(t, source, "s1")
	want := map[uint64][]byte{}
	for lba := uint64(0); lba < 4; lba++ {
		want[lba] = bytes.Repeat([]byte{byte(0x40 + lba)}, 4096)
		env.writeBlock(t, "s1", lba, want[lba])
	}
	// Starting the target is an operator precondition, not a driver step.
	target := env.start(t, "planned-target-waiting", "s2")
	conn, e := grpc.NewClient(env.h.masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	ws := control.NewWorkspaceServiceClient(conn)
	// Preparing endpoint ownership is an operator prerequisite, separate
	// from starting the target process. This legacy harness only registers
	// node endpoints; install both explicit bindings through the public RPC.
	for _, id := range []string{"s1", "s2"} {
		host := env.hosts[id]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := ws.RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{
			VolumeId: "v1", ServerId: id, DataAddr: host.data, CtrlAddr: host.ctrl,
		})
		cancel()
		if err != nil {
			t.Fatalf("prepare %s binding: %v", id, err)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		st, err := ws.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "v1"})
		cancel()
		bound := false
		if err == nil {
			for _, b := range st.GetBindings() {
				if b.GetServerId() == "s2" && !b.GetRetired() {
					bound = true
				}
			}
		}
		if bound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("prestarted target has no binding: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	gate := &stopBoundary{}
	lis, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	proxy := &plannedMoveProxy{ws: ws, lc: control.NewLifecycleServiceClient(conn), gate: gate}
	control.RegisterWorkspaceServiceServer(server, proxy)
	control.RegisterLifecycleServiceServer(server, proxy)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	httpProxy := func(addr, phase string) *httptest.Server {
		u, err := url.Parse("http://" + addr)
		if err != nil {
			t.Fatal(err)
		}
		p := httputil.NewSingleHostReverseProxy(u)
		p.ModifyResponse = func(r *http.Response) error {
			if r.StatusCode != http.StatusOK {
				return nil
			}
			if phase == "seal" && r.Request.URL.Path == "/chunk/detach" {
				gate.observed(phase)
			}
			if phase == "target" && r.Request.URL.Path == "/status" {
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					return err
				}
				_ = r.Body.Close()
				r.Body = io.NopCloser(bytes.NewReader(raw))
				var s struct {
					FrontendPrimaryReady bool
					Epoch                uint64
				}
				if json.Unmarshal(raw, &s) == nil && s.FrontendPrimaryReady && s.Epoch > epoch {
					gate.observed(phase)
				}
			}
			return nil
		}
		s := httptest.NewServer(p)
		t.Cleanup(s.Close)
		return s
	}
	sourceProxy := httpProxy(env.hosts["s1"].status, "seal")
	targetProxy := httpProxy(env.hosts["s2"].status, "target")
	args := []string{"ops", "workspace", "planned-move", "--master", lis.Addr().String(), "--volume", "v1", "--replica", "r1", "--epoch", fmt.Sprint(epoch), "--source-host", "s1", "--source-status", sourceProxy.URL, "--target-host", "s2", "--target-status", targetProxy.URL, "--request-id", "planned-interrupt-1", "--timeout", "120s"}
	// One journey, six kills, the same id throughout. Every boundary is causal:
	// no fixed sleep decides whether a stop, ACK, move or readiness happened.
	for _, phase := range []string{"precondition", "seal", "revoke", "ack", "move", "target"} {
		t.Run(phase, func(t *testing.T) {
			hit, release := gate.arm(phase)
			defer release()
			log, err := os.Create(filepath.Join(env.h.dir, "planned-command-"+phase+".log"))
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 130*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, env.h.cli, args...)
			cmd.Dir = env.h.dir
			cmd.Stdout = log
			cmd.Stderr = log
			cmd.WaitDelay = 5 * time.Second
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case <-hit:
				if err = cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case err = <-done:
					if err == nil {
						t.Fatal("command completed before the interrupt")
					}
				case <-time.After(10 * time.Second):
					t.Fatal("killed command did not exit")
				}
				release()
			case err = <-done:
				raw, _ := os.ReadFile(log.Name())
				t.Fatalf("command exited before %s boundary: %v\n%s", phase, err, raw)
			case <-ctx.Done():
				t.Fatalf("did not reach %s boundary: %v", phase, ctx.Err())
			}
		})
		if t.Failed() {
			return
		}
	}
	out, errout, code := env.h.run(t, args...)
	if code != 0 || !strings.Contains(out, "COMPLETE") {
		t.Fatalf("same-id final resume: exit%d\n%s\n%s", code, out, errout)
	}
	newEpoch := env.adopt(t, target, "s2")
	if newEpoch <= epoch {
		t.Fatalf("target epoch=%d, old=%d", newEpoch, epoch)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, e := ws.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "v1"})
	if e != nil {
		t.Fatal(e)
	}
	receipts := 0
	for _, m := range st.GetMoves() {
		if m.GetRequestId() == "planned-interrupt-1" {
			receipts++
		}
	}
	if receipts != 1 {
		t.Fatalf("same-id interruption produced %d move receipts", receipts)
	}
	for lba, data := range want {
		if got := env.readBlock(t, "s2", lba); !bytes.Equal(got, data) {
			t.Fatalf("ACKed block %d differs after planned move", lba)
		}
	}
	t.Logf("six interrupted steps -> COMPLETE, one move receipt, four ACKed blocks exact at epoch %d", newEpoch)
}
