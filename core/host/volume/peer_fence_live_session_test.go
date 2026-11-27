package volume

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// replyDropProxy sits between the primary and a replica. In drop mode a
// new connection forwards the primary's frames to the replica (which
// admits and applies them) but discards the replica's replies: the
// primary's exchange times out although the replica acted on every frame.
type replyDropProxy struct {
	ln       net.Listener
	upstream string
	drop     atomic.Bool
	mu       sync.Mutex
	conns    []net.Conn
	wg       sync.WaitGroup
}

func startReplyDropProxy(t *testing.T, addr, upstream string) *replyDropProxy {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	p := &replyDropProxy{ln: ln, upstream: upstream}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u, err := net.Dial("tcp", upstream)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, u)
			p.mu.Unlock()
			dropping := p.drop.Load()
			p.wg.Add(2)
			go func() { defer p.wg.Done(); _, _ = io.Copy(u, c); _ = u.Close() }()
			go func() {
				defer p.wg.Done()
				if dropping {
					// The replica acts on every frame; its replies are
					// discarded, so the sender times out waiting for them.
					_, _ = io.Copy(io.Discard, u)
				} else {
					_, _ = io.Copy(c, u)
				}
				_ = c.Close()
				_ = u.Close()
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		p.mu.Lock()
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
	return p
}

// fenceRig is a primary volume with one degraded peer r2 at epoch 2 and a
// real replica listener, either at the primary's end or `behind` entries
// short of it, optionally reached through a replyDropProxy.
type fenceRig struct {
	peer             *replication.ReplicaPeer
	exec             *PeerCommandExecutor
	v                *replication.ReplicationVolume
	primary, replica *storage.BlockStore
	proxy            *replyDropProxy
}

func newFenceRig(t *testing.T, behind int, proxied bool) *fenceRig {
	t.Helper()
	const blockSize, n = 4096, 4
	r := &fenceRig{primary: storage.NewBlockStore(64, blockSize), replica: storage.NewBlockStore(64, blockSize)}
	for i := 0; i < n; i++ {
		data := bytes.Repeat([]byte{byte(i + 1)}, blockSize)
		lsn, err := r.primary.Write(uint32(i), data)
		if err != nil {
			t.Fatal(err)
		}
		if i < n-behind {
			if err := r.replica.ApplyEntry(uint32(i), data, lsn); err != nil {
				t.Fatal(err)
			}
		}
	}
	own, _ := transport.LoadEpochTable("", "vol1")
	_ = own.Replace([]transport.EpochRow{{Epoch: 2, FirstLSN: 1}})
	theirs, _ := transport.LoadEpochTable("", "vol1")
	_ = theirs.Replace([]transport.EpochRow{{Epoch: 2, FirstLSN: 1}})

	// Nothing listens yet: the seed barrier fails and the peer is degraded
	// (the rejoining replica is not up).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	r.v = replication.NewReplicationVolume("vol1", r.primary)
	r.v.SetDurabilityMode(replication.DurabilitySyncQuorum)
	r.v.SetEpochTable(own)
	t.Cleanup(func() { _ = r.v.Close() })
	if err := r.v.ConfigurePeerLifecycleHook(func(p *replication.ReplicaPeer) { r.peer = p }, func(string) {}); err != nil {
		t.Fatal(err)
	}
	if err := r.v.UpdateReplicaSet(1, []replication.ReplicaTarget{{ReplicaID: "r2", DataAddr: addr, ControlAddr: addr, Epoch: 2, EndpointVersion: 1}}); err != nil {
		t.Fatal(err)
	}
	if r.peer == nil || r.peer.State() != replication.ReplicaDegraded {
		t.Fatalf("peer %v; want degraded after an unanswered seed barrier", r.peer)
	}
	listenAt := addr
	if proxied {
		listenAt = "127.0.0.1:0"
	}
	listener, err := transport.NewReplicaListener(listenAt, r.replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.SetEpochTable(theirs)
	listener.Serve()
	if proxied {
		r.proxy = startReplyDropProxy(t, addr, listener.Addr())
	}
	// The primary's connection closes first, or Stop waits for it.
	t.Cleanup(func() { _ = r.v.Close(); listener.Stop() })
	r.exec = NewPeerCommandExecutor(r.peer)
	r.exec.SetOnSessionStart(func(adapter.SessionStartResult) {})
	return r
}

// fencePeerRig is the caught-up rig, reached directly.
func fencePeerRig(t *testing.T) (*replication.ReplicaPeer, *PeerCommandExecutor) {
	t.Helper()
	r := newFenceRig(t, 0, false)
	return r.peer, r.exec
}

// fence runs a fence through the executor's registered callback and
// returns its result.
func (r *fenceRig) fence(t *testing.T) adapter.FenceResult {
	t.Helper()
	done := make(chan adapter.FenceResult, 1)
	r.exec.SetOnFenceComplete(func(res adapter.FenceResult) { done <- res })
	if err := r.exec.Fence("r2", r.exec.SessionIDs().Next(), 2, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-done:
		return res
	case <-time.After(10 * time.Second):
		t.Fatal("the fence did not complete")
		return adapter.FenceResult{}
	}
}

// catchUp runs a catch-up from the replica's end to the primary's through
// the executor's registered close callback and returns its result.
func (r *fenceRig) catchUp(t *testing.T) adapter.SessionCloseResult {
	t.Helper()
	closed := make(chan adapter.SessionCloseResult, 1)
	r.exec.SetOnSessionClose(func(res adapter.SessionCloseResult) { closed <- res })
	end, _ := r.replica.Sync()
	_, _, head := r.primary.Boundaries()
	// The catch-up states the LSN it starts after: the replica's durable end.
	if err := r.exec.StartCatchUp("r2", r.exec.SessionIDs().Next(), 2, 1, end, head); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-closed:
		return res
	case <-time.After(15 * time.Second):
		t.Fatal("the catch-up never closed")
		return adapter.SessionCloseResult{}
	}
}

// requireLiveTraffic writes one more block on the primary and requires it
// to be shipped and acknowledged on the live session (the replica's end
// reaches its LSN).
func (r *fenceRig) requireLiveTraffic(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data := bytes.Repeat([]byte{0x7C}, 4096)
	lsn, err := r.primary.Write(9, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.v.Observe(ctx, 9, lsn, data); err != nil {
		t.Fatalf("live ship: %v", err)
	}
	if err := r.v.Sync(ctx, lsn); err != nil {
		t.Fatalf("live barrier: %v", err)
	}
	// The live session's frames were admitted and acknowledged: the
	// replica's durable end reached the write.
	if R, _, _ := r.replica.Boundaries(); R < lsn {
		t.Fatalf("the live write (LSN %d) was not acknowledged on the replica (end %d)", lsn, R)
	}

}

// A replica that is already at the primary's end (R = H) is fenced at the
// peer's epoch with a session the fence mints (as the adapter's
// FenceAtEpoch does, from the peer's one session order), above the live
// session the peer drew at its creation. The replica then holds the
// fence's lineage. The engine's healthy verdict sends its barrier on the
// live session: it must be acknowledged and the peer healed, so the live
// session must be re-drawn above the fence's once the fence completes.
// Without that the replica refuses the barrier as stale, forever.
func TestAFencedCaughtUpPeerIsHealedOnItsLiveSession(t *testing.T) {
	for _, fence := range []bool{true, false} {
		name := map[bool]string{true: "fenced first", false: "control: no fence"}[fence]
		t.Run(name, func(t *testing.T) {
			r := newFenceRig(t, 0, false)
			if fence {
				if res := r.fence(t); !res.Success {
					t.Fatalf("fence failed: %s", res.FailReason)
				}
			}
			r.exec.PublishHealthy("r2")
			if s := r.peer.State(); s != replication.ReplicaHealthy {
				t.Fatalf("peer %s after the healthy verdict; want healthy (its barrier on the live session must be acknowledged)", s)
			}
			r.requireLiveTraffic(t)
		})
	}
}

// The order of the re-draw: the live session moves above the fence's only
// once the fence's exchange is over. Moved before (here: the fence's
// session drawn, the live one re-drawn above it, then the fence sent), the
// replica holds the higher live lineage and refuses the fence.
func TestTheLiveSessionMovesOnlyAfterTheFenceIsAcknowledged(t *testing.T) {
	peer, exec := fencePeerRig(t)
	done := make(chan adapter.FenceResult, 1)
	exec.SetOnFenceComplete(func(r adapter.FenceResult) { done <- r })
	sid := exec.SessionIDs().Next()
	if err := peer.RefreshLiveShipSessionAfter(sid, 0, "re-drawn too early"); err != nil {
		t.Fatal(err)
	}
	exec.PublishHealthy("r2") // the replica now holds the live lineage above sid
	if s := peer.State(); s != replication.ReplicaHealthy {
		t.Fatalf("fixture: peer %s; want healthy on the re-drawn live session", s)
	}
	if err := exec.Fence("r2", sid, 2, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.Success {
			t.Fatal("a fence below the replica's held live lineage was acknowledged; want it refused (which is why the re-draw waits for the fence's exchange)")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the fence did not complete")
	}
}

// The lost acknowledgement, through the executor's registered callbacks:
// the replica admits a session (it holds that lineage from then on) and
// acts on it, but its reply never reaches the primary, which sees the
// exchange fail. The failure is forwarded to the engine's callback, and
// the live session is re-drawn above the session all the same: the
// healthy verdict that follows is acknowledged and live traffic lands.
// Cases: a fence of a caught-up replica, and a catch-up of a behind one
// whose close fails.
func TestAnAdmittedSessionWhoseReplyIsLostStillMovesTheLiveSession(t *testing.T) {
	t.Run("fence", func(t *testing.T) {
		r := newFenceRig(t, 0, true)
		r.proxy.drop.Store(true)
		if res := r.fence(t); res.Success {
			t.Fatal("fixture: the fence succeeded; its reply was to be lost")
		}
		r.proxy.drop.Store(false)
		r.exec.PublishHealthy("r2")
		if s := r.peer.State(); s != replication.ReplicaHealthy {
			t.Fatalf("peer %s after the healthy verdict; want healthy: the live session must be above the admitted fence", s)
		}
		r.requireLiveTraffic(t)
	})
	t.Run("recovery session", func(t *testing.T) {
		r := newFenceRig(t, 2, true)
		r.proxy.drop.Store(true)
		if res := r.catchUp(t); res.Success {
			t.Fatal("fixture: the catch-up closed successfully; its replies were to be lost")
		}
		r.proxy.drop.Store(false)
		r.exec.PublishHealthy("r2")
		if s := r.peer.State(); s != replication.ReplicaHealthy {
			t.Fatalf("peer %s after the healthy verdict; want healthy: the live session must be above the admitted recovery session", s)
		}
		r.requireLiveTraffic(t)
	})
}

// The R < H control: a peer behind the primary is not healed by the
// engine's healthy verdict while it lags; a catch-up through the
// registered close callback brings it to the primary's end (every block
// equal, nothing skipped or rewound), heals it, and live traffic then
// lands on the re-drawn live session.
func TestABehindPeerHealsOnlyThroughItsRecovery(t *testing.T) {
	r := newFenceRig(t, 2, false)
	r.exec.PublishHealthy("r2")
	if s := r.peer.State(); s == replication.ReplicaHealthy {
		t.Fatal("a peer two entries behind was healed by the healthy verdict")
	}
	_, _, head := r.primary.Boundaries()
	res := r.catchUp(t)
	if !res.Success {
		t.Fatalf("the catch-up failed: %s", res.FailReason)
	}
	// The registered close advanced the live shipper's cursor to the
	// recovered frontier, before any later append could repair it.
	if c := r.peer.Executor().WalShipperFor("r2").Cursor(); c != head || res.AchievedLSN != head {
		t.Fatalf("after the close the shipper cursor is %d (achieved %d); want the recovered frontier %d", c, res.AchievedLSN, head)
	}
	if s := r.peer.State(); s != replication.ReplicaHealthy {
		t.Fatalf("peer %s after a completed catch-up; want healthy", s)
	}
	for lba := uint32(0); lba < 4; lba++ {
		want, _ := r.primary.Read(lba)
		if got, err := r.replica.Read(lba); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("after the catch-up LBA %d differs from the primary (err %v)", lba, err)
		}
	}
	r.requireLiveTraffic(t)
}

// sessionOrderSites counts every reference to the peer's session order in
// the given files, keyed by its site: the enclosing function and, inside a
// type switch, its case ("prepareQueuedCommands/FenceAtEpoch"), or
// "<package level>" for a declaration initializer. A reference is the
// sessionIDs field (read, written, or a composite-literal key), a
// SessionIDs() call, or a draw (Next, NextAfter) through a local alias of
// either: an alias's binding counts once and each draw through it once
// more, so a second draw in a listed case changes its count.
func sessionOrderSites(files []*ast.File) map[string]int {
	sites := map[string]int{}
	isRef := func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.SelectorExpr:
			return x.Sel.Name == "sessionIDs"
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				return sel.Sel.Name == "SessionIDs"
			}
		case *ast.KeyValueExpr:
			if id, ok := x.Key.(*ast.Ident); ok {
				return id.Name == "sessionIDs"
			}
		}
		return false
	}
	// containsRef reports whether expression e references the order.
	containsRef := func(e ast.Node) bool {
		found := false
		ast.Inspect(e, func(m ast.Node) bool {
			if isRef(m) {
				found = true
			}
			return !found
		})
		return found
	}
	// visit counts node n (and, recursively, its children) at site; a case
	// clause of a type switch opens the site of its case.
	var visit func(n ast.Node, fn, site string, aliases map[string]bool)
	visit = func(n ast.Node, fn, site string, aliases map[string]bool) {
		if n == nil {
			return
		}
		if cc, ok := n.(*ast.CaseClause); ok && fn != "" {
			name := site
			for _, e := range cc.List {
				if sel, ok := e.(*ast.SelectorExpr); ok {
					name = fn + "/" + sel.Sel.Name
				}
			}
			for _, st := range cc.Body {
				visit(st, fn, name, aliases)
			}
			return
		}
		switch x := n.(type) {
		case *ast.AssignStmt:
			for i, rhs := range x.Rhs {
				if i < len(x.Lhs) && containsRef(rhs) {
					if id, ok := x.Lhs[i].(*ast.Ident); ok {
						aliases[id.Name] = true
					}
				}
			}
		case *ast.ValueSpec:
			for i, v := range x.Values {
				if i < len(x.Names) && containsRef(v) {
					aliases[x.Names[i].Name] = true
				}
			}
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Next" || sel.Sel.Name == "NextAfter") {
				if id, ok := sel.X.(*ast.Ident); ok && aliases[id.Name] {
					sites[site]++ // a draw through an alias
				}
			}
		}
		if isRef(n) {
			sites[site]++
		}
		ast.Inspect(n, func(m ast.Node) bool {
			if m == n {
				return true
			}
			visit(m, fn, site, aliases)
			return false
		})
	}
	for _, f := range files {
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				visit(d, "", "<package level>", map[string]bool{})
			case *ast.FuncDecl:
				if d.Body != nil {
					visit(d.Body, d.Name.Name, d.Name.Name, map[string]bool{})
				}
			}
		}
	}
	return sites
}

// Every reference to the peer's session order in the adapter's production
// source is a listed site: the constructor binds the executor's order, and
// each command kind that draws a session is listed with what keeps the
// live session above it (recovery sessions re-draw at their close and
// fences at their completion, on any outcome, covered by the tests above;
// a probe does not move the replica's held lineage). A helper that draws,
// an alias of the order, or a draw outside these cases fails here until it
// is classified.
func TestEverySessionDrawIsFollowedByALiveRedraw(t *testing.T) {
	want := map[string]int{
		"NewVolumeReplicaAdapter":             4, // the field's own order (sessionIDs: &SessionIDs{}), then the executor's (a.sessionIDs = src.SessionIDs())
		"prepareQueuedCommands/StartCatchUp":  1, // recovery session: afterSessionClose re-draws
		"prepareQueuedCommands/StartRebuild":  1, // recovery session: afterSessionClose re-draws
		"prepareQueuedCommands/StartRecovery": 1, // recovery session: afterSessionClose re-draws
		"prepareQueuedCommands/FenceAtEpoch":  1, // fence: afterFence re-draws
		"prepareQueuedCommands/ProbeReplica":  1, // non-moving: the replica validates a probe without taking its lineage
	}
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, filepath.Join("..", "..", "adapter"), func(fi fs.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, p := range pkgs {
		for _, f := range p.Files {
			files = append(files, f)
		}
	}
	got := sessionOrderSites(files)
	for site, n := range got {
		if want[site] != n {
			t.Errorf("%s references the session order %d time(s), listed %d: classify it (re-draw the live session after it, or declare it non-moving)", site, n, want[site])
		}
	}
	for site := range want {
		if got[site] == 0 {
			t.Errorf("listed site %s no longer references the session order; update the list", site)
		}
	}

	// Negative controls, each a changed count or a site of its own: a
	// helper that draws, a second direct draw in a listed case, an alias
	// drawn from twice inside a listed case, a draw outside any case, a
	// composite literal writing the field, and a package-level initializer.
	src := `package adapter
var pkgOrder = defaultExec.SessionIDs()
func (a *A) helperDraw() uint64 { return a.sessionIDs.Next() }
func helperLiteral() *A { return &A{sessionIDs: &SessionIDs{}} }
func (a *A) prepareQueuedCommands(cmds []any) {
	_ = a.sessionIDs.Next()
	for _, c := range cmds {
		switch c.(type) {
		case engine.FenceAtEpoch:
			_ = a.sessionIDs.Next()
			_ = a.sessionIDs.Next()
		case engine.ProbeReplica:
			ids := a.sessionIDs
			_ = ids.Next()
			_ = ids.Next()
		}
	}
}
`
	f, err := parser.ParseFile(token.NewFileSet(), "control.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	c := sessionOrderSites([]*ast.File{f})
	wantControl := map[string]int{
		"<package level>":                    1,
		"helperDraw":                         1,
		"helperLiteral":                      1,
		"prepareQueuedCommands":              1,
		"prepareQueuedCommands/FenceAtEpoch": 2,
		"prepareQueuedCommands/ProbeReplica": 3,
	}
	for site, n := range wantControl {
		if c[site] != n {
			t.Errorf("control site %s counted %d, want %d (all: %v)", site, c[site], n, c)
		}
	}
}
