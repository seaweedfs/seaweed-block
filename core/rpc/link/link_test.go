package link

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func serve(t *testing.T, cfg ServerConfig) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(cfg)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	done := make(chan struct{})
	go func() { _ = srv.Serve(lis); close(done) }()
	t.Cleanup(func() {
		srv.Stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	return lis.Addr().String()
}

// A link dialed with Dial reaches a server built with NewServer, for a unary
// and a streaming call, as plaintext gRPC did before the seam.
func TestLink_DialReachesNewServerUnaryAndStream(t *testing.T) {
	addr := serve(t, ServerConfig{})
	conn, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := healthpb.NewHealthClient(conn)
	if r, err := c.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("unary call: %v %v", r, err)
	}
	w, err := c.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := w.Recv(); err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("stream call: %v %v", r, err)
	}
}

// The server runs its unary and stream interceptors in the configured order.
func TestLink_ServerRunsInterceptorsInOrder(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	note := func(s string) { mu.Lock(); seen = append(seen, s); mu.Unlock() }
	unary := func(name string) grpc.UnaryServerInterceptor {
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			note(name)
			return h(ctx, req)
		}
	}
	stream := func(name string) grpc.StreamServerInterceptor {
		return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			note(name)
			return h(srv, ss)
		}
	}
	addr := serve(t, ServerConfig{Unary: []grpc.UnaryServerInterceptor{unary("u1"), unary("u2")}, Stream: []grpc.StreamServerInterceptor{stream("s1"), stream("s2")}})
	conn, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := healthpb.NewHealthClient(conn)
	if _, err := c.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	w, err := c.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Recv(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := strings.Join(seen, ","); got != "u1,u2,s1,s2" {
		t.Fatalf("interceptors ran as %q, want u1,u2,s1,s2", got)
	}
}

// rawLinkCalls lists the gRPC link constructions in one Go source that
// bypass this package: a client (grpc.NewClient, grpc.Dial, grpc.DialContext),
// a server (grpc.NewServer) or plaintext credentials (insecure.NewCredentials),
// whatever local name the import has (aliases and dot imports included).
func rawLinkCalls(name string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	banned := map[string]map[string]bool{
		"google.golang.org/grpc":                      {"NewClient": true, "Dial": true, "DialContext": true, "NewServer": true},
		"google.golang.org/grpc/credentials/insecure": {"NewCredentials": true},
	}
	local := map[string]map[string]bool{} // import name -> banned functions
	dot := map[string]bool{}              // banned functions reachable unqualified
	for _, im := range f.Imports {
		path, _ := strconv.Unquote(im.Path.Value)
		fns, ok := banned[path]
		if !ok {
			continue
		}
		name := filepath.Base(path)
		if im.Name != nil {
			name = im.Name.Name
		}
		switch name {
		case "_":
		case ".":
			for fn := range fns {
				dot[fn] = true
			}
		default:
			local[name] = fns
		}
	}
	var found []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.SelectorExpr:
			if x, ok := fun.X.(*ast.Ident); ok && local[x.Name][fun.Sel.Name] {
				found = append(found, fmt.Sprintf("%s: %s.%s", fset.Position(call.Pos()), x.Name, fun.Sel.Name))
			}
		case *ast.Ident:
			if dot[fun.Name] {
				found = append(found, fmt.Sprintf("%s: %s", fset.Position(call.Pos()), fun.Name))
			}
		}
		return true
	})
	return found, nil
}

// The detector finds every form of a raw construction, and nothing in a file
// that only uses this package.
func TestLink_GuardDetectsRawConstructionsAndAliases(t *testing.T) {
	cases := map[string]string{
		"client": `package p
import "google.golang.org/grpc"
func f() { grpc.NewClient("x") }
`,
		"dial": `package p
import "google.golang.org/grpc"
func f() { grpc.Dial("x") }
`,
		"dialContext": `package p
import "google.golang.org/grpc"
func f() { grpc.DialContext(nil, "x") }
`,
		"server": `package p
import "google.golang.org/grpc"
func f() { grpc.NewServer() }
`,
		"aliasClient": `package p
import g "google.golang.org/grpc"
func f() { g.NewClient("x") }
`,
		"aliasCreds": `package p
import ins "google.golang.org/grpc/credentials/insecure"
func f() { ins.NewCredentials() }
`,
		"dotImport": `package p
import . "google.golang.org/grpc"
func f() { NewServer() }
`,
		"plainCredsFn": `package p
import "google.golang.org/grpc/credentials/insecure"
func f() { insecure.NewCredentials() }
`,
	}
	for name, src := range cases {
		got, err := rawLinkCalls(name+".go", []byte(src))
		if err != nil || len(got) != 1 {
			t.Errorf("%s: found %v (%v), want exactly one raw construction", name, got, err)
		}
	}
	clean := `package p
import (
	"google.golang.org/grpc"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
)
func f() { link.Dial("x", grpc.WithBlock()) }
`
	if got, err := rawLinkCalls("clean.go", []byte(clean)); err != nil || len(got) != 0 {
		t.Errorf("clean: found %v (%v), want none", got, err)
	}
}

// Every production gRPC link goes through this package: no other non-test Go
// file constructs a gRPC client or server or plaintext credentials directly.
func TestLink_NoProductionSiteBypassesTheSeam(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	self, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	var bad []string
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "vendor" || name == "testdata" || p == self {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		found, err := rawLinkCalls(p, src)
		if err != nil {
			return err
		}
		for _, f := range found {
			rel, _ := filepath.Rel(root, p)
			bad = append(bad, rel+" ("+f[strings.LastIndex(f, ": ")+2:]+")")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) > 0 {
		t.Fatalf("gRPC links built outside core/rpc/link (use link.Dial, link.DialWith or link.NewServer): %v", bad)
	}
}
