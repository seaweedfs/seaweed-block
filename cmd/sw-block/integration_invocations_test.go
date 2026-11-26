package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// The process-level integration tests (cmd/blockvolume, behind build tags)
// call this command by argument list: they compile without these tags in
// CI, so an invocation the CLI no longer accepts (a removed subcommand, a
// renamed or newly required flag) would pass compile and fail only on a
// lab run. Every literal `ops ...` invocation in them is run through this
// CLI's own argument handling here, each in a child process killed at its
// deadline. Variable values become placeholders; in the child the master
// resolves every name and the filer dial and the restore RPC answer
// errGuardPast, which a command reaches only after all its argument checks.
// Other calls meet addresses nothing listens on. A refusal of the arguments
// fails this test, and so does a call that neither answers nor fails
// within the deadline: waiting is not proof that the arguments passed.
func TestIntegrationTestInvocationsParse(t *testing.T) {
	invocations := literalOpsInvocations(t, filepath.Join("..", "blockvolume"))
	if len(invocations) == 0 {
		t.Fatal("no ops invocations found in the integration tests")
	}
	for _, inv := range invocations {
		inv := inv
		if len(inv.args) < 2 {
			t.Fatalf("%s: an ops invocation without a subcommand", inv.where)
		}
		t.Run(inv.where, func(t *testing.T) {
			if verdict, out := guardInvocation(t, inv.args); verdict != guardPast {
				t.Fatalf("sw-block %s\n%s: %s", strings.Join(inv.args, " "), verdict, out)
			}
		})
	}
}

// The guard's own controls: argument lists whose missing flag is checked
// only after the volume names resolve must be refused, and a complete one
// must pass.
func TestIntegrationInvocationGuardReachesLateFlagChecks(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"ops", "chunk", "snapshots", "--master", "127.0.0.1:1", "--volume", "v1"}, guardRefused},
		{[]string{"ops", "chunk", "snapshot-delete", "--master", "127.0.0.1:1", "--volume", "v1", "--name", "s1"}, guardRefused},
		{[]string{"ops", "chunk", "restore", "--source", "v1", "--snapshot", "s1", "--target", "v2", "--chunk-filer", "127.0.0.1:1"}, guardRefused},
		{[]string{"ops", "chunk", "snapshots", "--master", "127.0.0.1:1", "--volume", "v1", "--chunk-filer", "127.0.0.1:1"}, guardPast},
		{[]string{"ops", "chunk", "restore", "--master", "127.0.0.1:1", "--source", "v1", "--snapshot", "s1", "--target", "v2", "--chunk-filer", "127.0.0.1:1"}, guardPast},
	} {
		if verdict, out := guardInvocation(t, c.args); verdict != c.want {
			t.Errorf("sw-block %s: %s, want %s: %s", strings.Join(c.args, " "), verdict, c.want, out)
		}
	}
}

const (
	guardPast    = "past its arguments"
	guardRefused = "refused its arguments"
	guardTimeout = "still running at the deadline"
	guardOther   = "failed for another reason"
)

// errGuardPast is what the child's filer dial and restore RPC answer.
var errGuardPast = errors.New("invocation guard: past its arguments")

const guardArgsEnv = "SWB_INVOCATION_GUARD_ARGS"

// guardInvocation runs args in a child process of this test binary and
// classifies its output.
func guardInvocation(t *testing.T, args []string) (string, string) {
	t.Helper()
	enc, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIntegrationInvocationChild$", "-test.count=1")
	cmd.Dir = t.TempDir() // whatever a call writes lands here, never in the source tree
	cmd.Env = append(os.Environ(), guardArgsEnv+"="+string(enc))
	cmd.WaitDelay = time.Second
	raw, _ := cmd.CombinedOutput()
	out := string(raw)
	switch {
	case ctx.Err() != nil:
		return guardTimeout, out
	case strings.Contains(out, errGuardPast.Error()):
		return guardPast, firstLine(out, errGuardPast.Error())
	}
	for _, refusal := range []string{`"code":"bad_request"`, "unknown subcommand", "unknown command", "flag provided but not defined", "invalid value", "is required"} {
		if strings.Contains(out, refusal) {
			return guardRefused, firstLine(out, refusal)
		}
	}
	if strings.Contains(out, "rpc error") || strings.Contains(out, "dial tcp") || strings.Contains(out, "connection refused") {
		return guardPast, "the placeholder address refused the call"
	}
	return guardOther, out
}

// TestIntegrationInvocationChild is the child process: it runs only with
// guardArgsEnv set.
func TestIntegrationInvocationChild(t *testing.T) {
	enc := os.Getenv(guardArgsEnv)
	if enc == "" {
		t.Skip("the invocation guard's child process")
	}
	var args []string
	if err := json.Unmarshal([]byte(enc), &args); err != nil {
		t.Fatal(err)
	}
	resolveVolumeID = func(context.Context, string, string) (layout.VolumeID, error) { return layout.VolumeID{1}, nil }
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return nil, nil, errGuardPast }
	restoreCreateRPC = func(context.Context, string, *control.CreateVolumeRequest) (*control.CreateVolumeResponse, error) {
		return nil, errGuardPast
	}
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	fmt.Printf("exit %d\n%s%s", code, stdout.String(), stderr.String())
}

type opsInvocation struct {
	where string
	args  []string
}

// literalOpsInvocations returns every `<x>.command(t, "ops", ...)`, `<x>.run(t, "ops", ...)` and
// `runCLI(ctx, h, "ops", ...)` call in
// dir's test files whose arguments are listed (not spread from a slice),
// with each non-literal argument replaced by a placeholder for its flag.
func literalOpsInvocations(t *testing.T, dir string) []opsInvocation {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	var out []opsInvocation
	fset := token.NewFileSet()
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			var elems []ast.Expr
			switch x := n.(type) {
			case *ast.CallExpr:
				if x.Ellipsis != token.NoPos || len(x.Args) < 2 {
					return true
				}
				from := opsArg(x)
				if from < 0 {
					return true
				}
				elems = x.Args[from:]
			case *ast.CompositeLit:
				// a prepared argument list, []string{"ops", ...}, that a
				// harness call spreads
				if len(x.Elts) < 2 || literal(x.Elts[0]) != "ops" {
					return true
				}
				elems = x.Elts
			default:
				return true
			}
			var args []string
			for _, a := range elems {
				if v := literal(a); v != "" || isStringLit(a) {
					args = append(args, v)
					continue
				}
				args = append(args, placeholder(args, len(args)))
			}
			pos := fset.Position(n.Pos())
			out = append(out, opsInvocation{where: filepath.Base(pos.Filename) + ":" + strconv.Itoa(pos.Line), args: args})
			return true
		})
	}
	return out
}

// opsArg is the index of the literal "ops" that starts the CLI argument
// list of a harness call: x.command(t, "ops", ...) or runCLI(ctx, h, "ops",
// ...); -1 for any other call.
func opsArg(call *ast.CallExpr) int {
	name := ""
	switch f := call.Fun.(type) {
	case *ast.SelectorExpr:
		name = f.Sel.Name
	case *ast.Ident:
		name = f.Name
	}
	at := map[string]int{"command": 1, "run": 1, "runCLI": 2}[name]
	if at == 0 || len(call.Args) <= at || literal(call.Args[at]) != "ops" {
		return -1
	}
	return at
}

func isStringLit(e ast.Expr) bool {
	b, ok := e.(*ast.BasicLit)
	return ok && b.Kind == token.STRING
}

func literal(e ast.Expr) string {
	b, ok := e.(*ast.BasicLit)
	if !ok || b.Kind != token.STRING {
		return ""
	}
	v, err := strconv.Unquote(b.Value)
	if err != nil {
		return ""
	}
	return v
}

// placeholder is a value of the kind the preceding flag takes: an address
// nothing listens on, a number, a duration, or a name.
func placeholder(prev []string, at int) string {
	name := "x" + strconv.Itoa(at) // distinct, so two variables never compare equal
	if len(prev) == 0 {
		return name
	}
	flag := strings.TrimLeft(prev[len(prev)-1], "-")
	switch {
	case strings.Contains(flag, "timeout") || strings.Contains(flag, "interval") || strings.Contains(flag, "wait"):
		return "1s"
	case strings.Contains(flag, "lsn") || strings.Contains(flag, "epoch") || strings.Contains(flag, "gen") || strings.Contains(flag, "count") || strings.Contains(flag, "bytes") || strings.Contains(flag, "size"):
		return "1"
	case strings.Contains(flag, "confirm"):
		return volumeOf(prev) + "@1" // a forced move's confirmation names its volume and epoch
	case strings.Contains(flag, "master") || strings.Contains(flag, "addr") || strings.Contains(flag, "filer") || strings.Contains(flag, "data") || strings.Contains(flag, "ctrl") || strings.Contains(flag, "endpoint"):
		return "127.0.0.1:1"
	}
	return name
}

// volumeOf is the --volume value among args, or "".
func volumeOf(args []string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--volume" {
			return args[i+1]
		}
	}
	return ""
}

func firstLine(out, near string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, near) {
			return l
		}
	}
	return out
}
