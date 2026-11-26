package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The process-level integration tests (cmd/blockvolume, behind build tags)
// call this command by argument list: they compile without these tags in
// CI, so an invocation the CLI no longer accepts (a removed subcommand, a
// renamed or newly required flag) would pass compile and fail only on a
// lab run. Every literal `ops ...` invocation in them is run through this
// CLI's own argument handling here. Variable values become placeholders and
// addresses point at nothing, so a call that gets past its arguments fails
// on the network or is still waiting at the deadline; only a refusal of the
// arguments themselves fails this test.
func TestIntegrationTestInvocationsParse(t *testing.T) {
	invocations := literalOpsInvocations(t, filepath.Join("..", "blockvolume"))
	if len(invocations) == 0 {
		t.Fatal("no ops invocations found in the integration tests")
	}
	// Placeholders reach path flags too: whatever a call writes lands in a
	// temporary directory, never in the source tree.
	t.Chdir(t.TempDir())
	for _, inv := range invocations {
		inv := inv
		if len(inv.args) < 2 {
			t.Fatalf("%s: an ops invocation without a subcommand", inv.where)
		}
		t.Run(inv.where, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			done := make(chan int, 1)
			go func() { done <- run(inv.args, &stdout, &stderr) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				return // past its arguments: waiting on the network
			}
			out := stdout.String() + stderr.String()
			if strings.Contains(out, "rpc error") || strings.Contains(out, "dial tcp") {
				return // past its arguments: the placeholder address refused the call
			}
			for _, refusal := range []string{`"code":"bad_request"`, "unknown subcommand", "unknown command", "flag provided but not defined"} {
				if strings.Contains(out, refusal) {
					t.Fatalf("sw-block %s\nrefused its arguments: %s", strings.Join(inv.args, " "), firstLine(out, refusal))
				}
			}
		})
	}
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
			call, ok := n.(*ast.CallExpr)
			if !ok || call.Ellipsis != token.NoPos || len(call.Args) < 2 {
				return true
			}
			from := opsArg(call)
			if from < 0 {
				return true
			}
			var args []string
			for _, a := range call.Args[from:] {
				if v := literal(a); v != "" || isStringLit(a) {
					args = append(args, v)
					continue
				}
				args = append(args, placeholder(args, len(args)))
			}
			pos := fset.Position(call.Pos())
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
	case strings.Contains(flag, "lsn") || strings.Contains(flag, "epoch") || strings.Contains(flag, "count") || strings.Contains(flag, "bytes") || strings.Contains(flag, "size"):
		return "1"
	case strings.Contains(flag, "master") || strings.Contains(flag, "addr") || strings.Contains(flag, "filer") || strings.Contains(flag, "data") || strings.Contains(flag, "ctrl") || strings.Contains(flag, "endpoint"):
		return "127.0.0.1:1"
	}
	return name
}

func firstLine(out, near string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, near) {
			return l
		}
	}
	return out
}
