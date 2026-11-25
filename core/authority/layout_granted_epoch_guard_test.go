package authority

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Start relies on a granted higher epoch, not elapsed time at the volume.
// These are all epoch constructors and reassign request producers in the
// authority package. All request producers converge on applyWithWorkspaceMove:
// an enrolled LOCAL volume refuses an ordinary reassign, and a workspace
// grant is revalidated by prepareWorkspaceMoveLocked under the mint lock.
// Genesis placement has no predecessor; reload copies durable epochs. NET
// promotion and failback use ordinary reassign, never a LOCAL lease bypass.
func TestLayoutGrantedEpochIssuerSites(t *testing.T) {
	want := map[string]int{
		"authority.go applyWithWorkspaceMove epoch assignment":           1,
		"authority.go reloadFromStore epoch":                             1,
		"authority.go applyWithWorkspaceMove epoch":                      2,
		"controller.go decideVolume reassign":                            2,
		"failback_runtime.go ExecuteFailback reassign":                   1,
		"workspace.go MoveWorkspace reassign":                            1,
		"authority.go apply applyWithWorkspaceMove":                      2,
		"workspace.go MoveWorkspace applyWithWorkspaceMove":              1,
		"authority.go applyWithWorkspaceMove prepareWorkspaceMoveLocked": 1,
	}
	got := map[string]int{}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range files {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		sites, err := layoutEpochSites(p, raw)
		if err != nil {
			t.Fatal(err)
		}
		for k, n := range sites {
			got[k] += n
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("epoch issuers changed; enumerate each proof/barrier and add its probe: got=%v want=%v", got, want)
	}
	// Existing behavioral probes pin the deciding guards: clean needs detached
	// HEAD plus ACK; forced needs the lease barrier, including restart; an
	// ordinary failover cannot bypass an enrolled workspace's guarded move.
}

func layoutEpochSites(path string, raw []byte) (map[string]int, error) {
	f, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		record := func(kind string) { out[path+" "+fn.Name.Name+" "+kind]++ }
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if assign, ok := n.(*ast.AssignStmt); ok {
				for _, lhs := range assign.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "Epoch" {
						record("epoch assignment")
					}
				}
			}
			if inc, ok := n.(*ast.IncDecStmt); ok {
				if sel, ok := inc.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "Epoch" {
					record("epoch assignment")
				}
			}

			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "applyWithWorkspaceMove" || sel.Sel.Name == "prepareWorkspaceMoveLocked") {
					record(sel.Sel.Name)
				}
			}
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			typ := ""
			switch x := lit.Type.(type) {
			case *ast.SelectorExpr:
				typ = x.Sel.Name
			case *ast.Ident:
				typ = x.Name
			}
			for _, e := range lit.Elts {
				kv, ok := e.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				if typ == "AssignmentInfo" && key.Name == "Epoch" {
					record("epoch")
				}
				if typ == "AssignmentAsk" && key.Name == "Intent" {
					if id, ok := kv.Value.(*ast.Ident); ok && id.Name == "IntentReassign" {
						record("reassign")
					}
				}
			}
			return true
		})
	}
	return out, nil
}

func TestLayoutEpochEnumerationRejectsANewIssuer(t *testing.T) {
	got, err := layoutEpochSites("new.go", []byte(`package authority; func bypass(){ _ = adapter.AssignmentInfo{Epoch: 99}; _ = AssignmentAsk{Intent:IntentReassign} }`))
	if err != nil {
		t.Fatal(err)
	}
	if got["new.go bypass epoch"] != 1 || got["new.go bypass reassign"] != 1 {
		t.Fatal("new issuer escaped enumeration", got)
	}
}
