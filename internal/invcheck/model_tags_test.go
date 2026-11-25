package invcheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Every action of a state machine the code implements names the one Go
// function that implements it, by a line in that function's doc comment:
//
//	// model: RestoreTarget.AddDep
//
// modelActions lists each model's actions (the top-level definitions its
// Next relation names). An action marked env is the environment's step (a
// guest write, a lease running out, the store applying a request issued
// earlier): no function implements it and none may carry its tag. Every
// other action has exactly one tagged function; a function may carry
// several tags. A function renamed, moved or deleted without its tag, a tag
// naming no listed action, or a model action left untagged fails here.
var modelActions = map[string]map[string]string{
	"RestoreTarget": {
		"DeleteSnapS":  "code",
		"AddDep":       "code",
		"CreateVol":    "code",
		"CreateHead":   "code",
		"Diverge":      "env", // the target's own writes after the restore
		"OpTomb":       "code",
		"DeleteTarget": "code",
		"OpRelease":    "code",
	},
	"SnapshotRemoval": {
		"Write":       "env", // a guest write
		"DeleteSnap":  "code",
		"Expire":      "env", // the holder's lease runs out
		"Claim":       "code",
		"Begin":       "code",
		"Commit":      "code",
		"Abort":       "code",
		"BeginClean":  "code",
		"EndClean":    "code",
		"Upload":      "code",
		"Issue":       "code",
		"Validate":    "code",
		"Stat":        "code",
		"ApplyMark":   "env", // the store applies a mark issued earlier
		"ApplyDelete": "env", // the store applies a delete issued earlier
	},
}

var modelTag = regexp.MustCompile(`^model:\s*([A-Za-z][A-Za-z0-9]*)\.([A-Za-z][A-Za-z0-9]*)\s*$`)

// modelTags returns, for each "Module.Action" tag in a function's doc
// comment under core/ and cmd/, the functions that carry it.
func modelTags(t *testing.T, root string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	fset := token.NewFileSet()
	for _, dir := range []string{"core", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if name := d.Name(); name == "testdata" || strings.HasPrefix(name, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Doc == nil {
					continue
				}
				name := f.Name.Name + "." + fn.Name.Name
				if fn.Recv != nil && len(fn.Recv.List) > 0 {
					typ := fn.Recv.List[0].Type
					if star, ok := typ.(*ast.StarExpr); ok {
						typ = star.X
					}
					if id, ok := typ.(*ast.Ident); ok {
						name = f.Name.Name + "." + id.Name + "." + fn.Name.Name
					}
				}
				for _, c := range fn.Doc.List {
					if m := modelTag.FindStringSubmatch(strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))); m != nil {
						out[m[1]+"."+m[2]] = append(out[m[1]+"."+m[2]], name)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestModelActionsNameTheirFunctions(t *testing.T) {
	tags := modelTags(t, repoRoot(t))
	for tag, funcs := range tags {
		module, action, _ := strings.Cut(tag, ".")
		kind, ok := modelActions[module][action]
		switch {
		case !ok:
			t.Errorf("model: %s on %v names no listed model action", tag, funcs)
		case kind == "env":
			t.Errorf("model: %s on %v is an environment step; no function implements it", tag, funcs)
		}
	}
	modules := make([]string, 0, len(modelActions))
	for m := range modelActions {
		modules = append(modules, m)
	}
	sort.Strings(modules)
	for _, module := range modules {
		for action, kind := range modelActions[module] {
			if kind != "code" {
				continue
			}
			switch funcs := tags[module+"."+action]; len(funcs) {
			case 1:
			case 0:
				t.Errorf("model action %s.%s has no '// model: %s.%s' function", module, action, module, action)
			default:
				t.Errorf("model action %s.%s is tagged on %d functions %v; want one", module, action, len(funcs), funcs)
			}
		}
	}
}
