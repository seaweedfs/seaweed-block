package transport

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Every path that applies a primary's data to this store, with the epoch
// table rule that covers it. "row first": a live entry of the current epoch,
// noted before the apply. "invalid first": history from a recovery; the
// table is made invalid before its first byte and only the recovery's final
// replace makes it valid. A new apply path fails here until it is classified.
var peerApplyPaths = map[string]string{
	"core/transport/replica.go: func handleConn .ApplyEntry":           "row first (live lane) / invalid first (recovery lane, MsgRecoveryLaneStart)",
	"core/transport/replica.go: func handleConn .WriteBase":            "invalid first (beginInstall invalidates before the first base byte)",
	"core/transport/replica.go: func handleConn .BeginInstall":         "invalid first (beginInstall invalidates before BeginInstall)",
	"core/replication/apply_gate.go: func applyRecovery .ApplyEntry":   "invalid first (reached only from handleConn's recovery lane)",
	"core/replication/apply_gate.go: func applyLive .ApplyEntry":       "row first (reached only from handleConn's live lane, after the note)",
	"core/recovery/receiver.go: func beginInstall .BeginInstall":       "invalid first (dual-lane: SessionStart takes the tokened mark before any data frame)",
	"core/recovery/rebuild_session.go: func ApplyBaseBlock .WriteBase": "invalid first (dual-lane: SessionStart takes the tokened mark before any data frame)",
}

var peerApplyMethods = map[string]bool{"ApplyEntry": true, "WriteBase": true, "BeginInstall": true, "WriteExtentDirect": true}

func peerApplyCalls(t *testing.T, root string, dirs []string) []string {
	t.Helper()
	var out []string
	for _, d := range dirs {
		err := filepath.WalkDir(filepath.Join(root, d), func(path string, e fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if e.IsDir() {
				if path != filepath.Join(root, d) {
					return filepath.SkipDir // the listed package only, not its subpackages
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			if ok, err := build.Default.MatchFile(filepath.Dir(path), filepath.Base(path)); err != nil || !ok {
				return err
			}
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				ast.Inspect(fn, func(n ast.Node) bool {
					if c, ok := n.(*ast.CallExpr); ok {
						if s, ok := c.Fun.(*ast.SelectorExpr); ok && peerApplyMethods[s.Sel.Name] {
							out = append(out, rel+": func "+fn.Name.Name+" ."+s.Sel.Name)
						}
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(out)
	return out
}

func TestEveryPeerApplyPathHasAnEpochTableRule(t *testing.T) {
	got := peerApplyCalls(t, filepath.Join("..", ".."), []string{"core/transport", "core/replication", "core/recovery"})
	seen := map[string]bool{}
	for _, c := range got {
		seen[c] = true
		if rule, ok := peerApplyPaths[c]; !ok || rule == "" {
			t.Errorf("apply path %s has no epoch-table rule: classify it as row first or invalid first", c)
		}
	}
	for c, rule := range peerApplyPaths {
		if rule != "" && !seen[c] {
			t.Errorf("classified apply path %s no longer exists: remove it", c)
		}
	}
}
