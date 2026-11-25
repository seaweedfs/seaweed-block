package chunkstore

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestLayoutRemote_StopSessionDoesNotWaitForCommit(t *testing.T) {
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.PutIntent(context.Background(), storage.ChunkIntent{Gen: 1}); err != nil {
		t.Fatal(err)
	}
	want := lh.head().Activity.Session
	// CASHead holds mu across the conditional store call. Settlement must be
	// able to obtain that same session while the call remains unresolved.
	r.mu.Lock()
	done := make(chan string, 1)
	go func() { done <- r.StopSession() }()
	select {
	case got := <-done:
		if got == "" || got != want {
			t.Errorf("session=%q want=%q", got, want)
		}
	case <-time.After(time.Second):
		t.Error("stop session waits for the in-flight Commit")
	}
	r.mu.Unlock()
}

func TestLayoutRemoteSessionFieldSites(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "layout_remote.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]int{}
	writers := map[string]int{}
	readers := map[string]int{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectorExpr); ok && s.Sel.Name == "session" {
				fields[fn.Name.Name]++
			}
			if c, ok := n.(*ast.CallExpr); ok {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "StopSession" {
					readers[fn.Name.Name]++
				}
				if s, ok := c.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "setSession" {
					writers[fn.Name.Name]++
				}
			}
			return true
		})
	}
	if !reflect.DeepEqual(readers, map[string]int{"openSessionLocked": 1, "rollBackLocked": 1, "Put": 1, "CASHead": 1, "writeRootLocked": 2, "Clean": 1, "syncSessionLocked": 2}) {
		t.Fatal("session readers changed; enumerate and probe them", readers)
	}
	if !reflect.DeepEqual(fields, map[string]int{"StopSession": 1, "setSession": 2}) {
		t.Fatal("session field readers/writers changed; enumerate and probe them", fields)
	}
	if !reflect.DeepEqual(writers, map[string]int{"openSessionLocked": 1, "CASHead": 1, "syncSessionLocked": 1, "takeAbandonLocked": 1}) {
		t.Fatal("session writers changed; enumerate their settlement behavior", writers)
	}
}
