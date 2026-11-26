package layout

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// A repeated-id smoke test cannot prove a crypto source: pin the actual entropy
// call too. Full width is checked on the public VolumeID value.
func TestVolumeIDUses128CryptoRandomBits(t *testing.T) {
	id, err := NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != 16 || len(id.String()) != 32 {
		t.Fatal("volume id is not 128 bits")
	}
	f, err := parser.ParseFile(token.NewFileSet(), "volume.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	countCryptoReads := func() int {
		imports := map[string]string{}
		for _, i := range f.Imports {
			p, _ := strconv.Unquote(i.Path.Value)
			if p == "crypto/rand" {
				name := "rand"
				if i.Name != nil {
					name = i.Name.Name
				}
				imports[name] = p
			}
		}
		calls := 0
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "NewVolumeID" {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				c, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := c.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				recv, ok := sel.X.(*ast.Ident)
				if ok && imports[recv.Name] == "crypto/rand" && sel.Sel.Name == "Read" && len(c.Args) == 1 {
					if slice, ok := c.Args[0].(*ast.SliceExpr); ok && slice.Low == nil && slice.High == nil && slice.Max == nil {
						calls++
					}
				}
				return true
			})
		}
		return calls
	}
	calls := countCryptoReads()
	if calls != 1 {
		t.Fatalf("full-buffer crypto/rand.Read calls = %d, want 1", calls)
	}
	// A non-cryptographic replacement must fail even if ids are still 16 bytes.
	for _, i := range f.Imports {
		if i.Path.Value == "\"crypto/rand\"" {
			i.Path.Value = "\"math/rand\""
		}
	}
	if countCryptoReads() != 0 {
		t.Fatal("crypto-source guard accepted math/rand")
	}

}
