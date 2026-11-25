package auth_test

import (
	"go/parser"
	"go/token"
	"testing"
)

func TestCodexBypassCensusSeesAUseInsideASelectorReceiver(t *testing.T) {
	src := `package p
import "context"
import "github.com/seaweedfs/seaweed-block/core/auth"
var completed = auth.DevCall(context.Background()).Done
`
	f, err := parser.ParseFile(token.NewFileSet(), "probe.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := bypassUses(f, false)
	if len(got) != 1 {
		t.Fatalf("nested bypass reference census=%v; want one DevCall use", got)
	}
}
