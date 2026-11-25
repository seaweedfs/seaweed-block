package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The operator must never replace the master's proof with local admission.
func TestChunkVolumeDeleteAuthorityRPCSites(t *testing.T) {
	for _, tc := range []struct {
		file string
		want map[string]int
	}{
		{"chunk_volume_delete.go", map[string]int{"ReserveVolumeDeletion": 1, "CheckVolumeNoLiveLease": 1, "DialMasterFromEnv": 1}},
		// resolveVolumeID reads the name binding; restoreCreateRPC makes the
		// restore's one master CreateVolume call (TestOpsChunkRestoreUsesOnlyMasterCreate).
		{"chunk_b.go", map[string]int{"DialMasterFromEnv": 2}},
		{"../../core/host/master/volume_delete_authority.go", map[string]int{"VolumeDeleteAuthority": 2, "Reserve": 1, "NoLiveLease": 1}},
	} {
		f, err := parser.ParseFile(token.NewFileSet(), tc.file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]int{}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Dial", "DialMasterFromEnv", "ReserveVolumeDeletion", "CheckVolumeNoLiveLease", "MarkVolumeDeleting", "AbortVolumeDeleting", "CommitVolumeDeleting", "VolumeDeleteAuthority", "Reserve", "NoLiveLease":
				got[sel.Sel.Name]++
			}
			return true
		})
		for site, n := range got {
			if tc.want[site] != n {
				t.Errorf("%s: authority site %s=%d, want %d", tc.file, site, n, tc.want[site])
			}
		}
		for site, n := range tc.want {
			if got[site] != n {
				t.Errorf("%s: missing authority site %s=%d, want %d", tc.file, site, got[site], n)
			}
		}
	}
}
