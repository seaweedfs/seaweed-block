package master

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

// These are the record-to-placement and assignment decisions in this package.
// Reconcile and workload planning filter records; the product loop filters
// placements before both direct asks and RF3 promotion; workspaceVolume is the
// common RPC admission gate for binding, move, and publication. A new decision
// site must be listed here with a not-ready behavioral probe before it ships.
func TestLayoutStoreReadyDecisionSites(t *testing.T) {
	want := map[string]int{
		"journal_isolation.go reconcilePlacements ReconcilePlacement":            2,
		"journal_isolation.go reconcilePlacements readyVolumeRecords":            2,
		"launcher_plan.go RunLifecycleWorkloadPlanTick PlanBlockVolumeWorkloads": 1,
		"launcher_plan.go RunLifecycleWorkloadPlanTick readyVolumeRecords":       1,
		"layout_ready.go readyPlacements layoutStoreReady":                       1,
		"layout_ready.go readyVolumeRecords layoutStoreReady":                    1,
		"lifecycle_snapshot.go verifyPlacements readyPlacements":                 1,
		"product_loop.go RunLifecycleProductTick SubmitAssignmentAsk":            2,
		"product_loop.go RunLifecycleProductTick readyPlacements":                2,
		"product_loop.go submitPlacementSnapshots readyPlacements":               1,
		"workspace_binding.go PlaceWorkspace workspaceVolume":                    1,
		"workspace_binding.go RegisterWorkspaceBinding workspaceVolume":          1,
		"workspace_binding.go admitWorkspacePublication layoutStoreReady":        1,
		"workspace_service.go AckWorkspaceRevoked workspaceVolume":               1,
		"workspace_service.go MoveWorkspace MoveWorkspace":                       1,
		"workspace_service.go MoveWorkspace workspaceVolume":                     2,
		"workspace_service.go PreviewWorkspaceMove workspaceVolume":              1,
		"workspace_service.go QueryWorkspace workspaceVolume":                    1,
		"workspace_service.go QueryWorkspaceLostRange workspaceVolume":           1,
		"workspace_service.go RevokeWorkspace workspaceVolume":                   1,
		"workspace_service.go workspaceVolume layoutStoreReady":                  1,
		"workspace_service.go workspaceVolumeForRenewal layoutStoreReady":        1,
	}
	got := map[string]int{}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sites, err := layoutReadyDecisionSites(path, raw)
		if err != nil {
			t.Fatal(err)
		}
		for site, count := range sites {
			got[site] += count
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("placement or assignment site changed; enumerate its ready guard and behavioral probe: got=%v want=%v", got, want)
	}
}

func layoutReadyDecisionSites(path string, raw []byte) (map[string]int, error) {
	f, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
	if err != nil {
		return nil, err
	}
	methods := map[string]bool{
		"ReconcilePlacement": true, "PlanBlockVolumeWorkloads": true,
		"SubmitAssignmentAsk": true, "MoveWorkspace": true,
		"readyVolumeRecords": true, "readyPlacements": true,
		"workspaceVolume": true, "layoutStoreReady": true,
	}
	out := map[string]int{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if ok && methods[sel.Sel.Name] {
				out[path+" "+fn.Name.Name+" "+sel.Sel.Name]++
			}
			return true
		})
	}
	return out, nil
}

func TestLayoutReadyEnumerationSeesANewAssignmentSite(t *testing.T) {
	got, err := layoutReadyDecisionSites("new.go", []byte(`package master; func bypass(){ controller.SubmitAssignmentAsk(ask) }`))
	if err != nil {
		t.Fatal(err)
	}
	if got["new.go bypass SubmitAssignmentAsk"] != 1 {
		t.Fatal("new assignment site escaped enumeration", got)
	}
}
