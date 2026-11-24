package master

import (
	"sort"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/auth"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// T2 (phase 0): every gRPC method the master registers has a row in the
// authorization table, and every row names a registered method; a new RPC
// cannot ship without a row (an unlisted method is refused at run time).
func TestMasterAuthorizationTableCoversEveryMethod(t *testing.T) {
	h, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() { closeTestMaster(t, h) })
	registered := map[string]bool{}
	for svc, info := range h.grpc.GetServiceInfo() {
		for _, m := range info.Methods {
			registered["/"+svc+"/"+m.Name] = true
		}
	}
	var missing, stale []string
	for m := range registered {
		if _, ok := masterRules[m]; !ok {
			missing = append(missing, m)
		}
	}
	for m := range masterRules {
		if !registered[m] {
			stale = append(stale, m)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 || len(stale) > 0 {
		t.Fatalf("authorization table out of date: methods without a rule %v; rules without a method %v", missing, stale)
	}
	if len(registered) != 24 {
		t.Fatalf("%d registered methods; the table was written for 24: review the new ones", len(registered))
	}
}

// The strict set (audit attempt written before the handler, or the call is
// refused) is exactly the mutating operator and admin methods; node methods
// and reads are never strict (the lease never waits on the audit log).
func TestMasterStrictAuditMethodsAreTheMutatingOnes(t *testing.T) {
	want := []string{
		control.FailbackService_ExecuteFailback_FullMethodName,
		control.LifecycleService_AbortVolumeDeleting_FullMethodName,
		control.LifecycleService_CommitVolumeDeleting_FullMethodName,
		control.LifecycleService_CreateVolume_FullMethodName,
		control.LifecycleService_DeleteVolume_FullMethodName,
		control.LifecycleService_MarkVolumeDeleting_FullMethodName,
		control.WorkspaceService_MoveWorkspace_FullMethodName,
		control.WorkspaceService_PlaceWorkspace_FullMethodName,
		control.WorkspaceService_RegisterWorkspaceBinding_FullMethodName,
		control.WorkspaceService_RevokeWorkspace_FullMethodName,
	}
	sort.Strings(want)
	var got []string
	for m, r := range masterRules {
		if r.Strict {
			got = append(got, m)
			if r.Node || r.Min == auth.RoleViewer {
				t.Errorf("%s is strict but open to nodes or viewers", m)
			}
		}
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("strict audit methods %v; want %v", got, want)
	}
}
