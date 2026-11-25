package master

import (
	"sort"
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
	if len(registered) != 27 {
		t.Fatalf("%d registered methods; the table was written for 27: review the new ones", len(registered))
	}
}

// The strict set (audit attempt written before the handler, or the call is
// refused) is exactly the mutating operator and admin methods; node methods
// and reads are never strict (the lease never waits on the audit log).
// TestMasterOperatorMethodsAreStrict is inverted on purpose: every method an
// operator or admin may call is strict (its attempt line is written before it
// runs, or it is refused) unless it is named below with the reason. A new
// operator or admin method added without Strict fails here.
func TestMasterOperatorMethodsAreStrict(t *testing.T) {
	notStrict := map[string]string{
		// Appends an operator's note to the cluster event log; it changes no
		// volume, placement or lease state.
		control.ObservationService_ReportClusterEvent_FullMethodName: "event log append only",
	}
	strict := 0
	for m, r := range masterRules {
		person := r.Min == auth.RoleOperator || r.Min == auth.RoleAdmin
		if r.Strict && (r.Node || !person) {
			t.Errorf("%s is strict but open to nodes or viewers", m)
		}
		if r.Strict {
			strict++
		}
		_, excepted := notStrict[m]
		switch {
		case person && !r.Strict && !excepted:
			t.Errorf("%s (min %s) is not strict: mark it Strict or name it in notStrict with the reason", m, r.Min)
		case excepted && (r.Strict || !person):
			t.Errorf("%s is named in notStrict but is not a non-strict operator method: drop the entry", m)
		}
	}
	if strict != 11 {
		t.Errorf("%d strict methods; the phase 0 set is 11 (update this count with the change that adds one)", strict)
	}
}
