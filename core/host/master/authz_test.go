package master

import (
	"sort"
	"testing"
)

// T2 (phase 0): every gRPC method the master registers has a row in the
// authorization table, and every row names a registered method; a new RPC
// cannot ship without a row (an unlisted method is refused at run time).
func TestMasterAuthorizationTableCoversEveryMethod(t *testing.T) {
	h, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
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
