package main

import (
	"testing"
)

func TestOpsChunkFrozenDetachFlags(t *testing.T) {
	srv, got := chunkServer(t, 200, `{"volumeID":"v1"}`)
	code, _, errs := runChunk("detach", "--volume=v1", "--status-addr="+srv.URL, "--frozen", "--freeze-started-at-ns=1234567", "--allow-over-budget")
	if code != 0 || len(*got) != 1 || (*got)[0].body["frozen"] != true || (*got)[0].body["freezeStartedAtNs"] != float64(1234567) || (*got)[0].body["allowOverBudget"] != true {
		t.Fatalf("frozen request lost budget: %d %s %+v", code, errs, *got)
	}
	for _, flags := range [][]string{{"--frozen"}, {"--allow-over-budget"}, {"--freeze-started-at-ns=123"}} {
		args := append([]string{"detach", "--volume=v1", "--status-addr=" + srv.URL}, flags...)
		if code, _, _ := runChunk(args...); code == 0 || len(*got) != 1 {
			t.Fatalf("invalid flags sent request: %v", flags)
		}
	}
}
