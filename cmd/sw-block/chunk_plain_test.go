package main

import (
	"net/http"
	"strings"
	"testing"
)

// M5 PlainStop -> Quiesce, INV-CUT-PLAIN-DETACH-001: encode the existing
// idle seal request; preserve JSON stdout for existing operator tooling.
func TestOpsChunkPlainDetachSendsTheExistingRequest(t *testing.T) {
	const reply = `{"volumeID":"vol","headGen":11,"headState":"detached"}`
	srv, got := chunkServer(t, http.StatusOK, reply)
	code, out, errOut := runChunk("detach", "--plain", "--volume", "vol", "--status-addr", srv.URL, "--timeout", "2s")
	if code != 0 || out != reply || !strings.Contains(errOut, "sealed HEAD generation: 11\n") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, out, errOut)
	}
	if len(*got) != 1 || (*got)[0].path != "/chunk/detach" || (*got)[0].body["plain"] != true || (*got)[0].body["volumeID"] != "vol" || (*got)[0].body["timeoutMs"] != float64(2000) {
		t.Fatalf("plain request=%+v", *got)
	}
	for _, key := range []string{"frozen", "freezeStartedAtNs", "allowOverBudget"} {
		if _, ok := (*got)[0].body[key]; ok {
			t.Fatalf("plain request contains %s", key)
		}
	}
}

func TestOpsChunkPlainDetachRefusesInvalidMixBeforeHTTP(t *testing.T) {
	srv, got := chunkServer(t, http.StatusOK, `{}`)
	for _, extra := range [][]string{
		{"--frozen", "--freeze-started-at-ns", "1"}, {"--freeze-started-at-ns", "1"}, {"--allow-over-budget"},
		{"--timeout", "0s"}, {"--timeout", "-1s"}, {"unexpected"},
	} {
		args := append([]string{"detach", "--plain", "--volume", "vol", "--status-addr", srv.URL}, extra...)
		if code, _, _ := runChunk(args...); code != 2 || len(*got) != 0 {
			t.Fatalf("%v: exit=%d requests=%d", extra, code, len(*got))
		}
	}
}

func TestOpsChunkPlainDetachPreservesTypedFailure(t *testing.T) {
	for _, reason := range []string{"busy", "fenced", "outcome_unknown"} {
		t.Run(reason, func(t *testing.T) {
			reply := `{"code":"` + reason + `","message":"seal refused","retryable":true}`
			srv, got := chunkServer(t, http.StatusConflict, reply)
			code, out, errOut := runChunk("detach", "--plain", "--volume", "vol", "--status-addr", srv.URL)
			if code != 1 || out != reply+"\n" || len(*got) != 1 || strings.Contains(errOut, "sealed HEAD generation:") {
				t.Fatalf("exit=%d stdout=%q stderr=%q requests=%d", code, out, errOut, len(*got))
			}
		})
	}
}
