package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpsChunk_RestoreStartsAndReportsStatus(t *testing.T) {
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chunk/restore" || r.Method != http.MethodPost {
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		got = append(got, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"volumeID":"v2","phase":"owning","running":true,"foreign":3}`))
	}))
	defer srv.Close()
	for _, args := range [][]string{{"restore", "--volume", "v2", "--status-addr", srv.URL}, {"restore", "--volume", "v2", "--status-addr", srv.URL, "--status"}} {
		var out, errb bytes.Buffer
		if code := runOpsChunk(args, &out, &errb); code != 0 || !strings.Contains(out.String(), `"phase":"owning"`) {
			t.Fatalf("%v: exit %d out %q err %q", args, code, out.String(), errb.String())
		}
	}
	if len(got) != 2 || got[0]["volumeID"] != "v2" || got[0]["statusOnly"] != false || got[1]["statusOnly"] != true {
		t.Fatalf("requests %v: want a start, then a status-only call", got)
	}
	var out, errb bytes.Buffer
	if code := runOpsChunk([]string{"restore", "--status-addr", srv.URL}, &out, &errb); code == 0 {
		t.Fatal("restore without --volume must be refused")
	}
}
