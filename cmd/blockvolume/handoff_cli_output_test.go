package main_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// handoffHeadJSON adapts the CLI's typed show envelope to the canonical HEAD
// bytes the process tests inspect. A public volume name is not its directory.
func handoffHeadJSON(out string) (string, error) {
	var result struct {
		Head *layout.Head `json:"head"`
	}
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		return "", err
	}
	if result.Head == nil {
		return "", fmt.Errorf("show returned no HEAD")
	}
	raw, err := result.Head.Encode()
	return string(raw), err
}

func handoffDeleteCompleted(out string) bool {
	var result struct {
		Volume string `json:"volume"`
		State  string `json:"state"`
	}
	return json.Unmarshal([]byte(out), &result) == nil && result.Volume != "" && result.State == layout.StateDeleted
}

func TestHandoffShowUsesTheNestedLayoutHead(t *testing.T) {
	for _, state := range []string{layout.StateLive, layout.StateDeleted} {
		t.Run(state, func(t *testing.T) {
			head := layout.Head{VolumeID: layout.VolumeID{1}, State: state, Activity: layout.Activity{Kind: layout.ActIdle}}
			// This is chunkShow's public JSON shape, not HEAD's on-filer codec.
			raw, err := json.Marshal(struct {
				Volume any          `json:"volume"`
				Head   *layout.Head `json:"head"`
			}{Head: &head})
			if err != nil {
				t.Fatal(err)
			}
			got, err := handoffHeadJSON(string(raw))
			if err != nil || !strings.Contains(got, `"epoch":0,"holder":""`) || (state == layout.StateDeleted && !strings.Contains(got, `"state":"deleted"`)) {
				t.Fatalf("show adapter: %s, %v", got, err)
			}
		})
	}
	for _, raw := range []string{`{}`, `{"head":null}`, `{"head":{"State":"deleted"}}`, `not JSON`} {
		if got, err := handoffHeadJSON(raw); err == nil {
			t.Errorf("invalid/absent HEAD accepted: %s -> %s", raw, got)
		}
	}
}

func TestHandoffDeleteUsesTheCLIState(t *testing.T) {
	for _, row := range []struct {
		raw  string
		want bool
	}{
		{`{"volume":"v2-34071d5f","state":"deleted"}`, true},
		{`{"volume":"v2-34071d5f","state":"deleting"}`, false},
		{`{"completed":true}`, false},
		{`{"state":"deleted"}`, false},
		{`not JSON`, false},
	} {
		if got := handoffDeleteCompleted(row.raw); got != row.want {
			t.Errorf("delete completion for %s: %v, want %v", row.raw, got, row.want)
		}
	}
}
