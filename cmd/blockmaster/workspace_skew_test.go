package main

import (
	"testing"
	"time"
)

func TestWorkspaceClockSkewFlag(t *testing.T) {
	for _, v := range []string{"0s", "-1s", "60s", "90s"} {
		if _, err := parseFlags([]string{"--authority-store", "unused", "--workspace-max-clock-skew", v}); err == nil {
			t.Errorf("accepted %s", v)
		}
	}
	for _, v := range []string{"1s", "20s", "59s"} {
		f, err := parseFlags([]string{"--authority-store", "unused", "--workspace-max-clock-skew", v})
		want, _ := time.ParseDuration(v)
		if err != nil || f.workspaceMaxClockSkew != want {
			t.Fatal(v, f.workspaceMaxClockSkew, err)
		}
	}
	f, err := parseFlags([]string{"--authority-store", "unused"})
	if err != nil || f.workspaceMaxClockSkew != 20*time.Second {
		t.Fatal("default", err)
	}
}
