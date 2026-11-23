package main

import (
	"strings"
	"testing"
	"time"
)

func TestWorkspaceClockSkewFlag(t *testing.T) {
	for _, v := range []string{"0s", "-1s", "60s", "90s"} {
		if _, err := parseFlags([]string{"--authority-store", "unused", "--workspace-max-clock-skew", v}); err == nil {
			t.Errorf("accepted %s", v)
		}
	}
	for _, v := range []string{"10.2s", "11s", "20s", "59s"} {
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

func TestWorkspaceSkewMustExceedFenceExitBound(t *testing.T) {
	for _, tc := range []struct{ skew, half string }{{"8s", "4s"}, {"10.1s", "5.05s"}} {
		t.Run(tc.skew, func(t *testing.T) {
			_, err := parseFlags([]string{"--authority-store", "unused", "--workspace-max-clock-skew", tc.skew})
			if err == nil {
				t.Fatalf("accepted skew %s whose half does not exceed fence exit", tc.skew)
			}
			for _, want := range []string{"--workspace-max-clock-skew=" + tc.skew, "half-skew=" + tc.half, "configured fence-exit budget=5.05s", "watch cadence=50ms", "drain timeout=5s", "assuming normal scheduling", "increase", "10.1s"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q lacks %q", err, want)
				}
			}
		})
	}
	if _, err := parseFlags([]string{"--authority-store", "unused"}); err != nil {
		t.Fatalf("20s default refused: %v", err)
	}
}
