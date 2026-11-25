package main

import (
	"os"
	"strings"
	"testing"
)

// A plaintext control listener binds loopback only: --listen in development
// mode, and the runtime HTTP listener always when its API is enabled.
// --insecure-external-bind is refused with a cure line.
func TestParseFlags_PlaintextControlListenersStayOnLoopback(t *testing.T) {
	base := func(extra ...string) []string { return append([]string{"--authority-store", t.TempDir()}, extra...) }
	for _, ok := range []string{"127.0.0.1:9333", "localhost:9333", "[::1]:9333", "127.0.0.1:0"} {
		if _, err := parseFlags(base("--listen", ok)); err != nil {
			t.Fatalf("--listen %s: %v", ok, err)
		}
	}
	for _, bad := range []string{"0.0.0.0:9333", ":9333", "10.0.0.5:9333", "[::]:9333"} {
		_, err := parseFlags(base("--listen", bad))
		if err == nil || !strings.Contains(err.Error(), "not a loopback address") || !strings.Contains(err.Error(), "--tls-cert") {
			t.Fatalf("--listen %s in development mode: %v, want a refusal naming mutual TLS", bad, err)
		}
		if _, err := parseFlags(base("--listen", bad, "--insecure-external-bind")); err == nil || !strings.Contains(err.Error(), "no longer accepted") {
			t.Fatalf("--listen %s with --insecure-external-bind: %v, want refused", bad, err)
		}
	}
	if _, err := parseFlags(base("--frontend-publication-runtime-listen", "0.0.0.0:9334")); err != nil {
		t.Fatalf("runtime listener disabled: its address is not checked: %v", err)
	}
	_, err := parseFlags(base("--frontend-publication-runtime-http", "--frontend-publication-runtime-listen", "0.0.0.0:9334"))
	if err == nil || !strings.Contains(err.Error(), "frontend-publication-runtime-listen") || !strings.Contains(err.Error(), "always stays on loopback") {
		t.Fatalf("enabled runtime listener on 0.0.0.0: %v, want refused", err)
	}
}

// No shipped manifest passes the removed flag or binds blockmaster off
// loopback without mutual TLS.
func TestManifests_NoPlaintextExternalBlockmasterBind(t *testing.T) {
	for _, p := range []string{"../../charts/seaweed-block/templates/blockmaster.yaml", "../../deploy/k8s/alpha/block-stack.yaml", "../../deploy/k8s/g15b/block-stack.yaml", "../../deploy/k8s/g15d/block-stack.yaml"} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		s := string(b)
		if strings.Contains(s, "--insecure-external-bind") {
			t.Fatalf("%s passes --insecure-external-bind, which blockmaster refuses", p)
		}
		if strings.Contains(s, "--listen=0.0.0.0") && !strings.Contains(s, "--tls-cert") {
			t.Fatalf("%s binds blockmaster to 0.0.0.0 without mutual TLS", p)
		}
		if strings.Contains(s, "--tls-cert") && !strings.Contains(s, "--audit-log") {
			t.Fatalf("%s runs blockmaster with mutual TLS but without --audit-log, which it refuses", p)
		}
	}
}
