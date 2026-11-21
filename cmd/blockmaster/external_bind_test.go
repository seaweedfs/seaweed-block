package main

import (
	"os"
	"strings"
	"testing"
)

// The unauthenticated control listeners bind loopback unless
// --insecure-external-bind is given: --listen always, and the runtime HTTP
// listener when its API is enabled.
func TestParseFlags_ControlListenersNeedInsecureExternalBind(t *testing.T) {
	base := func(extra ...string) []string { return append([]string{"--authority-store", t.TempDir()}, extra...) }
	for _, ok := range []string{"127.0.0.1:9333", "localhost:9333", "[::1]:9333", "127.0.0.1:0"} {
		if _, err := parseFlags(base("--listen", ok)); err != nil {
			t.Fatalf("--listen %s: %v", ok, err)
		}
	}
	for _, bad := range []string{"0.0.0.0:9333", ":9333", "10.0.0.5:9333", "[::]:9333"} {
		_, err := parseFlags(base("--listen", bad))
		if err == nil || !strings.Contains(err.Error(), "--insecure-external-bind") {
			t.Fatalf("--listen %s without the opt-in: %v, want a refusal naming --insecure-external-bind", bad, err)
		}
		if _, err := parseFlags(base("--listen", bad, "--insecure-external-bind")); err != nil {
			t.Fatalf("--listen %s with --insecure-external-bind: %v", bad, err)
		}
	}
	if _, err := parseFlags(base("--frontend-publication-runtime-listen", "0.0.0.0:9334")); err != nil {
		t.Fatalf("runtime listener disabled: its address is not checked: %v", err)
	}
	_, err := parseFlags(base("--frontend-publication-runtime-http", "--frontend-publication-runtime-listen", "0.0.0.0:9334"))
	if err == nil || !strings.Contains(err.Error(), "frontend-publication-runtime-listen") {
		t.Fatalf("enabled runtime listener on 0.0.0.0 without the opt-in: %v", err)
	}
	if _, err := parseFlags(base("--frontend-publication-runtime-http", "--frontend-publication-runtime-listen", "0.0.0.0:9334", "--insecure-external-bind")); err != nil {
		t.Fatalf("with the opt-in: %v", err)
	}
}

// Every shipped manifest that binds blockmaster to 0.0.0.0 carries the
// explicit opt-in, so the change refuses nothing that ships.
func TestManifests_ExternalBlockmasterBindCarriesTheOptIn(t *testing.T) {
	for _, p := range []string{"../../charts/seaweed-block/templates/blockmaster.yaml", "../../deploy/k8s/alpha/block-stack.yaml", "../../deploy/k8s/g15b/block-stack.yaml", "../../deploy/k8s/g15d/block-stack.yaml"} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		s := string(b)
		if strings.Contains(s, "--listen=0.0.0.0") && !strings.Contains(s, "--insecure-external-bind") {
			t.Fatalf("%s binds blockmaster to 0.0.0.0 without --insecure-external-bind", p)
		}
	}
}
