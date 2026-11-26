package main

import (
	"errors"
	"strings"
	"testing"
)

// v1 removed --workspace-head-base-url (blockmaster reads HEAD through the
// layout catalog). A unit or script still passing it is refused at start
// with a typed error that names the replacement, in either flag form; it is
// never accepted and ignored.
func TestRemovedHeadBaseURLFlagIsRefusedAtStart(t *testing.T) {
	base := func(extra ...string) []string {
		return append([]string{"--authority-store", t.TempDir()}, extra...)
	}
	if _, err := parseFlags(base()); err != nil {
		t.Fatalf("control without the removed flag: %v", err)
	}
	for name, args := range map[string][]string{
		"name=value":   base("--workspace-head-base-url=http://filer:8888/swblock/chunks"),
		"name value":   base("--workspace-head-base-url", "http://filer:8888/swblock/chunks"),
		"empty value":  base("--workspace-head-base-url="),
		"single dash":  base("-workspace-head-base-url=http://filer:8888/swblock/chunks"),
		"after others": base("--workspace-rpc", "--workspace-head-base-url=http://filer:8888/swblock/chunks"),
	} {
		_, err := parseFlags(args)
		if !errors.Is(err, errRemovedInV1) {
			t.Fatalf("%s: %v; want the removed-in-v1 refusal", name, err)
		}
		if !strings.Contains(err.Error(), "--workspace-head-base-url") || !strings.Contains(err.Error(), "--chunk-filer") {
			t.Fatalf("%s: %q; want it to name the removed flag and its replacement", name, err)
		}
	}
}
