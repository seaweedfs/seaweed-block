package main

import "testing"

// --durable-impl defaults to empty (auto): new volumes are walstore, and an
// existing volume opens with its on-disk format. smartwal is experimental
// and only used when chosen explicitly or found on disk.
func TestBlockvolumeDurableImplDefaultIsAuto(t *testing.T) {
	f, err := parseFlags(phase150RequiredBlockvolumeArgs())
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.durableImpl != "" {
		t.Fatalf("durableImpl default=%q, want empty (auto)", f.durableImpl)
	}
}

func TestBlockvolumeDurableImplExplicitValues(t *testing.T) {
	for _, impl := range []string{"walstore", "smartwal", "parallel-walstore"} {
		args := append(phase150RequiredBlockvolumeArgs(), "--durable-root=/tmp/sw-block", "--durable-impl="+impl)
		f, err := parseFlags(args)
		if err != nil {
			t.Fatalf("--durable-impl=%s: %v", impl, err)
		}
		if f.durableImpl != impl {
			t.Fatalf("durableImpl=%q, want %q", f.durableImpl, impl)
		}
	}
	args := append(phase150RequiredBlockvolumeArgs(), "--durable-impl=bogus")
	if _, err := parseFlags(args); err == nil {
		t.Fatal("--durable-impl=bogus accepted")
	}
}
