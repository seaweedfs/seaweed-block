package main

import "testing"

func TestParseFlags_LifecycleStoreOptional(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--lifecycle-store", "lifecycle-dir",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.lifecycleStore != "lifecycle-dir" {
		t.Fatalf("lifecycleStore=%q want lifecycle-dir", f.lifecycleStore)
	}
}
