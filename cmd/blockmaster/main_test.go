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

func TestParseFlags_LifecyclePlacementSeedOptional(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--lifecycle-store", "lifecycle-dir",
		"--lifecycle-placement-seed", "seed.json",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.lifecyclePlacementSeed != "seed.json" {
		t.Fatalf("lifecyclePlacementSeed=%q want seed.json", f.lifecyclePlacementSeed)
	}
}

func TestParseFlags_ClusterSpecOptional(t *testing.T) {
	f, err := parseFlags([]string{
		"--authority-store", "authority-dir",
		"--lifecycle-store", "lifecycle-dir",
		"--cluster-spec", "m01.yaml",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.clusterSpec != "m01.yaml" {
		t.Fatalf("clusterSpec=%q want m01.yaml", f.clusterSpec)
	}
}
