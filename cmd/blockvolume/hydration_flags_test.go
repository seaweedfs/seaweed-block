package main

import "testing"

func TestChunkHydrationFlagsAreConfigurable(t *testing.T) {
	base := append(phase150RequiredBlockvolumeArgs(), "--durable-root=hydration-test", "--durable-impl=walstore-chunk", "--chunk-filer=x:1")
	if _, err := parseFlags(append(base, "--chunk-hydration-workers=3", "--chunk-hydration-queue-depth=17")); err != nil {
		t.Fatalf("valid hydration limits refused: %v", err)
	}
	for _, flag := range []string{"--chunk-hydration-workers=0", "--chunk-hydration-workers=65", "--chunk-hydration-queue-depth=0", "--chunk-hydration-queue-depth=65537"} {
		if _, err := parseFlags(append(base, flag)); err == nil {
			t.Fatalf("invalid hydration limit accepted: %s", flag)
		}
	}
}
