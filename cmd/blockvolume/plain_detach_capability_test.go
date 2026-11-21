package main

import (
	"reflect"
	"testing"
)

func TestPlainDetachCapabilityFlagIsNoop(t *testing.T) {
	base := []string{"--master", "127.0.0.1:1", "--server-id", "h1", "--volume-id", "v1", "--replica-id", "r1", "--data-addr", "127.0.0.1:2", "--ctrl-addr", "127.0.0.1:3"}
	want, err := parseFlags(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, arg := range []string{"--chunk-detach-plain-supported", "--chunk-detach-plain-supported=true", "--chunk-detach-plain-supported=false"} {
		got, err := parseFlags(append(append([]string{}, base...), arg))
		if err != nil {
			t.Fatalf("capability flag unavailable: %s: %v", arg, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("capability flag changed runtime configuration: %s", arg)
		}
	}
}
