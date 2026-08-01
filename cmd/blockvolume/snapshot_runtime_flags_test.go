package main

import (
	"strings"
	"testing"
)

func TestPhase175SnapshotRuntimeFlagsRequireCompleteSecureConfiguration(t *testing.T) {
	base := phase175RequiredBlockvolumeArgs()
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "partial", args: append(base, "--snapshot-runtime-listen=127.0.0.1:24443"), want: "requires listen, advertise, TLS cert/key, client CA, and token file"},
		{name: "no durable backend", args: append(base,
			"--snapshot-runtime-listen=127.0.0.1:24443",
			"--snapshot-runtime-advertise=https://127.0.0.1:24443",
			"--snapshot-runtime-tls-cert=cert.pem",
			"--snapshot-runtime-tls-key=key.pem",
			"--snapshot-runtime-client-ca=ca.pem",
			"--snapshot-runtime-token-file=token",
		), want: "requires --durable-root"},
		{name: "plain HTTP", args: append(base,
			"--durable-root=/tmp/sw-block",
			"--snapshot-runtime-listen=127.0.0.1:24443",
			"--snapshot-runtime-advertise=http://127.0.0.1:24443",
			"--snapshot-runtime-tls-cert=cert.pem",
			"--snapshot-runtime-tls-key=key.pem",
			"--snapshot-runtime-client-ca=ca.pem",
			"--snapshot-runtime-token-file=token",
		), want: "invalid HTTPS runtime endpoint"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseFlags(tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestPhase175SnapshotRuntimeFlagsAcceptExplicitSecureConfiguration(t *testing.T) {
	args := append(phase175RequiredBlockvolumeArgs(),
		"--durable-root=/tmp/sw-block",
		"--snapshot-runtime-listen=0.0.0.0:24443",
		"--snapshot-runtime-advertise=https://snapshot-r1.example:24443",
		"--snapshot-runtime-tls-cert=/tls/tls.crt",
		"--snapshot-runtime-tls-key=/tls/tls.key",
		"--snapshot-runtime-client-ca=/tls/ca.crt",
		"--snapshot-runtime-token-file=/auth/token",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatal(err)
	}
	if got.snapshotRuntimeAdvertise != "https://snapshot-r1.example:24443" || got.snapshotRuntimeTokenFile != "/auth/token" {
		t.Fatalf("snapshot runtime flags=%+v", got)
	}
}

func phase175RequiredBlockvolumeArgs() []string {
	return []string{
		"--master=127.0.0.1:9333",
		"--server-id=node-a",
		"--volume-id=vol-a",
		"--replica-id=r1",
		"--data-addr=127.0.0.1:19101",
		"--ctrl-addr=127.0.0.1:19102",
	}
}
