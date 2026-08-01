package main

import (
	"os"
	"path/filepath"
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

func TestPhase175RestoreFlagRequiresSecureRuntime(t *testing.T) {
	args := append(phase175RequiredBlockvolumeArgs(),
		"--durable-root=/tmp/sw-block",
		"--restore-snapshot-id=snap-a",
	)
	if _, err := parseFlags(args); err == nil || !strings.Contains(err.Error(), "requires the authenticated snapshot runtime") {
		t.Fatalf("error=%v", err)
	}
	args = append(args,
		"--snapshot-runtime-listen=0.0.0.0:24443",
		"--snapshot-runtime-advertise=https://snapshot-r1.example:24443",
		"--snapshot-runtime-tls-cert=/tls/tls.crt",
		"--snapshot-runtime-tls-key=/tls/tls.key",
		"--snapshot-runtime-client-ca=/tls/ca.crt",
		"--snapshot-runtime-token-file=/auth/token",
	)
	got, err := parseFlags(args)
	if err != nil || got.restoreSnapshotID != "snap-a" {
		t.Fatalf("flags=%+v error=%v", got, err)
	}
}

func TestPhase175PrepareRestoreTargetCreatesMarkerBeforeDataAndResumes(t *testing.T) {
	root := t.TempDir()
	f := flags{
		durableRoot:           root,
		volumeID:              "target-vol",
		replicaID:             "r2",
		restoreSnapshotID:     "snap-a",
		snapshotRuntimeListen: "127.0.0.1:24443",
	}
	dataPath := filepath.Join(root, "target-vol.bin")
	target, err := prepareRestoreTarget(f, restorePathStub(dataPath))
	if err != nil {
		t.Fatal(err)
	}
	if target.Marker().State != "pending" {
		t.Fatalf("marker=%+v", target.Marker())
	}
	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Fatalf("restore preparation created target data: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "target-vol.restore.json")); err != nil {
		t.Fatal(err)
	}

	f.restoreSnapshotID = ""
	resumed, err := prepareRestoreTarget(f, restorePathStub(dataPath))
	if err != nil || resumed.Marker().SnapshotID != "snap-a" {
		t.Fatalf("resumed=%v error=%v", resumed, err)
	}
	f.snapshotRuntimeListen = ""
	if _, err := prepareRestoreTarget(f, restorePathStub(dataPath)); err == nil || !strings.Contains(err.Error(), "unfinished restore") {
		t.Fatalf("error=%v", err)
	}
}

type restorePathStub string

func (p restorePathStub) VolumeDataPath(string) (string, error) { return string(p), nil }

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
