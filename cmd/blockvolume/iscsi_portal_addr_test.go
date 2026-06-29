package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseFlags_VersionDoesNotRequireVolumeArgs(t *testing.T) {
	got, err := parseFlags([]string{"--version"})
	if err != nil {
		t.Fatalf("parseFlags --version: %v", err)
	}
	if !got.version {
		t.Fatal("version flag not set")
	}
}

func TestParseFlags_IscsiPortalAddrRequiresListen(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-portal-addr", "203.0.113.10:3260,1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want --iscsi-portal-addr without --iscsi-listen rejected")
	}
	if !strings.Contains(err.Error(), "--iscsi-portal-addr requires --iscsi-listen") {
		t.Fatalf("error = %q, want portal/listen requirement", err)
	}
}

func TestParseFlags_NVMeTransportDefaultsTCP(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--nvme-listen", "127.0.0.1:4420",
		"--nvme-subsysnqn", "nqn.2026-05.io.seaweedfs:test-v1",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got.nvmeTransport != "tcp" {
		t.Fatalf("nvmeTransport=%q want tcp", got.nvmeTransport)
	}
}

func TestParseFlags_NVMeTransportRejectsRDMA(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--nvme-listen", "127.0.0.1:4420",
		"--nvme-subsysnqn", "nqn.2026-05.io.seaweedfs:test-v1",
		"--nvme-transport", "rdma",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want rdma transport rejected")
	}
	if !strings.Contains(err.Error(), `--nvme-transport="rdma" unsupported`) {
		t.Fatalf("error = %q, want unsupported transport", err)
	}
}

func TestParseFlags_NVMeExternalBindRequiresExplicitOptIn(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--nvme-listen", "203.0.113.10:4420",
		"--nvme-subsysnqn", "nqn.2026-05.io.seaweedfs:test-v1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want external NVMe/TCP bind rejected")
	}
	if !strings.Contains(err.Error(), "not loopback") {
		t.Fatalf("error = %q, want loopback bind rejection", err)
	}
}

func TestParseFlags_NVMeExternalBindOptIn(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--allow-external-nvme-bind",
		"--nvme-listen", "203.0.113.10:4420",
		"--nvme-subsysnqn", "nqn.2026-05.io.seaweedfs:test-v1",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !got.allowExternalNVMeBind {
		t.Fatal("allowExternalNVMeBind=false")
	}
}

func TestParseFlags_NVMeExternalBindOptInRejectsLoopback(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--allow-external-nvme-bind",
		"--nvme-listen", "127.0.0.1:4420",
		"--nvme-subsysnqn", "nqn.2026-05.io.seaweedfs:test-v1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want loopback external bind rejected")
	}
	if !strings.Contains(err.Error(), "non-loopback") {
		t.Fatalf("error = %q, want non-loopback requirement", err)
	}
}

func TestParseFlags_IscsiPortalAddrDoesNotChangeLoopbackBind(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-listen", "127.0.0.1:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-portal-addr", "203.0.113.10:3260,1",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got.iscsiPortalAddr != "203.0.113.10:3260,1" {
		t.Fatalf("iscsiPortalAddr = %q", got.iscsiPortalAddr)
	}
	if !got.enableT1Readiness {
		t.Fatal("iSCSI should still auto-enable t1 readiness")
	}
}

func TestParseFlags_IscsiPortalAddrStillRejectsExternalBind(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-listen", "203.0.113.10:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-portal-addr", "203.0.113.10:3260,1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want external iSCSI bind rejected")
	}
	if !strings.Contains(err.Error(), "not loopback") {
		t.Fatalf("error = %q, want loopback bind rejection", err)
	}
}

func TestParseFlags_IscsiExternalBindRequiresExplicitOptIn(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-listen", "203.0.113.10:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-chap-username", "user1",
		"--iscsi-chap-secret", "secret1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want explicit external bind opt-in")
	}
	if !strings.Contains(err.Error(), "not loopback") {
		t.Fatalf("error = %q, want loopback bind rejection", err)
	}
}

func TestParseFlags_IscsiExternalBindOptInRequiresCHAP(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--allow-external-iscsi-bind",
		"--iscsi-listen", "203.0.113.10:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want CHAP requirement")
	}
	if !strings.Contains(err.Error(), "requires CHAP") {
		t.Fatalf("error = %q, want CHAP requirement", err)
	}
}

func TestParseFlags_IscsiExternalBindOptInWithCHAP(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--allow-external-iscsi-bind",
		"--iscsi-listen", "203.0.113.10:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-chap-username", "user1",
		"--iscsi-chap-secret", "secret1",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !got.allowExternalISCSIBind {
		t.Fatal("allowExternalISCSIBind=false")
	}
}

func TestParseFlags_IscsiExternalBindOptInRejectsLoopback(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--allow-external-iscsi-bind",
		"--iscsi-listen", "127.0.0.1:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-chap-username", "user1",
		"--iscsi-chap-secret", "secret1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want loopback external bind rejected")
	}
	if !strings.Contains(err.Error(), "non-loopback") {
		t.Fatalf("error = %q, want non-loopback requirement", err)
	}
}

func TestParseFlags_ExternalStatusBindIsExplicitFlag(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--status-addr", "10.0.0.12:23260",
		"--allow-external-status-bind",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !got.allowExternalStatusBind {
		t.Fatal("allowExternalStatusBind=false")
	}
}

func TestParseFlags_RuntimeRebuildEndpointIsExplicitFlag(t *testing.T) {
	f, err := parseFlags([]string{
		"--master", "127.0.0.1:9333",
		"--server-id", "m01",
		"--volume-id", "v1",
		"--replica-id", "r1",
		"--data-addr", "127.0.0.1:19101",
		"--ctrl-addr", "127.0.0.1:19102",
		"--status-addr", "127.0.0.1:23260",
		"--runtime-rebuild-endpoint",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !f.runtimeRebuildEndpoint {
		t.Fatalf("runtimeRebuildEndpoint=false, want true")
	}
}

func TestParseFlags_IscsiDataOutTimeoutRequiresListen(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-dataout-timeout", "5s",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want --iscsi-dataout-timeout without --iscsi-listen rejected")
	}
	if !strings.Contains(err.Error(), "--iscsi-dataout-timeout requires --iscsi-listen") {
		t.Fatalf("error = %q, want dataout/listen requirement", err)
	}
}

func TestParseFlags_IscsiDataOutTimeoutPlumbed(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-listen", "127.0.0.1:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-dataout-timeout", "5s",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got.iscsiDataOutTTL != 5*time.Second {
		t.Fatalf("iscsiDataOutTTL = %s", got.iscsiDataOutTTL)
	}
}

func TestParseFlags_IscsiCHAPRequiresListen(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-chap-username", "user1",
		"--iscsi-chap-secret", "secret1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want CHAP without --iscsi-listen rejected")
	}
	if !strings.Contains(err.Error(), "require --iscsi-listen") {
		t.Fatalf("error = %q, want CHAP/listen requirement", err)
	}
}

func TestParseFlags_IscsiCHAPRequiresUserAndSecret(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-listen", "127.0.0.1:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-chap-username", "user1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want username without secret rejected")
	}
	if !strings.Contains(err.Error(), "must be set together") {
		t.Fatalf("error = %q, want paired CHAP flag requirement", err)
	}
}

func TestParseFlags_IscsiCHAPPlumbed(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-listen", "127.0.0.1:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-chap-username", "user1",
		"--iscsi-chap-secret", "secret1",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got.iscsiCHAPUser != "user1" || got.iscsiCHAPSecret != "secret1" {
		t.Fatalf("CHAP flags = %q/%q", got.iscsiCHAPUser, got.iscsiCHAPSecret)
	}
}

func TestParseFlags_IscsiCHAPFromEnvironment(t *testing.T) {
	t.Setenv("SW_BLOCK_ISCSI_CHAP_USERNAME", "env-user")
	t.Setenv("SW_BLOCK_ISCSI_CHAP_SECRET", "env-secret")
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-listen", "127.0.0.1:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got.iscsiCHAPUser != "env-user" || got.iscsiCHAPSecret != "env-secret" {
		t.Fatalf("CHAP env = %q/%q", got.iscsiCHAPUser, got.iscsiCHAPSecret)
	}
}

func requiredBlockvolumeArgs() []string {
	return []string{
		"--master", "127.0.0.1:9333",
		"--server-id", "s1",
		"--volume-id", "v1",
		"--replica-id", "r1",
		"--data-addr", "127.0.0.1:18080",
		"--ctrl-addr", "127.0.0.1:18081",
	}
}
