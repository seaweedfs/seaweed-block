package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvmerdma"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
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

func TestParseFlags_NVMeTransportAcceptsExplicitRDMA(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--nvme-listen", "10.0.0.3:4420",
		"--nvme-subsysnqn", "nqn.2026-05.io.seaweedfs:test-v1",
		"--nvme-transport", "rdma",
		"--allow-external-nvme-bind",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got.nvmeTransport != "rdma" {
		t.Fatalf("nvmeTransport=%q want rdma", got.nvmeTransport)
	}
}

func TestParseFlags_NVMERDMARejectsTCPMaxH2COption(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--nvme-listen", "10.0.0.3:4420",
		"--nvme-subsysnqn", "nqn.2026-05.io.seaweedfs:test-v1",
		"--nvme-transport", "rdma",
		"--allow-external-nvme-bind",
		"--nvme-max-h2c-data-length", "65536",
	)
	_, err := parseFlags(args)
	if err == nil || !strings.Contains(err.Error(), "applies only to --nvme-transport=tcp") {
		t.Fatalf("error=%v want TCP-only MaxH2C refusal", err)
	}
}

func TestNVMERDMAStandaloneTargetIsNotPublishedToCSI(t *testing.T) {
	if shouldPublishNVMeFrontendTarget("rdma") {
		t.Fatal("standalone RDMA target must not enter master/CSI publish context")
	}
	if !shouldPublishNVMeFrontendTarget("tcp") {
		t.Fatal("TCP target publication changed")
	}
}

func TestNVMeFrontendCapabilitiesExposeRDMAUnsupportedNoListener(t *testing.T) {
	facts := []volume.FrontendTransportPreflightFact{{
		Name:      "nvme_rdma_module",
		Available: false,
		Reason:    "nvme_rdma_module_missing",
	}}
	caps := nvmeFrontendCapabilitiesWithRDMAPreflight("127.0.0.1:4420", "tcp", true, facts)
	if len(caps) != 2 {
		t.Fatalf("capabilities=%d want 2", len(caps))
	}
	tcp := caps[0]
	if tcp.Protocol != "nvme" || tcp.Transport != "tcp" || !tcp.Supported || !tcp.ListenerImplemented || !tcp.ListenerStarted || !tcp.StartAllowed {
		t.Fatalf("tcp capability unexpected: %+v", tcp)
	}
	rdma := caps[1]
	if rdma.Protocol != "nvme" || rdma.Transport != "rdma" || rdma.Supported || rdma.ListenerImplemented != nvmerdma.Implemented() || rdma.ListenerStarted || rdma.StartAllowed {
		t.Fatalf("rdma capability must stay unsupported with no listener: %+v", rdma)
	}
	if rdma.Reason != "nvme_rdma_transport_unsupported" {
		t.Fatalf("rdma reason=%q want nvme_rdma_transport_unsupported", rdma.Reason)
	}
	if rdma.StartReason != "nvme_rdma_listener_disabled" {
		t.Fatalf("rdma startReason=%q want nvme_rdma_listener_disabled", rdma.StartReason)
	}
	if len(rdma.Preflight) != 1 || rdma.Preflight[0].Name != "nvme_rdma_module" || rdma.Preflight[0].Available {
		t.Fatalf("rdma preflight unexpected: %+v", rdma.Preflight)
	}
}

func TestNVMERDMAListenerStartDecisionDisabledByDefault(t *testing.T) {
	got := nvmeRDMAListenerStartDecision(false, nil)
	if got.allowed || got.reason != "nvme_rdma_listener_disabled" {
		t.Fatalf("start decision=%+v want disabled refusal", got)
	}
}

func TestNVMERDMAListenerStartDecisionMapsPreflightFailure(t *testing.T) {
	got := nvmeRDMAListenerStartDecision(true, []volume.FrontendTransportPreflightFact{{
		Name:      "rdma_device",
		Available: false,
		Reason:    "rdma_device_missing",
	}})
	if got.allowed || got.reason != "rdma_device_missing" {
		t.Fatalf("start decision=%+v want rdma_device_missing", got)
	}
}

func TestNVMERDMAListenerStartDecisionMapsEveryTargetPreflightFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
	}{
		{name: "nvmet_rdma_module", reason: "nvmet_rdma_module_missing"},
		{name: "nbd_module", reason: "nbd_module_missing"},
		{name: "rdma_device", reason: "rdma_device_missing"},
		{name: "rdma_bind_address", reason: "rdma_bind_address_invalid"},
		{name: "configfs", reason: "configfs_missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := nvmeRDMAListenerStartDecision(true, []volume.FrontendTransportPreflightFact{{
				Name:      tc.name,
				Available: false,
				Reason:    tc.reason,
			}})
			if got.allowed || got.reason != tc.reason {
				t.Fatalf("start decision=%+v want %s", got, tc.reason)
			}
		})
	}
}

func TestNVMERDMATargetStartFailureReason(t *testing.T) {
	for _, tc := range []struct {
		message string
		want    string
	}{
		{message: "configure kernel target: port ID already exists: 4420", want: "rdma_port_conflict"},
		{message: "configure kernel target: subsystem already exists: nqn.test", want: "rdma_subsystem_conflict"},
		{message: "symlink: cannot assign requested address", want: "rdma_bind_address_unassigned"},
		{message: "open /dev/nbd0: permission denied", want: "rdma_target_permission_denied"},
		{message: "nvmerdma: kernel target requires root", want: "rdma_target_permission_denied"},
		{message: "unexpected target error", want: "rdma_target_start_failed"},
	} {
		if got := nvmeRDMATargetStartFailureReason(errors.New(tc.message)); got != tc.want {
			t.Errorf("failure reason for %q = %q, want %q", tc.message, got, tc.want)
		}
	}
}

func TestNVMERDMAListenerStartDecisionDoesNotRequireInitiatorModule(t *testing.T) {
	got := nvmeRDMAListenerStartDecision(true, []volume.FrontendTransportPreflightFact{
		{
			Name:      "nvme_rdma_module",
			Available: false,
			Reason:    "nvme_rdma_module_missing",
		},
		{
			Name:      "nvmet_rdma_module",
			Available: true,
			Reason:    "nvmet_rdma_module_available",
		},
	})
	if nvmerdma.Implemented() {
		if !got.allowed || got.reason != "implemented" {
			t.Fatalf("start decision=%+v want target allowed without initiator module", got)
		}
	} else if got.allowed || got.reason != "nvme_rdma_transport_unsupported" {
		t.Fatalf("start decision=%+v want unsupported platform", got)
	}
}

func TestNVMERDMAListenerStartDecisionAfterPreflight(t *testing.T) {
	got := nvmeRDMAListenerStartDecision(true, []volume.FrontendTransportPreflightFact{{
		Name:      "nvme_rdma_module",
		Available: true,
		Reason:    "nvme_rdma_module_loaded",
	}, {
		Name:      "rdma_device",
		Available: true,
		Reason:    "rdma_device_present",
	}, {
		Name:      "rdma_bind_address",
		Available: true,
		Reason:    "rdma_bind_address_candidate",
	}})
	if nvmerdma.Implemented() {
		if !got.allowed || got.reason != "implemented" {
			t.Fatalf("start decision=%+v want implemented", got)
		}
	} else if got.allowed || got.reason != "nvme_rdma_transport_unsupported" {
		t.Fatalf("start decision=%+v want unsupported platform", got)
	}
}

func TestRDMABindAddressFactRejectsLoopback(t *testing.T) {
	got := rdmaBindAddressFact("127.0.0.1:4420")
	if got.Name != "rdma_bind_address" || got.Available || got.Reason != "rdma_bind_address_invalid" {
		t.Fatalf("bind fact=%+v want unavailable rdma_bind_address_invalid", got)
	}
}

func TestRDMABindAddressFactAcceptsNonLoopbackCandidate(t *testing.T) {
	got := rdmaBindAddressFact("192.168.100.10:4420")
	if got.Name != "rdma_bind_address" || !got.Available || got.Reason != "rdma_bind_address_candidate" || got.Detail != "192.168.100.10" {
		t.Fatalf("bind fact=%+v want non-loopback candidate", got)
	}
}

func TestParseFlags_NVMeMaxH2CDataLengthRequiresNVMeListen(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--nvme-max-h2c-data-length", "65536",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want --nvme-max-h2c-data-length without --nvme-listen rejected")
	}
	if !strings.Contains(err.Error(), "--nvme-max-h2c-data-length requires --nvme-listen") {
		t.Fatalf("error = %q, want requires nvme-listen", err)
	}
}

func TestParseFlags_NVMeMaxH2CDataLengthAcceptsCandidate(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--nvme-listen", "127.0.0.1:4420",
		"--nvme-subsysnqn", "nqn.2026-05.io.seaweedfs:test-v1",
		"--nvme-max-h2c-data-length", "65536",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got.nvmeMaxH2C != 65536 {
		t.Fatalf("nvmeMaxH2C=%d want 65536", got.nvmeMaxH2C)
	}
}

func TestParseFlags_NVMeMaxH2CDataLengthRejectsInvalid(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--nvme-listen", "127.0.0.1:4420",
		"--nvme-subsysnqn", "nqn.2026-05.io.seaweedfs:test-v1",
		"--nvme-max-h2c-data-length", "49152",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want invalid H2C size rejected")
	}
	if !strings.Contains(err.Error(), "--nvme-max-h2c-data-length=49152 invalid") {
		t.Fatalf("error = %q, want invalid H2C size", err)
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
