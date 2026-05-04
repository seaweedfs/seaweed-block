package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseFlags_IscsiPortalAddrRequiresListen(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-portal-addr", "192.168.1.184:3260,1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want --iscsi-portal-addr without --iscsi-listen rejected")
	}
	if !strings.Contains(err.Error(), "--iscsi-portal-addr requires --iscsi-listen") {
		t.Fatalf("error = %q, want portal/listen requirement", err)
	}
}

func TestParseFlags_IscsiPortalAddrDoesNotChangeLoopbackBind(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-listen", "127.0.0.1:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-portal-addr", "192.168.1.184:3260,1",
	)
	got, err := parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got.iscsiPortalAddr != "192.168.1.184:3260,1" {
		t.Fatalf("iscsiPortalAddr = %q", got.iscsiPortalAddr)
	}
	if !got.enableT1Readiness {
		t.Fatal("iSCSI should still auto-enable t1 readiness")
	}
}

func TestParseFlags_IscsiPortalAddrStillRejectsExternalBind(t *testing.T) {
	args := append(requiredBlockvolumeArgs(),
		"--iscsi-listen", "192.168.1.184:3260",
		"--iscsi-iqn", "iqn.2026-05.io.seaweedfs:test-v1",
		"--iscsi-portal-addr", "192.168.1.184:3260,1",
	)
	_, err := parseFlags(args)
	if err == nil {
		t.Fatal("parseFlags succeeded; want external iSCSI bind rejected")
	}
	if !strings.Contains(err.Error(), "not loopback") {
		t.Fatalf("error = %q, want loopback bind rejection", err)
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
