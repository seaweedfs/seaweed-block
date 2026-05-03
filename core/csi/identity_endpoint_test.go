package csi

import (
	"context"
	"testing"
)

func TestIdentity_NilRequests(t *testing.T) {
	s := NewIdentityServer()
	if _, err := s.GetPluginInfo(context.Background(), nil); err != nil {
		t.Fatalf("GetPluginInfo(nil): %v", err)
	}
	if _, err := s.GetPluginCapabilities(context.Background(), nil); err != nil {
		t.Fatalf("GetPluginCapabilities(nil): %v", err)
	}
	if _, err := s.Probe(context.Background(), nil); err != nil {
		t.Fatalf("Probe(nil): %v", err)
	}
}

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		in      string
		network string
		addr    string
		wantErr bool
	}{
		{in: "unix:///csi/csi.sock", network: "unix", addr: "/csi/csi.sock"},
		{in: "tcp://127.0.0.1:50051", network: "tcp", addr: "127.0.0.1:50051"},
		{in: "http://127.0.0.1:50051", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tt := range tests {
		network, addr, err := ParseEndpoint(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("ParseEndpoint(%q) expected error", tt.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParseEndpoint(%q): %v", tt.in, err)
		}
		if network != tt.network || addr != tt.addr {
			t.Fatalf("ParseEndpoint(%q)=(%q,%q), want (%q,%q)", tt.in, network, addr, tt.network, tt.addr)
		}
	}
}
