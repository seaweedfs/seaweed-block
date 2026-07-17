package nvme

import (
	"net"
	"testing"
)

func TestNewSessionUsesTCPPDUTransport(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	s := newSession(server, nil, nil, "", nil)
	if _, ok := s.wire.(*tcpPDUTransport); !ok {
		t.Fatalf("session wire=%T want *tcpPDUTransport", s.wire)
	}
	if s.conn != server {
		t.Fatal("session must keep the original connection for shutdown")
	}
}

func TestTCPPDUTransportImplementsSessionTransport(t *testing.T) {
	var _ sessionTransport = (*tcpPDUTransport)(nil)
}
