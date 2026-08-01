package master

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func TestPhase175SnapshotRuntimeClientUsesOperationContextForLongRestore(t *testing.T) {
	client := newSnapshotRuntimeHTTPClient(&tls.Config{MinVersion: tls.VersionTLS12})
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport=%T", client.Transport)
	}
	if transport.ResponseHeaderTimeout != 0 {
		t.Fatalf("response header timeout=%v; long restore must use operation context", transport.ResponseHeaderTimeout)
	}
}
