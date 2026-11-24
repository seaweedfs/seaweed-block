package wsclient

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Refusals by authentication or authorization are "not authorized", and a
// plaintext client against a TLS master is told the cure; a transport error
// with a client identity configured stays unknown.
func TestClassifyNotAuthorized(t *testing.T) {
	preface := status.Error(codes.Unavailable, `connection error: desc = "error reading server preface: EOF"`)
	for _, c := range []struct {
		name    string
		err     error
		tlsEnv  bool
		want    Outcome
		message string
	}{
		{"unauthenticated", status.Error(codes.Unauthenticated, "certificate sha256:ab (CN=x) is not in the role file"), false, OutcomeNotAuthorized, "role file"},
		{"permission denied", status.Error(codes.PermissionDenied, "operator alice may not do this: it needs admin"), false, OutcomeNotAuthorized, "needs admin"},
		{"plaintext against TLS", preface, false, OutcomeNotAuthorized, link.EnvTLSCert},
		{"preface error with an identity configured", preface, true, OutcomeUnknown, ""},
		{"master down", status.Error(codes.Unavailable, "connection refused"), false, OutcomeUnknown, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := ""
			if c.tlsEnv {
				v = "/some/cert.pem"
			}
			t.Setenv(link.EnvTLSCert, v)
			err := classify("show", c.err)
			if got := OutcomeOf(err); got != c.want {
				t.Fatalf("outcome %v; want %v (%v)", got, c.want, err)
			}
			if c.message != "" && !strings.Contains(err.Error(), c.message) {
				t.Fatalf("message %q does not name %q", err, c.message)
			}
		})
	}
}
