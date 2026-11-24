package link

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
)

// A tool (sw-block) takes its master identity from SW_BLOCK_TLS_CERT/_KEY/_CA:
// none is plaintext, a partial set or a wrong file is refused before a dial.
func TestMasterTLSFromEnv(t *testing.T) {
	dir := t.TempDir()
	ca := authtest.NewCA(t, "ca")
	caPath := ca.WriteCA(t, dir)
	cert, key := ca.Issue(t, "alice", time.Now().Add(time.Hour)).WriteFiles(t, dir, "alice")

	for _, c := range []struct {
		name          string
		cert, key, ca string
		want          string // "" plaintext, "tls", or a refusal substring
	}{
		{name: "none"},
		{name: "all three", cert: cert, key: key, ca: caPath, want: "tls"},
		{name: "no CA", cert: cert, key: key, want: "needs a certificate, its key and a CA"},
		{name: "missing key file", cert: cert, key: key + ".gone", ca: caPath, want: "key"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(EnvTLSCert, c.cert)
			t.Setenv(EnvTLSKey, c.key)
			t.Setenv(EnvTLSCA, c.ca)
			d, err := MasterTLSFromEnv()
			switch c.want {
			case "":
				if err != nil || d != nil {
					t.Fatalf("no variables: got option=%v err=%v, want plaintext", d != nil, err)
				}
			case "tls":
				if err != nil || d == nil {
					t.Fatalf("all three: got option=%v err=%v, want mutual TLS", d != nil, err)
				}
			default:
				if err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), EnvTLSCert) {
					t.Fatalf("got %v, want a refusal naming %s and containing %q", err, EnvTLSCert, c.want)
				}
				if _, err := DialMasterFromEnv("127.0.0.1:1"); err == nil {
					t.Fatal("DialMasterFromEnv dialed with a bad identity")
				}
			}
		})
	}
}
