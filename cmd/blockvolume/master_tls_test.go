package main

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
)

// Phase 0: the volume's master-link TLS flags are checked at start: a node
// certificate whose CN differs from --server-id, a partial set, or TLS with
// --allow-external-status-bind refuses the start with a cure line.
func TestMasterTLSFlags(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	dir := t.TempDir()
	caPath := ca.WriteCA(t, dir)
	good, goodKey := ca.Issue(t, "m02", time.Now().Add(time.Hour)).WriteFiles(t, dir, "m02") // --server-id=m02 in the base args
	other, otherKey := ca.Issue(t, "s9", time.Now().Add(time.Hour)).WriteFiles(t, dir, "s9")
	base := phase150RequiredBlockvolumeArgs()
	for _, c := range []struct {
		name string
		args []string
		want string // "" = accepted
	}{
		{"no TLS (development mode)", nil, ""},
		{"CN equals --server-id", []string{"--master-tls-cert", good, "--master-tls-key", goodKey, "--master-ca", caPath}, ""},
		{"CN differs from --server-id", []string{"--master-tls-cert", other, "--master-tls-key", otherKey, "--master-ca", caPath}, `CN="s9" but --server-id is "m02"`},
		{"no CA", []string{"--master-tls-cert", good, "--master-tls-key", goodKey}, "a certificate, its key and a CA"},
		{"with --allow-external-status-bind", []string{"--master-tls-cert", good, "--master-tls-key", goodKey, "--master-ca", caPath, "--allow-external-status-bind"}, "cannot be combined"},
	} {
		f, err := parseFlags(append(append([]string{}, base...), c.args...))
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: %v; want accepted", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: %v; want an error naming %q", c.name, err, c.want)
		case c.want == "" && c.args != nil:
			if d, err := masterDialTLS(f); err != nil || d == nil {
				t.Errorf("%s: dial option %v %v; want mutual TLS", c.name, d, err)
			}
		}
	}
}
