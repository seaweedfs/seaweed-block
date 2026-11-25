package main

import (
	"strings"
	"testing"
)

// Phase 0 flags: the four authentication flags go together; with them the
// listener may bind any address; without them the control listener stays
// loopback-only; --insecure-external-bind is refused either way; the
// launcher's node TLS needs blockmaster's own and no external status.
func TestAuthFlags(t *testing.T) {
	base := []string{"--authority-store", t.TempDir()}
	all := []string{"--tls-cert", "c.pem", "--tls-key", "k.pem", "--tls-client-ca", "ca.pem", "--role-file", "roles", "--audit-log", "audit.log"}
	for _, c := range []struct {
		name string
		args []string
		want string // "" = accepted
	}{
		{"development mode, loopback", []string{"--listen", "127.0.0.1:9180"}, ""},
		{"development mode, off loopback", []string{"--listen", "192.0.2.5:9180"}, "not a loopback address"},
		{"authenticated, off loopback", append([]string{"--listen", "192.0.2.5:9180"}, all...), ""},
		{"partial: no role file", []string{"--listen", "192.0.2.5:9180", "--tls-cert", "c.pem", "--tls-key", "k.pem", "--tls-client-ca", "ca.pem"}, "together"},
		{"partial: role file only", []string{"--role-file", "roles"}, "together"},
		{"audit log without authentication", []string{"--audit-log", "audit.log"}, "needs authentication"},
		{"authentication without an audit log", []string{"--listen", "192.0.2.5:9180", "--tls-cert", "c.pem", "--tls-key", "k.pem", "--tls-client-ca", "ca.pem", "--role-file", "roles"}, "needs --audit-log"},
		{"authenticated with --insecure-external-bind", append([]string{"--listen", "192.0.2.5:9180", "--insecure-external-bind"}, all...), "no longer accepted"},
		{"development mode with --insecure-external-bind", []string{"--listen", "192.0.2.5:9180", "--insecure-external-bind"}, "no longer accepted"},
		{"launcher node TLS without blockmaster TLS", []string{"--launcher-master-tls-secret-prefix", "sw-block-node-tls"}, "needs blockmaster's own mutual TLS"},
		{"launcher node TLS", append([]string{"--launcher-master-tls-secret-prefix", "sw-block-node-tls"}, all...), ""},
		{"launcher node TLS with external status", append([]string{"--launcher-master-tls-secret-prefix", "sw-block-node-tls", "--launcher-external-status"}, all...), "cannot be combined"},
	} {
		_, err := parseFlags(append(append([]string{}, base...), c.args...))
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: %v; want accepted", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: %v; want an error naming %q", c.name, err, c.want)
		}
	}
}
