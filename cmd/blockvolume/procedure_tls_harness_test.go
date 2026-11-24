//go:build subprocess && chunkstore_integration

package main_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
)

// procedureTLS is the procedure harness's mutual-TLS setup: one CA, the
// master's certificate, one node certificate per server id (CN = server id),
// an admin and an operator certificate, and the role file. With it the
// harness runs blockmaster with --tls-*/--role-file, every blockvolume with
// --master-tls-*, and every sw-block command with SW_BLOCK_TLS_* (admin).
type procedureTLS struct {
	dir      string
	ca       *authtest.CA
	roles    string
	notAfter map[string]time.Time // per certificate name, when set
}

// procedureTLSOn reports whether the procedure gate runs on the TLS binaries
// (T14): SW_BLOCK_PROCEDURE_TLS=1.
func procedureTLSOn() bool { return os.Getenv("SW_BLOCK_PROCEDURE_TLS") == "1" }

func (p *procedureTLS) path(name, ext string) string { return filepath.Join(p.dir, name+ext) }

// issue writes name.crt/name.key for CN cn and adds its role-file line.
func (p *procedureTLS) issue(t *testing.T, name, cn, role string, notAfter time.Time) {
	t.Helper()
	leaf := p.ca.Issue(t, cn, notAfter)
	leaf.WriteFiles(t, p.dir, name)
	if role != "" {
		p.addRole(t, leaf.Fingerprint, role, cn)
	}
}

func (p *procedureTLS) addRole(t *testing.T, fingerprint, role, id string) {
	t.Helper()
	f, err := os.OpenFile(p.roles, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, fingerprint, role, id); err != nil {
		t.Fatal(err)
	}
}

// removeRole drops id's line from the role file (a revocation before SIGHUP
// or a restart).
func (p *procedureTLS) removeRole(t *testing.T, id string) {
	t.Helper()
	raw, err := os.ReadFile(p.roles)
	if err != nil {
		t.Fatal(err)
	}
	var keep []string
	for _, l := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if f := strings.Fields(l); len(f) == 3 && f[2] == id {
			continue
		}
		keep = append(keep, l)
	}
	if err := os.WriteFile(p.roles, []byte(strings.Join(keep, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// newProcedureTLS issues the master, admin and operator certificates and one
// node certificate per server id (notAfter overrides a node's expiry).
func newProcedureTLS(t *testing.T, dir string, servers []string, notAfter map[string]time.Time) *procedureTLS {
	t.Helper()
	p := &procedureTLS{dir: filepath.Join(dir, "tls"), notAfter: notAfter}
	if err := os.MkdirAll(p.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	p.roles = p.path("roles", "")
	p.ca = authtest.NewCA(t, "procedure CA")
	p.ca.WriteCA(t, p.dir)
	day := time.Now().Add(24 * time.Hour)
	p.issue(t, "master", "blockmaster", "", day)
	p.issue(t, "admin", "procedure-admin", "admin", day)
	p.issue(t, "operator", "procedure-operator", "operator", day)
	seen := map[string]bool{}
	for _, s := range servers {
		if seen[s] {
			continue
		}
		seen[s] = true
		exp := day
		if at, ok := notAfter[s]; ok {
			exp = at
		}
		p.issue(t, s, s, "node", exp)
	}
	return p
}

func (p *procedureTLS) masterArgs() []string {
	return []string{"--tls-cert", p.path("master", ".crt"), "--tls-key", p.path("master", ".key"),
		"--tls-client-ca", p.path("ca", ".crt"), "--role-file", p.roles}
}

// volumeArgs is the node identity of server for blockvolume.
func (p *procedureTLS) volumeArgs(server string) []string {
	return []string{"--master-tls-cert", p.path(server, ".crt"), "--master-tls-key", p.path(server, ".key"), "--master-ca", p.path("ca", ".crt")}
}

// cliEnv is the identity name (admin, operator, a server id) for sw-block.
func (p *procedureTLS) cliEnv(name string) []string {
	return []string{"SW_BLOCK_TLS_CERT=" + p.path(name, ".crt"), "SW_BLOCK_TLS_KEY=" + p.path(name, ".key"), "SW_BLOCK_TLS_CA=" + p.path("ca", ".crt")}
}
