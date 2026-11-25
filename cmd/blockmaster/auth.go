package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc"
)

// authFlags are blockmaster's phase 0 authentication flags: its own TLS
// certificate and key, the CA every client certificate must verify against,
// and the role file (fingerprint -> role, id). All four or none; none is
// development mode (plaintext, loopback only).
type authFlags struct {
	tlsCert, tlsKey, tlsClientCA, roleFile string
	auditLog                               string // append-only audit log (optional)
	// connectionAge is how long a client connection lives before blockmaster
	// asks for a reconnect (a fresh handshake); 24 h, shorter only in tests.
	connectionAge time.Duration
}

func (a authFlags) enabled() bool {
	return a.tlsCert != "" || a.tlsKey != "" || a.tlsClientCA != "" || a.roleFile != ""
}

func (a authFlags) files() link.TLSFiles {
	return link.TLSFiles{Cert: a.tlsCert, Key: a.tlsKey, CA: a.tlsClientCA}
}

// validate refuses a partial set, and the plaintext escape hatch together
// with authentication.
func (a authFlags) validate(insecureExternalBind bool) error {
	if a.auditLog != "" && !a.enabled() {
		return fmt.Errorf("--audit-log needs authentication (--tls-cert, --tls-key, --tls-client-ca, --role-file)")
	}
	if !a.enabled() {
		return nil
	}
	if a.tlsCert == "" || a.tlsKey == "" || a.tlsClientCA == "" || a.roleFile == "" {
		return fmt.Errorf("authentication needs --tls-cert, --tls-key, --tls-client-ca and --role-file together")
	}
	if a.auditLog == "" {
		return fmt.Errorf("authentication needs --audit-log: every changing call is recorded before it runs, and is refused when it cannot be")
	}
	if insecureExternalBind {
		return fmt.Errorf("--insecure-external-bind cannot be combined with mutual TLS: the authenticated listener may bind any address; drop the flag")
	}
	return nil
}

// build returns the authenticator and the server options of an
// authenticated master, or nil and none in development mode. Every file is
// read here, so a wrong one refuses the start.
func (a authFlags) build() (*auth.Authenticator, []grpc.ServerOption, error) {
	if !a.enabled() {
		return nil, nil, nil
	}
	creds, err := link.ServerCredentials(a.files())
	if err != nil {
		return nil, nil, err
	}
	table, err := auth.LoadRoleFile(a.roleFile)
	if err != nil {
		return nil, nil, err
	}
	authn := auth.NewAuthenticator(table, nil)
	if a.auditLog != "" {
		l, _, err := auth.OpenAuditLog(a.auditLog)
		if err != nil {
			return nil, nil, err
		}
		authn.SetAudit(l)
	}
	age := a.connectionAge
	if age <= 0 {
		age = link.MasterConnectionAge
	}
	return authn, []grpc.ServerOption{creds, link.ConnectionAge(age)}, nil
}

// certDaysLeft is the days to expiry of the certificate file at path.
func certDaysLeft(certPath, keyPath string, now time.Time) (int, time.Time, error) {
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return 0, time.Time{}, err
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return 0, time.Time{}, err
	}
	return int(leaf.NotAfter.Sub(now).Hours() / 24), leaf.NotAfter, nil
}

// reloadRolesOnSIGHUP reloads the role file at every SIGHUP; a malformed or
// unreadable file keeps the current table and says so. Removing a line
// revokes that certificate at the reload (its open streams end).
func reloadRolesOnSIGHUP(a *auth.Authenticator, path string) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if err := a.Reload(path); err != nil {
				fmt.Fprintf(os.Stderr, "blockmaster: role file %s NOT reloaded, the previous table stays: %v\n", path, err)
				continue
			}
			fmt.Fprintf(os.Stderr, "blockmaster: role file %s reloaded\n", path)
		}
	}()
}

// warnUnauthenticated repeats the development-mode warning once a minute.
func warnUnauthenticated(listen string) {
	msg := fmt.Sprintf("blockmaster: UNAUTHENTICATED CONTROL API on %s (development mode)", listen)
	fmt.Fprintln(os.Stderr, msg)
	go func() {
		for range time.Tick(time.Minute) {
			fmt.Fprintln(os.Stderr, msg)
		}
	}()
}
