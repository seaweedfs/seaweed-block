package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc"
)

// checkMasterTLS checks the master link's mutual-TLS flags at start (phase
// 0): all three files load; the node certificate's CN equals --server-id
// (one node certificate per server id; the master enforces the same rule
// through the role file); and an authenticated volume never listens off
// loopback.
func checkMasterTLS(f flags) error {
	if !f.masterTLS.Enabled() {
		return nil
	}
	if err := f.masterTLS.Check(); err != nil {
		return fmt.Errorf("--master-tls-cert/--master-tls-key/--master-ca: %w", err)
	}
	pair, err := tls.LoadX509KeyPair(f.masterTLS.Cert, f.masterTLS.Key)
	if err != nil {
		return err
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return fmt.Errorf("--master-tls-cert %s: %w", f.masterTLS.Cert, err)
	}
	if leaf.Subject.CommonName != f.serverID {
		return fmt.Errorf("--master-tls-cert %s has CN=%q but --server-id is %q: a node certificate names its server id; issue one with CN=%s", f.masterTLS.Cert, leaf.Subject.CommonName, f.serverID, f.serverID)
	}
	if f.allowExternalStatusBind {
		return fmt.Errorf("--allow-external-status-bind cannot be combined with --master-tls-cert: an authenticated volume's status listener stays on loopback")
	}
	return nil
}

// masterDialTLS is the master link's dial option: mutual TLS when the flags
// name the files, nil (development-mode plaintext) otherwise.
func masterDialTLS(f flags) (grpc.DialOption, error) {
	if !f.masterTLS.Enabled() {
		return nil, nil
	}
	return link.ClientCredentials(f.masterTLS, "")
}
