// Package authtest issues test certificates for the phase 0 authentication
// tests: a CA, and leaf certificates signed by it with a chosen CN and expiry.
// Test support only; nothing in the product imports it.
package authtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// CA is a test certificate authority.
type CA struct {
	Cert *x509.Certificate
	key  *ecdsa.PrivateKey
	PEM  []byte
}

// Leaf is a certificate the CA issued, with its key.
type Leaf struct {
	Cert        *x509.Certificate
	TLS         tls.Certificate
	CertPEM     []byte
	KeyPEM      []byte
	Fingerprint string // "sha256:<hex>" of the leaf DER, as the role file writes it
}

// NewCA makes a CA valid for a day.
func NewCA(t testing.TB, name string) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &CA{Cert: cert, key: key, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

var serial = big.NewInt(100)

// Issue signs a leaf certificate with the given CN that expires at notAfter;
// it is valid as a TLS client and server certificate for 127.0.0.1,
// localhost and any extra IP addresses.
func (ca *CA) Issue(t testing.TB, cn string, notAfter time.Time, extraIPs ...net.IP) *Leaf {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial = new(big.Int).Add(serial, big.NewInt(1))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  append([]net.IP{net.ParseIP("127.0.0.1")}, extraIPs...),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(der)
	return &Leaf{Cert: cert, TLS: pair, CertPEM: certPEM, KeyPEM: keyPEM, Fingerprint: "sha256:" + hex.EncodeToString(sum[:])}
}

// WriteFiles writes the leaf's certificate and key to dir as <name>.crt and
// <name>.key and returns their paths.
func (l *Leaf) WriteFiles(t testing.TB, dir, name string) (certPath, keyPath string) {
	t.Helper()
	certPath, keyPath = filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	if err := os.WriteFile(certPath, l.CertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, l.KeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// WriteCA writes the CA certificate to dir/ca.crt and returns its path.
func (ca *CA) WriteCA(t testing.TB, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(p, ca.PEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
