package link

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// TLSFiles names one side's files for a mutual-TLS link to or from
// blockmaster (phase 0): its own certificate and key, and the CA the other
// side's certificate must verify against. All three or none.
type TLSFiles struct {
	Cert, Key, CA string
}

// Enabled reports whether any file is named.
func (f TLSFiles) Enabled() bool { return f.Cert != "" || f.Key != "" || f.CA != "" }

// Check refuses a partial set and loads every file once, so a wrong path or
// a key that does not match its certificate fails at start, not at the
// first handshake.
func (f TLSFiles) Check() error {
	if !f.Enabled() {
		return nil
	}
	if f.Cert == "" || f.Key == "" || f.CA == "" {
		return fmt.Errorf("mutual TLS needs a certificate, its key and a CA: got cert=%q key=%q ca=%q", f.Cert, f.Key, f.CA)
	}
	if _, err := tls.LoadX509KeyPair(f.Cert, f.Key); err != nil {
		return fmt.Errorf("TLS certificate %s / key %s: %w", f.Cert, f.Key, err)
	}
	_, err := f.pool()
	return err
}

func (f TLSFiles) pool() (*x509.CertPool, error) {
	raw, err := os.ReadFile(f.CA)
	if err != nil {
		return nil, fmt.Errorf("TLS CA: %w", err)
	}
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(raw) {
		return nil, fmt.Errorf("TLS CA %s holds no PEM certificate", f.CA)
	}
	return p, nil
}

// loadPair reads the certificate and key at every handshake, so a rotated
// file takes effect at the next connection without a restart.
func (f TLSFiles) loadPair() (*tls.Certificate, error) {
	c, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil {
		return nil, fmt.Errorf("TLS certificate %s / key %s: %w", f.Cert, f.Key, err)
	}
	return &c, nil
}

// ServerCredentials is blockmaster's mutual-TLS server: every client must
// present a certificate that verifies against f.CA (who it is, and what it
// may do, is the role file's: auth.PeerIdentity).
func ServerCredentials(f TLSFiles) (grpc.ServerOption, error) {
	if err := f.Check(); err != nil {
		return nil, err
	}
	pool, err := f.pool()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return f.loadPair()
		},
	}
	return grpc.Creds(credentials.NewTLS(cfg)), nil
}

// ClientCredentials is a mutual-TLS client of blockmaster: it presents
// f.Cert and verifies the master's certificate against f.CA. serverName
// overrides the name checked against the master's certificate ("" uses
// the dialed host).
func ClientCredentials(f TLSFiles, serverName string) (grpc.DialOption, error) {
	if err := f.Check(); err != nil {
		return nil, err
	}
	pool, err := f.pool()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
		ServerName: serverName,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return f.loadPair()
		},
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(cfg)), nil
}

// MasterConnectionAge is how long blockmaster keeps one client connection
// before it asks the client to reconnect (a fresh handshake: a rotated or
// revoked certificate is seen within this bound even on an idle link).
const MasterConnectionAge = 24 * time.Hour

// ConnectionAge closes each server connection after age (plus a grace for
// calls in flight: 30 s, or half the age when that is shorter); clients
// reconnect.
func ConnectionAge(age time.Duration) grpc.ServerOption {
	grace := 30 * time.Second
	if age/2 < grace {
		grace = age / 2
	}
	return grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: age, MaxConnectionAgeGrace: grace})
}

// DialMaster opens a lazy connection to blockmaster: mutual TLS when tls is
// non-nil (ClientCredentials), plaintext otherwise (development mode); opts
// follow. SeaweedFS filer links keep Dial: they are not master links.
func DialMaster(target string, tls grpc.DialOption, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if tls == nil {
		return Dial(target, opts...)
	}
	return DialWith(target, append([]grpc.DialOption{tls}, opts...)...)
}

// The environment variables a command-line tool (sw-block) reads for its
// mutual-TLS identity toward blockmaster: a person's or tool's certificate,
// its key, and the CA the master's certificate must verify against.
const (
	EnvTLSCert = "SW_BLOCK_TLS_CERT"
	EnvTLSKey  = "SW_BLOCK_TLS_KEY"
	EnvTLSCA   = "SW_BLOCK_TLS_CA"
)

// MasterTLSFromEnv is the dial option for a tool's master link: mutual TLS
// when SW_BLOCK_TLS_CERT/_KEY/_CA name files (all three), nil (plaintext,
// development mode) when none is set.
func MasterTLSFromEnv() (grpc.DialOption, error) {
	f := TLSFiles{Cert: os.Getenv(EnvTLSCert), Key: os.Getenv(EnvTLSKey), CA: os.Getenv(EnvTLSCA)}
	if !f.Enabled() {
		return nil, nil
	}
	d, err := ClientCredentials(f, "")
	if err != nil {
		return nil, fmt.Errorf("%s/%s/%s: %w", EnvTLSCert, EnvTLSKey, EnvTLSCA, err)
	}
	return d, nil
}

// DialMasterFromEnv opens a tool's lazy connection to blockmaster with the
// identity from the environment (MasterTLSFromEnv).
func DialMasterFromEnv(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	d, err := MasterTLSFromEnv()
	if err != nil {
		return nil, err
	}
	return DialMaster(target, d, opts...)
}
