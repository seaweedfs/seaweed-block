package csi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	serviceAccountCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type InClusterPVCMetadataResolver struct {
	client *http.Client
	host   string
	token  string
}

func NewInClusterPVCMetadataResolver() (*InClusterPVCMetadataResolver, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST/PORT are not set")
	}
	tokenBytes, err := os.ReadFile(serviceAccountTokenPath)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	caBytes, err := os.ReadFile(serviceAccountCAPath)
	if err != nil {
		return nil, fmt.Errorf("read service account ca: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("parse service account ca")
	}
	return &InClusterPVCMetadataResolver{
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				RootCAs:    roots,
				MinVersion: tls.VersionTLS12,
			}},
		},
		host:  "https://" + net.JoinHostPort(host, port),
		token: strings.TrimSpace(string(tokenBytes)),
	}, nil
}

func (r *InClusterPVCMetadataResolver) ResolvePVCUID(ctx context.Context, name, namespace string) (string, error) {
	if r == nil || r.client == nil {
		return "", fmt.Errorf("resolver not configured")
	}
	if name == "" || namespace == "" {
		return "", fmt.Errorf("pvc name and namespace are required")
	}
	endpoint := r.host + "/api/v1/namespaces/" + url.PathEscape(namespace) + "/persistentvolumeclaims/" + url.PathEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := r.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("kubernetes api status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Metadata.UID == "" {
		return "", fmt.Errorf("kubernetes api response missing metadata.uid")
	}
	return out.Metadata.UID, nil
}
