package ops

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	SwBlockClusterPlural = "swblockclusters"
	SwBlockVolumePlural  = "swblockvolumes"

	serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	serviceAccountCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

// KubernetesStatusClient patches only CRD status subresources. It deliberately
// has no methods for spec, workload, PVC/PV, Secret, StorageClass, or host mutation.
type KubernetesStatusClient struct {
	BaseURL     string
	BearerToken string
	HTTPClient  *http.Client
}

func NewInClusterKubernetesStatusClient() (*KubernetesStatusClient, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT are required for non-dry-run operator status")
	}
	token, err := os.ReadFile(serviceAccountTokenPath)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	ca, err := os.ReadFile(serviceAccountCAPath)
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("service account CA bundle is empty or invalid")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	return &KubernetesStatusClient{
		BaseURL:     "https://" + netJoinHostPort(host, port),
		BearerToken: strings.TrimSpace(string(token)),
		HTTPClient:  &http.Client{Transport: transport, Timeout: 10 * time.Second},
	}, nil
}

func (c *KubernetesStatusClient) WriteClusterStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockClusterCRDStatus) error {
	return c.patchStatus(ctx, ref.Namespace, SwBlockClusterPlural, ref.Name, status)
}

func (c *KubernetesStatusClient) WriteVolumeStatus(ctx context.Context, ref OperatorObjectRef, status SwBlockVolumeCRDStatus) error {
	return c.patchStatus(ctx, ref.Namespace, SwBlockVolumePlural, ref.Name, status)
}

func (c *KubernetesStatusClient) patchStatus(ctx context.Context, namespace, resource, name string, status any) error {
	if namespace == "" || resource == "" || name == "" {
		return fmt.Errorf("namespace, resource, and name are required for status patch")
	}
	body, err := json.Marshal(map[string]any{"status": status})
	if err != nil {
		return fmt.Errorf("marshal status patch: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.statusURL(namespace, resource, name), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/merge-patch+json")
	if c.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.BearerToken)
	}
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("patch %s/%s status failed: http %d %s", resource, name, resp.StatusCode, strings.TrimSpace(string(raw)))
}

func (c *KubernetesStatusClient) statusURL(namespace, resource, name string) string {
	base := strings.TrimRight(c.BaseURL, "/")
	return base + "/apis/block.seaweedfs.com/v1alpha1/namespaces/" +
		pathEscape(namespace) + "/" + pathEscape(resource) + "/" + pathEscape(name) + "/status"
}

func pathEscape(value string) string {
	return url.PathEscape(value)
}

func netJoinHostPort(host, port string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]:" + port
	}
	return host + ":" + port
}
