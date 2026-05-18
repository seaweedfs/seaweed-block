package launcher

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
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultServiceAccountRoot = "/var/run/secrets/kubernetes.io/serviceaccount"
	defaultFieldManager       = "sw-block-launcher"
)

type KubernetesDeploymentClientConfig struct {
	BaseURL      string
	Token        string
	CAFile       string
	FieldManager string
	HTTPClient   *http.Client
}

type KubernetesDeploymentClient struct {
	baseURL      string
	token        string
	fieldManager string
	httpClient   *http.Client
	configErr    error
}

func NewKubernetesDeploymentClient(cfg KubernetesDeploymentClientConfig) *KubernetesDeploymentClient {
	if cfg.FieldManager == "" {
		cfg.FieldManager = defaultFieldManager
	}
	var configErr error
	if cfg.HTTPClient == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		if cfg.CAFile != "" {
			caRaw, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				configErr = fmt.Errorf("launcher: read Kubernetes CA file: %w", err)
			} else {
				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(caRaw) {
					configErr = fmt.Errorf("launcher: parse Kubernetes CA file %s", cfg.CAFile)
				} else {
					transport.TLSClientConfig = &tls.Config{
						RootCAs:    pool,
						MinVersion: tls.VersionTLS12,
					}
				}
			}
		}
		cfg.HTTPClient = &http.Client{Transport: transport, Timeout: 10 * time.Second}
	}
	return &KubernetesDeploymentClient{
		baseURL:      strings.TrimRight(cfg.BaseURL, "/"),
		token:        cfg.Token,
		fieldManager: cfg.FieldManager,
		httpClient:   cfg.HTTPClient,
		configErr:    configErr,
	}
}

func NewInClusterDeploymentClient() (*KubernetesDeploymentClient, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("launcher: in-cluster Kubernetes service env is not set")
	}
	tokenPath := filepath.Join(defaultServiceAccountRoot, "token")
	tokenRaw, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("launcher: read service account token: %w", err)
	}
	caPath := filepath.Join(defaultServiceAccountRoot, "ca.crt")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if caRaw, err := os.ReadFile(caPath); err == nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caRaw) {
			return nil, fmt.Errorf("launcher: parse service account ca.crt")
		}
		transport.TLSClientConfig = &tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		}
	}
	return NewKubernetesDeploymentClient(KubernetesDeploymentClientConfig{
		BaseURL:    "https://" + host + ":" + port,
		Token:      strings.TrimSpace(string(tokenRaw)),
		HTTPClient: &http.Client{Transport: transport, Timeout: 10 * time.Second},
	}), nil
}

func (c *KubernetesDeploymentClient) ListBlockVolumeDeployments(ctx context.Context, namespace string) ([]DeploymentIdentity, error) {
	endpoint := c.apiPath(namespace, "deployments")
	q := endpoint.Query()
	q.Set("labelSelector", LabelApp+"="+AppBlockVolume)
	endpoint.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Items []struct {
			Metadata struct {
				Name      string            `json:"name"`
				Namespace string            `json:"namespace"`
				Labels    map[string]string `json:"labels"`
			} `json:"metadata"`
			Spec struct {
				Replicas *int `json:"replicas"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := c.doJSON(req, http.StatusOK, &out); err != nil {
		return nil, err
	}
	identities := make([]DeploymentIdentity, 0, len(out.Items))
	for _, item := range out.Items {
		identities = append(identities, DeploymentIdentity{
			Namespace:    item.Metadata.Namespace,
			Name:         item.Metadata.Name,
			Labels:       item.Metadata.Labels,
			SpecReplicas: item.Spec.Replicas,
		})
	}
	return identities, nil
}

func (c *KubernetesDeploymentClient) ApplyDeployment(ctx context.Context, manifest RenderedManifest) error {
	identity, err := DecodeRenderedDeploymentIdentity(manifest)
	if err != nil {
		return err
	}
	endpoint := c.apiPath(identity.Namespace, "deployments/"+url.PathEscape(identity.Name))
	q := endpoint.Query()
	q.Set("fieldManager", c.fieldManager)
	q.Set("force", "true")
	endpoint.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, endpoint.String(), bytes.NewReader(manifest.YAML))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/apply-patch+yaml")
	return c.do(req, http.StatusOK, http.StatusCreated)
}

func (c *KubernetesDeploymentClient) DeleteDeployment(ctx context.Context, identity DeploymentIdentity) error {
	endpoint := c.apiPath(identity.Namespace, "deployments/"+url.PathEscape(identity.Name))
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint.String(), nil)
	if err != nil {
		return err
	}
	return c.do(req, http.StatusOK, http.StatusAccepted, http.StatusNoContent, http.StatusNotFound)
}

func (c *KubernetesDeploymentClient) apiPath(namespace, resource string) *url.URL {
	u, _ := url.Parse(c.baseURL)
	u.Path = "/apis/apps/v1/namespaces/" + url.PathEscape(namespace) + "/" + resource
	return u
}

func (c *KubernetesDeploymentClient) doJSON(req *http.Request, want int, out any) error {
	resp, err := c.send(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		return responseError(resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("launcher: decode Kubernetes response: %w", err)
	}
	return nil
}

func (c *KubernetesDeploymentClient) do(req *http.Request, want ...int) error {
	resp, err := c.send(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	for _, code := range want {
		if resp.StatusCode == code {
			return nil
		}
	}
	return responseError(resp)
}

func (c *KubernetesDeploymentClient) send(req *http.Request) (*http.Response, error) {
	if c.configErr != nil {
		return nil, c.configErr
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("launcher: Kubernetes %s %s: %w", req.Method, req.URL.Path, err)
	}
	return resp, nil
}

func responseError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	body := strings.TrimSpace(string(raw))
	if body == "" {
		body = resp.Status
	}
	return fmt.Errorf("launcher: Kubernetes %s %s: status=%d body=%s", resp.Request.Method, resp.Request.URL.Path, resp.StatusCode, body)
}
