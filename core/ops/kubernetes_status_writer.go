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

func (c *KubernetesStatusClient) EmitEvent(ctx context.Context, event OperatorKubernetesEvent) error {
	namespace := event.InvolvedObject.Namespace
	if namespace == "" {
		namespace = "default"
	}
	now := event.ObservedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	body := kubernetesCoreEvent{
		APIVersion:     "v1",
		Kind:           "Event",
		Metadata:       kubernetesMetadata{Name: kubernetesEventName(event, now), Namespace: namespace},
		InvolvedObject: kubernetesObjectReferenceFromOperator(event.InvolvedObject),
		Type:           event.Type,
		Reason:         event.Reason,
		Message:        event.Message,
		Source:         kubernetesEventSource{Component: "sw-block-operator-status"},
		FirstTimestamp: now.Format(time.RFC3339),
		LastTimestamp:  now.Format(time.RFC3339),
		Count:          1,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.eventsURL(namespace), bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
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
	rawResp, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("create event %s failed: http %d %s", event.Reason, resp.StatusCode, strings.TrimSpace(string(rawResp)))
}

func (c *KubernetesStatusClient) statusURL(namespace, resource, name string) string {
	base := strings.TrimRight(c.BaseURL, "/")
	return base + "/apis/block.seaweedfs.com/v1alpha1/namespaces/" +
		pathEscape(namespace) + "/" + pathEscape(resource) + "/" + pathEscape(name) + "/status"
}

func (c *KubernetesStatusClient) eventsURL(namespace string) string {
	base := strings.TrimRight(c.BaseURL, "/")
	return base + "/api/v1/namespaces/" + pathEscape(namespace) + "/events"
}

func kubernetesObjectReferenceFromOperator(ref OperatorObjectRef) kubernetesObjectReference {
	return kubernetesObjectReference{
		APIVersion: ref.APIVersion,
		Kind:       ref.Kind,
		Namespace:  ref.Namespace,
		Name:       ref.Name,
	}
}

func kubernetesEventName(event OperatorKubernetesEvent, at time.Time) string {
	base := kubernetesName(event.InvolvedObject.Name + "-" + event.Reason)
	if base == "unknown-volume" {
		base = "sw-block-event"
	}
	return fmt.Sprintf("%s.%d", base, at.UnixNano())
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

type kubernetesCoreEvent struct {
	APIVersion     string                    `json:"apiVersion"`
	Kind           string                    `json:"kind"`
	Metadata       kubernetesMetadata        `json:"metadata"`
	InvolvedObject kubernetesObjectReference `json:"involvedObject"`
	Type           string                    `json:"type"`
	Reason         string                    `json:"reason"`
	Message        string                    `json:"message"`
	Source         kubernetesEventSource     `json:"source"`
	FirstTimestamp string                    `json:"firstTimestamp"`
	LastTimestamp  string                    `json:"lastTimestamp"`
	Count          int                       `json:"count"`
}

type kubernetesMetadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type kubernetesObjectReference struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	Name       string `json:"name,omitempty"`
}

type kubernetesEventSource struct {
	Component string `json:"component"`
}
