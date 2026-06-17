package csi

import (
	"bytes"
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

	blockops "github.com/seaweedfs/seaweed-block/core/ops"
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

type InClusterSwBlockVolumeRegistrar struct {
	client    *http.Client
	host      string
	token     string
	namespace string
}

func NewInClusterPVCMetadataResolver() (*InClusterPVCMetadataResolver, error) {
	client, host, token, err := newInClusterKubernetesClient()
	if err != nil {
		return nil, err
	}
	return &InClusterPVCMetadataResolver{client: client, host: host, token: token}, nil
}

func NewInClusterSwBlockVolumeRegistrar(namespace string) (*InClusterSwBlockVolumeRegistrar, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("SwBlockVolume namespace is required")
	}
	client, host, token, err := newInClusterKubernetesClient()
	if err != nil {
		return nil, err
	}
	return &InClusterSwBlockVolumeRegistrar{client: client, host: host, token: token, namespace: namespace}, nil
}

func newInClusterKubernetesClient() (*http.Client, string, string, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, "", "", fmt.Errorf("KUBERNETES_SERVICE_HOST/PORT are not set")
	}
	tokenBytes, err := os.ReadFile(serviceAccountTokenPath)
	if err != nil {
		return nil, "", "", fmt.Errorf("read service account token: %w", err)
	}
	caBytes, err := os.ReadFile(serviceAccountCAPath)
	if err != nil {
		return nil, "", "", fmt.Errorf("read service account ca: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caBytes) {
		return nil, "", "", fmt.Errorf("parse service account ca")
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs:    roots,
			MinVersion: tls.VersionTLS12,
		}},
	}
	return client, "https://" + net.JoinHostPort(host, port), strings.TrimSpace(string(tokenBytes)), nil
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

func (r *InClusterSwBlockVolumeRegistrar) EnsureVolumeObject(ctx context.Context, spec VolumeSpec) error {
	if r == nil || r.client == nil {
		return fmt.Errorf("registrar not configured")
	}
	name := blockops.SwBlockVolumeObjectName(blockops.ManagedVolumeOperatorStatus{
		VolumeID: spec.VolumeID,
		PVCName:  spec.PVCName,
	})
	body := swBlockVolumeObject{
		APIVersion: blockops.SwBlockVolumeAPIVersion,
		Kind:       blockops.SwBlockVolumeKind,
		Metadata: swBlockVolumeMetadata{
			Name:      name,
			Namespace: r.namespace,
		},
		Spec: swBlockVolumeSpec{
			PVCName:      spec.PVCName,
			StorageClass: spec.StorageClass,
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal SwBlockVolume: %w", err)
	}
	endpoint := r.host + "/apis/block.seaweedfs.com/v1alpha1/namespaces/" + url.PathEscape(r.namespace) + "/swblockvolumes"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	r.setJSONHeaders(req)
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if resp.StatusCode == http.StatusConflict {
		return r.patchVolumeSpec(ctx, name, body.Spec)
	}
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("create SwBlockVolume %s/%s failed: http %d %s", r.namespace, name, resp.StatusCode, strings.TrimSpace(string(respBody)))
}

func (r *InClusterSwBlockVolumeRegistrar) patchVolumeSpec(ctx context.Context, name string, spec swBlockVolumeSpec) error {
	raw, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		return fmt.Errorf("marshal SwBlockVolume spec patch: %w", err)
	}
	endpoint := r.host + "/apis/block.seaweedfs.com/v1alpha1/namespaces/" + url.PathEscape(r.namespace) + "/swblockvolumes/" + url.PathEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	r.setJSONHeaders(req)
	req.Header.Set("Content-Type", "application/merge-patch+json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("patch SwBlockVolume %s/%s spec failed: http %d %s", r.namespace, name, resp.StatusCode, strings.TrimSpace(string(respBody)))
}

func (r *InClusterSwBlockVolumeRegistrar) setJSONHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/json")
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
}

type swBlockVolumeObject struct {
	APIVersion string                `json:"apiVersion"`
	Kind       string                `json:"kind"`
	Metadata   swBlockVolumeMetadata `json:"metadata"`
	Spec       swBlockVolumeSpec     `json:"spec"`
}

type swBlockVolumeMetadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type swBlockVolumeSpec struct {
	PVCName      string `json:"pvcName,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
}
