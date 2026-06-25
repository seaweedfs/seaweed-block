package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type HTTPFrontendPublicationRuntime struct {
	Endpoint string
	Client   *http.Client
}

func NewHTTPFrontendPublicationRuntime(endpoint string, client *http.Client) *HTTPFrontendPublicationRuntime {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPFrontendPublicationRuntime{
		Endpoint: strings.TrimSpace(endpoint),
		Client:   client,
	}
}

func (r *HTTPFrontendPublicationRuntime) ExecuteFrontendPublication(ctx context.Context, req FrontendPublicationRuntimeRequest) (FrontendPublicationRuntimeResult, error) {
	if r == nil || strings.TrimSpace(r.Endpoint) == "" {
		return FrontendPublicationRuntimeResult{}, fmt.Errorf("frontend publication runtime endpoint is required")
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return FrontendPublicationRuntimeResult{}, fmt.Errorf("marshal frontend publication runtime request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Endpoint, bytes.NewReader(body))
	if err != nil {
		return FrontendPublicationRuntimeResult{}, fmt.Errorf("create frontend publication runtime request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return FrontendPublicationRuntimeResult{}, fmt.Errorf("post frontend publication runtime request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return FrontendPublicationRuntimeResult{}, fmt.Errorf("frontend publication runtime returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	var result FrontendPublicationRuntimeResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return FrontendPublicationRuntimeResult{}, fmt.Errorf("decode frontend publication runtime response: %w", err)
	}
	return result, nil
}
