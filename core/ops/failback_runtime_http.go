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

type HTTPFailbackRuntime struct {
	Endpoint string
	Client   *http.Client
}

func NewHTTPFailbackRuntime(endpoint string, client *http.Client) *HTTPFailbackRuntime {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPFailbackRuntime{
		Endpoint: strings.TrimSpace(endpoint),
		Client:   client,
	}
}

func (r *HTTPFailbackRuntime) ExecuteFailback(ctx context.Context, req FailbackRuntimeRequest) (FailbackRuntimeResult, error) {
	if r == nil || strings.TrimSpace(r.Endpoint) == "" {
		return FailbackRuntimeResult{}, fmt.Errorf("failback runtime endpoint is required")
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return FailbackRuntimeResult{}, fmt.Errorf("marshal failback runtime request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Endpoint, bytes.NewReader(body))
	if err != nil {
		return FailbackRuntimeResult{}, fmt.Errorf("create failback runtime request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return FailbackRuntimeResult{}, fmt.Errorf("post failback runtime request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return FailbackRuntimeResult{}, fmt.Errorf("failback runtime returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	var result FailbackRuntimeResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return FailbackRuntimeResult{}, fmt.Errorf("decode failback runtime response: %w", err)
	}
	return result, nil
}
