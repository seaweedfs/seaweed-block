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

type HTTPAuthorityRebuildRuntime struct {
	Endpoint string
	Client   *http.Client
}

func NewHTTPAuthorityRebuildRuntime(endpoint string, client *http.Client) *HTTPAuthorityRebuildRuntime {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPAuthorityRebuildRuntime{
		Endpoint: strings.TrimSpace(endpoint),
		Client:   client,
	}
}

func (r *HTTPAuthorityRebuildRuntime) ExecuteRebuild(ctx context.Context, req AuthorityRebuildRuntimeRequest) (AuthorityRebuildRuntimeResult, error) {
	if r == nil || strings.TrimSpace(r.Endpoint) == "" {
		return AuthorityRebuildRuntimeResult{}, fmt.Errorf("authority rebuild runtime endpoint is required")
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return AuthorityRebuildRuntimeResult{}, fmt.Errorf("marshal rebuild runtime request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Endpoint, bytes.NewReader(body))
	if err != nil {
		return AuthorityRebuildRuntimeResult{}, fmt.Errorf("create rebuild runtime request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return AuthorityRebuildRuntimeResult{}, fmt.Errorf("post rebuild runtime request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return AuthorityRebuildRuntimeResult{}, fmt.Errorf("rebuild runtime returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	var result AuthorityRebuildRuntimeResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return AuthorityRebuildRuntimeResult{}, fmt.Errorf("decode rebuild runtime response: %w", err)
	}
	return result, nil
}
