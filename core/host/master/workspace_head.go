package master

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// NewWorkspaceHEADReader reads inline HEAD content from the configured filer
// HTTP directory. It never follows redirects or accepts operator-supplied HEAD
// evidence. W2's detached state is part of the committed HEAD document.
func NewWorkspaceHEADReader(base string) (func(context.Context, string) (authority.WorkspaceHead, error), error) {
	u, err := url.Parse(base)
	if err != nil || u == nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("workspace HEAD base must be an absolute HTTP directory URL")
	}
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return func(ctx context.Context, volume string) (authority.WorkspaceHead, error) {
		if volume == "" || volume == "." || volume == ".." || strings.ContainsAny(volume, "/\\\x00") {
			return authority.WorkspaceHead{}, fmt.Errorf("invalid volume path")
		}
		endpoint := strings.TrimRight(base, "/") + "/" + url.PathEscape(volume) + "/HEAD"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		res, err := client.Do(req)
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return authority.WorkspaceHead{}, fmt.Errorf("HEAD read HTTP %d", res.StatusCode)
		}
		raw, err := io.ReadAll(io.LimitReader(res.Body, (16<<20)+1))
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		if len(raw) > 16<<20 {
			return authority.WorkspaceHead{}, fmt.Errorf("HEAD too large")
		}
		var doc struct {
			Format    int    `json:"format"`
			Gen       uint64 `json:"gen"`
			Epoch     uint64 `json:"epoch"`
			Holder    string `json:"holder"`
			RemoteLSN uint64 `json:"remote_lsn"`
			State     string `json:"state"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			return authority.WorkspaceHead{}, err
		}
		if doc.Format != 1 || doc.Gen == 0 || doc.Epoch == 0 || doc.Holder == "" || (doc.State != "" && doc.State != "attached" && doc.State != "detached") {
			return authority.WorkspaceHead{}, fmt.Errorf("invalid HEAD identity/state")
		}
		return authority.WorkspaceHead{Gen: doc.Gen, Epoch: doc.Epoch, Holder: doc.Holder, RemoteLSN: doc.RemoteLSN, Detached: doc.State == "detached"}, nil
	}, nil
}
