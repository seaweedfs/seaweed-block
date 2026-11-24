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
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
)

// headFilerMtimeNS is the HEAD entry's mtime from the filer's Last-Modified
// header of the same response as the content (1 s resolution), or 0
// (unknown) when the header is absent, unparseable or not after the epoch.
func headFilerMtimeNS(h http.Header) int64 {
	v := h.Get("Last-Modified")
	if v == "" {
		return 0
	}
	t, err := http.ParseTime(v)
	if err != nil || t.UnixNano() <= 0 {
		return 0
	}
	return t.UnixNano()
}

// NewWorkspaceHEADReader reads inline HEAD content from the configured filer
// HTTP directory. It never follows redirects or accepts operator-supplied HEAD
// evidence. W2's detached state is part of the committed HEAD document. The
// HEAD's filer mtime comes from the same response (headFilerMtimeNS).
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
			Gen        uint64 `json:"gen"`
			Epoch      uint64 `json:"epoch"`
			Holder     string `json:"holder"`
			RemoteLSN  uint64 `json:"remote_lsn"`
			State      string `json:"state"`
			DetachedBy *struct {
				Holder string `json:"holder"`
				Epoch  uint64 `json:"epoch"`
				Gen    uint64 `json:"gen"`
			} `json:"detachedBy"`
			LegacyDetached json.RawMessage `json:"detached"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			return authority.WorkspaceHead{}, err
		}
		if _, err := chunkstore.DecodeHeadDocument("workspace/"+volume, raw); err != nil {
			return authority.WorkspaceHead{}, err
		}
		// DecodeHeadDocument above owns the supported-format checks.
		// The initial HEAD of a volume no holder ever claimed: epoch 0, no
		// holder, active. It names no identity, so callers that need one get
		// an error as before; the delete reads it as "never started".
		if doc.Gen > 0 && doc.Epoch == 0 && doc.Holder == "" && doc.LegacyDetached == nil && doc.DetachedBy == nil && (doc.State == "" || doc.State == "active") {
			return authority.WorkspaceHead{Gen: doc.Gen, FilerMtimeNS: headFilerMtimeNS(res.Header)}, authority.ErrWorkspaceHeadUnclaimed
		}
		if doc.Gen == 0 || doc.Epoch == 0 || doc.Holder == "" || doc.LegacyDetached != nil || (doc.State != "" && doc.State != "active" && doc.State != "detached") {
			return authority.WorkspaceHead{}, fmt.Errorf("invalid HEAD identity/state")
		}
		// Missing state is the legacy active representation. Only a final-cut
		// CAS whose proof names the installed identity can authorize CLEAN.
		if doc.State == "detached" {
			if doc.DetachedBy == nil || doc.DetachedBy.Holder != doc.Holder || doc.DetachedBy.Epoch != doc.Epoch || doc.DetachedBy.Gen != doc.Gen {
				return authority.WorkspaceHead{}, fmt.Errorf("invalid HEAD detachedBy proof")
			}
		} else if doc.DetachedBy != nil {
			return authority.WorkspaceHead{}, fmt.Errorf("active HEAD retains detachedBy proof")
		}
		return authority.WorkspaceHead{Gen: doc.Gen, Epoch: doc.Epoch, Holder: doc.Holder, RemoteLSN: doc.RemoteLSN, Detached: doc.State == "detached", FilerMtimeNS: headFilerMtimeNS(res.Header)}, nil
	}, nil
}
