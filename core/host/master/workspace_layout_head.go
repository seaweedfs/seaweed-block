package master

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// NewLayoutWorkspaceHEADReader resolves the master's immutable identity before
// reading one ordinary filer metadata response. Content, generation and mtime
// come from that same entry; no legacy path or body decoder is a fallback.
func NewLayoutWorkspaceHEADReader(base string, resolve func(context.Context, string) (chunkstore.VolumeRef, error)) (func(context.Context, string) (authority.WorkspaceHead, error), error) {
	u, err := url.Parse(base)
	if err != nil || u == nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || resolve == nil {
		return nil, fmt.Errorf("workspace HEAD needs an absolute HTTP directory and master volume resolver")
	}
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return func(ctx context.Context, name string) (authority.WorkspaceHead, error) {
		if err := ctx.Err(); err != nil {
			return authority.WorkspaceHead{}, err
		}
		v, err := resolve(ctx, name)
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		if v.Name != name {
			return authority.WorkspaceHead{}, layout.ErrIdentity
		}
		if _, err := (layout.Volume{ID: v.ID, Name: v.Name, Format: layout.VolumeFormat}).Encode(); err != nil {
			return authority.WorkspaceHead{}, err
		}
		endpoint := *u
		endpoint.Path = strings.TrimRight(u.Path, "/") + "/" + v.Dir() + "/HEAD"
		endpoint.RawPath = ""
		endpoint.RawQuery = "metadata=true"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		res, err := client.Do(req)
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return authority.WorkspaceHead{}, fmt.Errorf("HEAD metadata read HTTP %d", res.StatusCode)
		}
		// HEAD is at most 64 KiB inline; allow its base64 envelope and ordinary
		// filer attributes without accepting an unbounded metadata response.
		const limit = 4 * layout.MaxHeadBytes
		raw, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		if len(raw) > limit {
			return authority.WorkspaceHead{}, fmt.Errorf("HEAD metadata too large")
		}
		var entry struct {
			FullPath string
			Content  []byte
			Extended map[string][]byte
			Mtime    time.Time
			Mode     uint32
			Chunks   []*filerpb.FileChunk
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return authority.WorkspaceHead{}, err
		}
		if entry.FullPath != endpoint.Path {
			return authority.WorkspaceHead{}, fmt.Errorf("HEAD metadata path mismatch")
		}
		h, err := chunkstore.DecodeLayoutHeadEntry(&filerpb.Entry{Name: "HEAD", IsDirectory: entry.Mode&uint32(os.ModeDir) != 0, Content: entry.Content, Extended: entry.Extended, Chunks: entry.Chunks}, v.ID)
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		mtime := int64(0)
		if ns := entry.Mtime.UnixNano(); ns > 0 && time.Unix(0, ns).Equal(entry.Mtime) {
			mtime = ns
		}
		proof := authority.WorkspaceHead{Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.Position, Detached: h.State == layout.StateDetached, FilerMtimeNS: mtime}
		if h.State == layout.StateDeleting || h.State == layout.StateDeleted {
			proof.Deleting = true
			return proof, authority.ErrWorkspaceHeadDeleting
		}
		if h.Epoch == 0 {
			return proof, authority.ErrWorkspaceHeadUnclaimed
		}
		return proof, nil
	}, nil
}

// WorkspaceLayoutVolume reads only published lifecycle intent. A missing
// identity cannot be inferred from a filer directory or a public HEAD.
func (h *Host) WorkspaceLayoutVolume(ctx context.Context, name string) (chunkstore.VolumeRef, error) {
	if err := ctx.Err(); err != nil {
		return chunkstore.VolumeRef{}, err
	}
	if h == nil || h.lifecycle == nil || h.lifecycle.Volumes == nil {
		return chunkstore.VolumeRef{}, fmt.Errorf("workspace lifecycle intent is not ready")
	}
	rec, ok := h.lifecycle.Volumes.GetVolumeSnapshot(name)
	if !ok {
		return chunkstore.VolumeRef{}, fmt.Errorf("workspace volume %q is not registered", name)
	}
	id, err := layout.ParseVolumeID(rec.Spec.LayoutID)
	if err != nil {
		return chunkstore.VolumeRef{}, fmt.Errorf("workspace volume %q has no valid layout identity; recreate the legacy volume: %w", name, err)
	}
	v := chunkstore.VolumeRef{Name: rec.Spec.VolumeID, ID: id}
	if v.Name != name {
		return chunkstore.VolumeRef{}, layout.ErrIdentity
	}
	return v, nil
}
