package chunkstore

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// verifyListPage is the page size of one directory listing.
const verifyListPage = 1000

// List returns the entry names of directory p under the store base, all
// pages, in name order. A missing directory lists as empty.
func (s *LayoutStore) List(ctx context.Context, p string) ([]string, error) {
	if !path.IsAbs(p) || path.Clean(p) != p || !strings.HasPrefix(p, path.Dir(s.root)+"/") {
		return nil, errors.New("chunkstore: list path outside the store base")
	}
	var out []string
	after := ""
	for {
		page, err := s.f.ListEntries(ctx, p, "", after, verifyListPage)
		if err != nil {
			return nil, err
		}
		for _, e := range page {
			out = append(out, e.Name)
		}
		if len(page) < verifyListPage {
			return out, nil
		}
		after = page[len(page)-1].Name
	}
}

// VerifyLayout runs layout.Verify for vol under base through this adapter's
// Fetch and List (read-only; neither uses the volume id, which the pass
// reads from VOLUME itself). f must also serve blobs, as the SWFS client does.
func VerifyLayout(ctx context.Context, f Filer, base, vol string, opts layout.VerifyOptions) (layout.VerifyResult, error) {
	b, ok := f.(Blobs)
	if !ok {
		return layout.VerifyResult{}, errors.New("chunkstore: verify needs a filer that also serves blobs")
	}
	if !path.IsAbs(base) || path.Clean(base) != base || base == "/" {
		return layout.VerifyResult{}, errors.New("chunkstore: verify needs a clean absolute chunk base")
	}
	s := &LayoutStore{f: f, b: b, root: path.Join(base, vol)}
	opts.List = s.List
	return layout.Verify(ctx, base, vol, s.Fetch, opts)
}
