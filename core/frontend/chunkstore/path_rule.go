package chunkstore

// The chunk path's filer storage rule (/etc/seaweedfs/filer.conf) applies to
// every entry and needle the store writes under it (the assign path is the
// chunk path). A rule can expire entries (ttl), refuse writes (readOnly),
// freeze them (worm) or leave needle writes unsynced (no fsync); a chunk
// path under the filer's buckets directory is subject to S3 lifecycle. Each
// of these loses or blocks a volume, so start refuses them.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

const (
	filerConfDir  = "/etc/seaweedfs"
	filerConfName = "filer.conf"
)

// ErrPathRule: the chunk path's storage rule would lose or block data.
var ErrPathRule = errors.New("chunkstore: chunk path storage rule refused")

// pathRule is one filer.conf location; the filer writes protojson
// (lowerCamelCase), and reads either name.
type pathRule struct {
	prefix          string
	ttl             string
	fsync, readOnly bool
	worm            *bool
}

func parseFilerConf(body []byte) ([]pathRule, error) {
	var doc struct {
		Locations []map[string]json.RawMessage `json:"locations"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	field := func(m map[string]json.RawMessage, camel, snake string, v any) error {
		raw, ok := m[camel]
		if !ok {
			raw, ok = m[snake]
		}
		if !ok {
			return nil
		}
		return json.Unmarshal(raw, v)
	}
	out := make([]pathRule, 0, len(doc.Locations))
	for _, m := range doc.Locations {
		var r pathRule
		var worm bool
		_, hasWorm := m["worm"]
		for _, err := range []error{
			field(m, "locationPrefix", "location_prefix", &r.prefix),
			field(m, "ttl", "ttl", &r.ttl),
			field(m, "fsync", "fsync", &r.fsync),
			field(m, "readOnly", "read_only", &r.readOnly),
			field(m, "worm", "worm", &worm),
		} {
			if err != nil {
				return nil, err
			}
		}
		if hasWorm {
			r.worm = &worm
		}
		out = append(out, r)
	}
	return out, nil
}

// checkPathRule judges the rules for the chunk base directory base, with
// the filer's buckets directory buckets. Stock merging: every rule whose
// prefix is a prefix of a path applies, shorter first; fsync and readOnly
// are OR'ed, ttl and worm are taken from the longest rule that sets them.
// A rule inside the chunk path applies to the entries under it.
func checkPathRule(rules []pathRule, base, buckets string) error {
	dir := strings.TrimSuffix(base, "/") + "/"
	if b := strings.TrimSuffix(buckets, "/"); b != "" && (base == b || strings.HasPrefix(dir, b+"/")) {
		return fmt.Errorf("%w: %s is under the filer's buckets directory %s (S3 lifecycle can expire it)", ErrPathRule, base, b)
	}
	sort.SliceStable(rules, func(i, j int) bool { return len(rules[i].prefix) < len(rules[j].prefix) })
	var fsync, readOnly, worm bool
	ttl := ""
	for _, r := range rules {
		switch {
		case r.prefix != "" && strings.HasPrefix(dir, r.prefix): // applies to the chunk path
			fsync = fsync || r.fsync
			readOnly = readOnly || r.readOnly
			if r.ttl != "" {
				ttl = r.ttl
			}
			if r.worm != nil {
				worm = *r.worm
			}
		case strings.HasPrefix(r.prefix, dir): // a rule inside the chunk path
			if r.ttl != "" || r.readOnly || (r.worm != nil && *r.worm) {
				return fmt.Errorf("%w: the rule on %s sets ttl %q, readOnly %t or worm", ErrPathRule, r.prefix, r.ttl, r.readOnly)
			}
		}
	}
	switch {
	case ttl != "":
		return fmt.Errorf("%w: the rule on %s sets ttl %q: the filer would expire the volume's entries", ErrPathRule, base, ttl)
	case readOnly:
		return fmt.Errorf("%w: the rule on %s is readOnly", ErrPathRule, base)
	case worm:
		return fmt.Errorf("%w: the rule on %s is worm", ErrPathRule, base)
	case !fsync:
		return fmt.Errorf("%w: no rule on %s sets fsync; configure one, e.g. fs.configure -locationPrefix=%s -fsync -apply", ErrPathRule, base, dir)
	}
	return nil
}

// CheckPathRule is the start check of the chunk path base: it reads the
// filer's storage rules and refuses a rule that would expire, freeze or
// block the volume's entries, or leave its needle writes unsynced, and a
// chunk path under the buckets directory buckets.
func CheckPathRule(ctx context.Context, f Filer, blobs Blobs, base, buckets string) error {
	if !path.IsAbs(base) || path.Clean(base) != base {
		return fmt.Errorf("chunkstore: chunk base %q is not a clean absolute directory", base)
	}
	var rules []pathRule
	_, body, err := lookupRecord(ctx, f, blobs, filerConfDir, filerConfName)
	switch {
	case errors.Is(err, ErrNotFound): // no rules: fsync is unset
	case err != nil:
		return fmt.Errorf("chunkstore: read %s/%s: %w", filerConfDir, filerConfName, err)
	default:
		if rules, err = parseFilerConf(body); err != nil {
			return fmt.Errorf("chunkstore: parse %s/%s: %v", filerConfDir, filerConfName, err)
		}
	}
	return checkPathRule(rules, base, buckets)
}

// CheckPathRule runs the chunk path start check against this filer.
func (c *SWFS) CheckPathRule(ctx context.Context, base string) error {
	cfg, err := c.fc.GetFilerConfiguration(ctx, &filerpb.GetFilerConfigurationRequest{})
	if err != nil {
		return fmt.Errorf("chunkstore: filer %s configuration: %v", c.cfg.FilerGRPC, err)
	}
	return CheckPathRule(ctx, c, c, base, cfg.GetDirBuckets())
}
