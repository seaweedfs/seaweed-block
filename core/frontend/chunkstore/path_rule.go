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

// AssignPathFor is the assign path of chunk base base: the directory form
// (base/), the path every entry under base has as a prefix and the one
// checkPathRule judges, so the rule the check accepts is the rule the filer
// applies to the needles it assigns.
func AssignPathFor(base string) string { return strings.TrimSuffix(base, "/") + "/" }

// ErrPathRule: the chunk path's storage rule would lose or block data.
var ErrPathRule = errors.New("chunkstore: chunk path storage rule refused")

// pathRule is one filer.conf location; the filer writes protojson
// (lowerCamelCase), and reads either name. fsync and worm are kept by
// presence (unset differs from false), as the filer merges them.
type pathRule struct {
	prefix                      string
	ttl                         string
	readOnly, disableChunkDelet bool
	fsync, worm                 *bool
}

func parseFilerConf(body []byte) ([]pathRule, error) {
	var doc struct {
		Version   int                          `json:"version"`
		Locations []map[string]json.RawMessage `json:"locations"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	field := func(m map[string]json.RawMessage, camel, snake string, v any) (bool, error) {
		raw, ok := m[camel]
		if !ok {
			raw, ok = m[snake]
		}
		if !ok {
			return false, nil
		}
		return true, json.Unmarshal(raw, v)
	}
	out := make([]pathRule, 0, len(doc.Locations))
	for _, m := range doc.Locations {
		var r pathRule
		var fsync, worm bool
		var hasFsync, hasWorm bool
		var err error
		for _, step := range []func() error{
			func() error { _, err := field(m, "locationPrefix", "location_prefix", &r.prefix); return err },
			func() error { _, err := field(m, "ttl", "ttl", &r.ttl); return err },
			func() error { _, err := field(m, "readOnly", "read_only", &r.readOnly); return err },
			func() error {
				_, err := field(m, "disableChunkDeletion", "disable_chunk_deletion", &r.disableChunkDelet)
				return err
			},
			func() error { hasFsync, err = field(m, "fsync", "fsync", &fsync); return err },
			func() error { hasWorm, err = field(m, "worm", "worm", &worm); return err },
		} {
			if err := step(); err != nil {
				return nil, err
			}
		}
		// Older files were written with every field present: an explicit
		// false there means unset (worm before version 1, fsync before 2).
		if hasWorm && (worm || doc.Version >= 1) {
			r.worm = &worm
		}
		if hasFsync && (fsync || doc.Version >= 2) {
			r.fsync = &fsync
		}
		out = append(out, r)
	}
	return out, nil
}

// checkPathRule judges the rules for the chunk base directory base, with
// the filer's buckets directory buckets, merged as the filer merges them
// (SeaweedFS 4.53 mergePathConf): every rule whose prefix is a prefix of a
// path applies, shorter first; readOnly and disableChunkDeletion are OR'ed;
// ttl, fsync and worm come from the longest rule that sets them. A rule
// inside the chunk path applies to the entries under it, so it must not
// set ttl, readOnly, disableChunkDeletion or worm, nor turn fsync off.
func checkPathRule(rules []pathRule, base, buckets string) error {
	dir := AssignPathFor(base)
	if b := strings.TrimSuffix(buckets, "/"); b != "" && (base == b || strings.HasPrefix(dir, b+"/")) {
		return fmt.Errorf("%w: %s is under the filer's buckets directory %s (S3 lifecycle can expire it)", ErrPathRule, base, b)
	}
	sort.SliceStable(rules, func(i, j int) bool { return len(rules[i].prefix) < len(rules[j].prefix) })
	var fsync, readOnly, worm, noDelete bool
	ttl := ""
	for _, r := range rules {
		switch {
		case r.prefix != "" && strings.HasPrefix(dir, r.prefix): // applies to the chunk path
			readOnly = readOnly || r.readOnly
			noDelete = noDelete || r.disableChunkDelet
			if r.ttl != "" {
				ttl = r.ttl
			}
			if r.fsync != nil {
				fsync = *r.fsync
			}
			if r.worm != nil {
				worm = *r.worm
			}
		case strings.HasPrefix(r.prefix, dir): // a rule inside the chunk path
			if r.ttl != "" || r.readOnly || r.disableChunkDelet || (r.worm != nil && *r.worm) || (r.fsync != nil && !*r.fsync) {
				return fmt.Errorf("%w: the rule on %s sets ttl %q, readOnly, disableChunkDeletion or worm, or turns fsync off", ErrPathRule, r.prefix, r.ttl)
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
	case noDelete:
		return fmt.Errorf("%w: the rule on %s sets disableChunkDeletion: reclaimed needles would never be deleted", ErrPathRule, base)
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
