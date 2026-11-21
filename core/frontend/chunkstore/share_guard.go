package chunkstore

import (
	"context"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ShareGuard is told about every sharer of a volume's chunks. A snapshot or
// fork calls Begin before it obtains the HEAD it will reference, and Finish,
// with that HEAD, right before it activates its result; an error from either
// aborts the share before anything is activated. A token Begin returned and
// nobody finished is the guard's to expire.
//
// No guard (the default) changes nothing: no call is made and no check is
// added.
type ShareGuard interface {
	Begin(ctx context.Context, vol string) (token string, err error)
	Finish(ctx context.Context, vol, token string, head storage.ChunkHead) error
}

// SetShareGuard installs g (nil removes it). Set it before the catalog is
// used.
func (c *Catalog) SetShareGuard(g ShareGuard) { c.guard = g }

// ShareGuard returns the installed guard, or nil.
func (c *Catalog) ShareGuard() ShareGuard { return c.guard }

// beginShare starts a share of vol's chunks for a caller that passes the
// HEAD in: with a guard, it begins the share, then refuses unless head is
// still vol's current HEAD (a share that obtained an older HEAD could
// reference chunks superseded meanwhile). token is "" without a guard.
func (c *Catalog) beginShare(ctx context.Context, vol string, head storage.ChunkHead) (token string, err error) {
	if c.guard == nil {
		return "", nil
	}
	token, err = c.guard.Begin(ctx, vol)
	if err != nil {
		return "", remoteErr(err, "share guard for %s", vol)
	}
	cur, ok, err := c.ReadHead(ctx, vol)
	if err != nil {
		return "", err
	}
	if !ok || cur.Gen != head.Gen || cur.Epoch != head.Epoch {
		return "", catErr(CodeOutcomeUnknown, true, nil, "volume %s: the HEAD to share (gen %d) is no longer current (gen %d); retry", vol, head.Gen, cur.Gen)
	}
	return token, nil
}

// finishShare finishes a share before its result is activated; no-op
// without a guard.
func (c *Catalog) finishShare(ctx context.Context, vol, token string, head storage.ChunkHead) error {
	if c.guard == nil {
		return nil
	}
	if err := c.guard.Finish(ctx, vol, token, head); err != nil {
		return remoteErr(err, "share guard for %s refused the share", vol)
	}
	return nil
}
