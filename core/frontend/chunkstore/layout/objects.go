package layout

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ObjectMeta is what an object entry (chunk, segment, root) carries besides
// its bytes: the session of its last writer, a removal mark (the session
// of a cleaning that planned to remove it; empty when unmarked) and the id
// of the volume it belongs to (content names repeat across volumes and
// across a volume's re-creation under its name).
type ObjectMeta struct {
	Session string
	Mark    string
	Volume  VolumeID // the volume the entry belongs to, stamped by the store at Create
}

// ErrExists is returned by ObjectStore.Create when the entry exists.
var ErrExists = errors.New("layout: object exists")

// ObjectStore holds a volume's objects by path relative to the volume
// root. Every change of an existing entry is conditional on its metadata,
// so a late write fails its own condition instead of undoing a newer one.
type ObjectStore interface {
	// Stat returns an entry's metadata; a missing entry is found=false.
	Stat(ctx context.Context, rel string) (meta ObjectMeta, found bool, err error)
	// Create writes a new entry exclusively (data first, entry after),
	// with session as its writer and the store's volume id; ErrExists if
	// the entry exists.
	Create(ctx context.Context, rel string, data []byte, session string) error
	// Reuse sets session as the writer and clears the mark, only if the
	// entry's metadata is still expect (ErrPrecondition otherwise).
	Reuse(ctx context.Context, rel string, expect ObjectMeta, session string) error
	// Mark sets mark, only if the entry's metadata is still expect
	// (ErrPrecondition otherwise).
	Mark(ctx context.Context, rel string, expect ObjectMeta, mark string) error
	// DeleteMarked deletes the entry only if it carries mark
	// (ErrPrecondition otherwise); a missing entry succeeds.
	DeleteMarked(ctx context.Context, rel string, mark string) error
	// DeleteOfVolume deletes the entry only if it belongs to volume
	// (ErrPrecondition otherwise); a missing entry succeeds.
	DeleteOfVolume(ctx context.Context, rel string, volume VolumeID) error
}

// maxEnsureRounds bounds EnsureObject's retries when another writer moves
// the entry between its read and its conditional write.
const maxEnsureRounds = 8

// EnsureObject is transition 3, the write of one object for a session: an
// absent entry is created with the session as writer; an existing one
// (content-named, so already complete) is reused by a conditional update
// that sets the session and clears any mark, so a cleaning's planned mark
// or delete of it fails its own condition from then on. An entry already
// written by this session and unmarked needs nothing.
func EnsureObject(ctx context.Context, os ObjectStore, rel string, data []byte, session string) error {
	if session == "" {
		return errors.New("layout: EnsureObject needs a session")
	}
	for round := 0; round < maxEnsureRounds; round++ {
		meta, found, err := os.Stat(ctx, rel)
		if err != nil {
			return err
		}
		switch {
		case !found:
			err = os.Create(ctx, rel, data, session)
			if errors.Is(err, ErrExists) {
				continue
			}
			return err
		case meta.Session == session && meta.Mark == "":
			return nil
		default:
			err = os.Reuse(ctx, rel, meta, session)
			if errors.Is(err, ErrPrecondition) {
				continue
			}
			return err
		}
	}
	return fmt.Errorf("layout: %s kept moving under %d attempts", rel, maxEnsureRounds)
}

// PrepareRoot writes root for session: every segment it names (segs holds
// their bytes, by name) in shard order, then the root itself, each by
// EnsureObject, so the root is written only after all it names. It returns
// the root's name. A segment the root names that segs lacks is an error.
func PrepareRoot(ctx context.Context, os ObjectStore, root Root, segs map[SegName][]byte, session string) (RootName, error) {
	shards := make([]uint64, 0, len(root.Segments))
	for sh := range root.Segments {
		shards = append(shards, sh)
	}
	sort.Slice(shards, func(a, b int) bool { return shards[a] < shards[b] })
	for _, sh := range shards {
		n := root.Segments[sh]
		b, ok := segs[n]
		if !ok {
			return RootName{}, fmt.Errorf("layout: root names segment %s, its bytes are not given", n.Path())
		}
		if err := EnsureObject(ctx, os, n.Path(), b, session); err != nil {
			return RootName{}, err
		}
	}
	rn, rb, err := NameRoot(root)
	if err != nil {
		return RootName{}, err
	}
	return rn, EnsureObject(ctx, os, rn.Path(), rb, session)
}

// ownedBySession reports whether rel exists, carries session as its writer
// and is unmarked: an object a commit of that session may name.
func ownedBySession(ctx context.Context, os ObjectStore, rel, session string) (bool, error) {
	meta, found, err := os.Stat(ctx, rel)
	if err != nil {
		return false, err
	}
	return found && meta.Session == session && meta.Mark == "", nil
}
