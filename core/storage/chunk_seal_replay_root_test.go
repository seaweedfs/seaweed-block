package storage

import (
	"context"
	"errors"
	"testing"
)

// V1-SPEC r2.38: a seal replay is the sealing root's own: another root of
// the same holder reads the detached proof as fenced; the sealing root and a
// pre-release proof without a root are accepted.
func TestReadDetachedChunkHeadComparesTheLocalRoot(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, headRoot, asker string
		ok                    bool
	}{
		{"another root of the holder", "root1", "root2", false},
		{"the sealing root", "root1", "root1", true},
		{"pre-release proof without a root", "", "root2", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := NewMemChunkRemote()
			gen, err := remote.CASHead(ctx, ChunkHeadVersion{}, ChunkHead{Epoch: 1, Holder: "s/r", HolderRoot: tc.headRoot, State: ChunkHeadActive})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := remote.CASHead(ctx, ChunkHeadVersion{Gen: gen, Epoch: 1}, ChunkHead{Epoch: 1, Holder: "s/r", HolderRoot: tc.headRoot, State: ChunkHeadDetached,
				DetachedBy: &ChunkDetachedBy{Holder: "s/r", HolderRoot: tc.headRoot, Epoch: 1, Gen: gen + 1}}); err != nil {
				t.Fatal(err)
			}
			_, err = ReadDetachedChunkHead(ctx, remote, 1, "s/r", tc.asker)
			if tc.ok && err != nil {
				t.Fatalf("refused: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrChunkLayerFenced) {
				t.Fatalf("err=%v, want fenced", err)
			}
		})
	}
}
