package storage

import "testing"

func TestDiscardStagedRechecksCaptureState(t *testing.T) {
	for _, name := range []string{"idle", "staging_advanced", "cut_advanced", "pending_commit", "fenced", "detached", "closed", "cut_running"} {
		t.Run(name, func(t *testing.T) {
			r := newFakeChunkRemote()
			l := &chunkLayerImpl{remote: r, stats: chunkLayerStats{StagedChunks: 3, CutSeq: 2}, reusable: map[uint64]stagedChunk{1: {}}}
			switch name {
			case "staging_advanced":
				l.stats.StagedChunks++
			case "cut_advanced":
				l.stats.CutSeq++
			case "pending_commit":
				l.pending = &chunkHead{}
			case "fenced":
				l.fenced = true
			case "detached":
				l.detached = &chunkCutResult{}
			case "closed":
				l.closed = true
			case "cut_running":
				l.capMu.Lock()
				defer l.capMu.Unlock()
			}
			if got := l.DiscardStagedIfUnchanged(3, 2); got != (name == "idle") {
				t.Fatalf("discard=%v for %s", got, name)
			}
			if (len(l.reusable) == 0) != (name == "idle") {
				t.Fatal("staged uploads changed on a refused discard")
			}
		})
	}
}
