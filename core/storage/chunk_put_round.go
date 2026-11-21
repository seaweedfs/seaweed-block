package storage

import (
	"context"
	"errors"
)

// ErrChunkPutFinal marks a Put failure no retry can change (for example a
// rejected token): the caller stops retrying the object.
var ErrChunkPutFinal = errors.New("storage: chunk upload refused for good")

// ChunkPutRounds is an optional ChunkRemote capability: the objects of one
// upload round share remote reservations (batch assign) instead of taking
// one each. The round belongs to the cut or staging round that begins it and
// ends with it; it starts no goroutine of its own, so joining the round's
// workers joins every call it made.
type ChunkPutRounds interface {
	BeginPutRound(n, objSize int) ChunkPutRound
}

// ChunkPutRound stores object i (0 <= i < n) of its round under key, with
// Put's rules: a bound key is never rebound (ErrChunkKeyExists). A failed Put
// may be retried with the same i and bytes.
type ChunkPutRound interface {
	Put(ctx context.Context, i int, key string, data []byte) error
}

// beginPutRound returns the put for a round of n objects: the remote's
// round when it has one, otherwise one Put per object.
func (l *chunkLayerImpl) beginPutRound(n int) func(ctx context.Context, i int, key string, data []byte) error {
	if rounds, ok := l.remote.(ChunkPutRounds); ok && n > 0 {
		return rounds.BeginPutRound(n, int(uint64(l.blockSize)*l.chunkBlocks)).Put
	}
	return func(ctx context.Context, _ int, key string, data []byte) error {
		return l.remote.Put(ctx, key, data)
	}
}
