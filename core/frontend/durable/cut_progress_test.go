package durable

import (
 "context"
 "sync/atomic"
 "testing"
 "testing/synctest"
 "time"

 "github.com/seaweedfs/seaweed-block/core/storage"
)

// The public layer counters report completed staging even when its cut times out.
type progressLoopLayer struct {
 storage.ChunkLayer
 staged atomic.Uint64
 calls atomic.Int32
}
func (l *progressLoopLayer) Fenced() bool { return false }
func (l *progressLoopLayer) Stats() storage.ChunkLayerStats { return storage.ChunkLayerStats{StagedChunks:l.staged.Load()} }
func (l *progressLoopLayer) Cut(context.Context) (storage.ChunkCutResult,error) {
 l.calls.Add(1)
 l.staged.Add(1)
 return storage.ChunkCutResult{},context.DeadlineExceeded
}

func TestCutLoopProgressRetriesWithoutBackoff(t *testing.T) {
 synctest.Test(t,func(t *testing.T){
  layer:=&progressLoopLayer{}
  p:=&DurableProvider{cfg:ProviderConfig{ChunkCaptureInterval:time.Millisecond}}
  ctx,cancel:=context.WithCancel(context.Background())
  done:=make(chan struct{})
  go p.chunkLoop(ctx,"progress",layer,done,nil)
  defer func(){cancel(); <-done}()
  time.Sleep(500*time.Millisecond)
  synctest.Wait()
  if n:=layer.calls.Load();n<3 {t.Fatalf("only %d attempts despite new staged chunks; progress must retry at the next poll, without backoff or minimum spacing",n)}
 })
}