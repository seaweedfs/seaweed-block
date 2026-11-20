package storage

import (
 "bytes"
 "context"
 "testing"
)

// Exercise the owner-facing optional seam without changing ChunkLayer's
// existing interface or the remote protocol.
func TestPrecopyDetachConvergesBeforeStoppingIO(t *testing.T) {
 s, f, l, _ := precopyLayer(t, 1024, 2)
 type precopy interface {
  PrecopyDetach(context.Context, func() bool) (ChunkCutResult, error)
 }
 owner, ok := any(l).(precopy)
 if !ok { t.Fatal("chunk layer cannot converge rolling cuts before final detach") }
 frozen := false
 r, err := owner.PrecopyDetach(ctTimeout(t), func() bool { frozen = true; return true })
 if err != nil { t.Fatal(err) }
 if !frozen || r.Head.State != ChunkHeadDetached { t.Fatalf("frozen=%v state=%s", frozen, r.Head.State) }
 got := f.imageOf(t, r.Head)
 want := make([]byte, len(got))
 for i := uint32(0); i < ctNumBlocks; i++ {
  b := make([]byte, ctBlockSize)
  if _, err := s.Read(i, b); err != nil { t.Fatal(err) }
  copy(want[int(i)*ctBlockSize:], b)
 }
 if !bytes.Equal(got, want) { t.Fatal("detached HEAD differs from local image") }
 assertNoIntentsLeft(t, f)
}
