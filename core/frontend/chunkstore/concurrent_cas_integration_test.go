//go:build chunkstore_integration

package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// concurrentCASWeedPins are the filer builds this test is pinned to, by
// binary SHA-256: the mono build it was written against and stock SeaweedFS
// 4.53 (4ab66fb7), the version the product supports.
var concurrentCASWeedPins = map[string]string{
	"5fafd54e10ee307a7fd7246ca6763d0069cefbef47f9a86ce722f304b162da7b": "mono 8b582399d",
	"fa936e61bfb11f33c1f823045c4007e7da4b8b38bb6adc3b970f132d832e805b": "stock 4.53 4ab66fb7",
}

type casRPCResult struct {
	index int
	code  codes.Code
}
type casRPCGate struct {
	filerpb.SeaweedFilerClient
	index     int
	arrived   chan<- int
	release   <-chan struct{}
	responses chan<- casRPCResult
	expected  storage.ChunkHeadVersion
}

// This gate does not implement CAS: every operation delegates to a real filer
// RPC. It holds updates AFTER all client pre-reads so a client-side refusal
// cannot masquerade as evidence of the server's conditional-write contract.
func (c *casRPCGate) UpdateEntry(ctx context.Context, in *filerpb.UpdateEntryRequest, opts ...grpc.CallOption) (*filerpb.UpdateEntryResponse, error) {
	if in.Entry.GetName() != headName || string(in.ExpectedExtended[extGen]) != strconv.FormatUint(c.expected.Gen, 10) || string(in.ExpectedExtended[extEpoch]) != strconv.FormatUint(c.expected.Epoch, 10) {
		return nil, fmt.Errorf("unexpected HEAD CAS precondition: gen=%q epoch=%q", in.ExpectedExtended[extGen], in.ExpectedExtended[extEpoch])
	}
	select {
	case c.arrived <- c.index:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	resp, err := c.SeaweedFilerClient.UpdateEntry(ctx, in, opts...)
	c.responses <- casRPCResult{c.index, status.Code(err)}
	return resp, err
}

func requireConcurrentCASWeed(t *testing.T) {
	t.Helper()
	binary := os.Getenv("CHUNKSTORE_WEED")
	if binary == "" {
		t.Skip("CHUNKSTORE_WEED not set")
	}
	f, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprintf("%x", h.Sum(nil))
	build, ok := concurrentCASWeedPins[got]
	if !ok {
		t.Fatalf("weed must be a pinned build: SHA256=%s want one of %v", got, concurrentCASWeedPins)
	}
	t.Logf("real filer binary SHA256=%s (%s)", got, build)
}

func TestIntegration_ConcurrentHEADCASRealFiler(t *testing.T) {
	requireConcurrentCASWeed(t)
	cluster := startWeed(t)
	const root = "/swblock/chunks/concurrent-cas"
	reader := cluster.remote(t, root)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	initial := storage.ChunkHead{Epoch: 1, Holder: "initial", RemoteLSN: 1, State: storage.ChunkHeadActive, Chunks: map[uint64]storage.ChunkRef{}}
	if gen, err := reader.CASHead(ctx, storage.ChunkHeadVersion{}, initial); err != nil || gen != 1 {
		t.Fatalf("seed: gen=%d err=%v", gen, err)
	}
	// Multiple expected values exercise generation advancement, not just create.
	for round := 0; round < 4; round++ {
		t.Run(fmt.Sprintf("generation-%d", round+1), func(t *testing.T) {
			current, err := reader.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expected := storage.ChunkHeadVersion{Gen: current.Gen, Epoch: current.Epoch}
			const n = 16
			roundCtx, roundCancel := context.WithTimeout(ctx, 30*time.Second)
			arrived := make(chan int, n)
			release := make(chan struct{})
			responses := make(chan casRPCResult, n)
			var once sync.Once
			var wg sync.WaitGroup
			joined := make(chan struct{})
			type result struct {
				index int
				gen   uint64
				err   error
			}
			results := make(chan result, n)
			proposals := make([]storage.ChunkHead, n)
			// Construct clients on the test goroutine: each has its own real connection.
			remotes := make([]*ChunkRemote, n)
			for i := 0; i < n; i++ {
				client := cluster.client(t, root)
				client.fc = &casRPCGate{SeaweedFilerClient: client.fc, index: i, arrived: arrived, release: release, responses: responses, expected: expected}
				remotes[i], err = NewSWFSChunkRemote(client, root)
				if err != nil {
					t.Fatal(err)
				}
				proposals[i] = storage.ChunkHead{Epoch: current.Epoch, Holder: fmt.Sprintf("writer-%d-%d", round, i), RemoteLSN: current.RemoteLSN + uint64(i) + 1, State: storage.ChunkHeadActive, Chunks: map[uint64]storage.ChunkRef{}}
			}
			defer func() {
				roundCancel()
				once.Do(func() { close(release) })
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("CAS callers did not join after cancellation")
				}
			}()
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					gen, err := remotes[i].CASHead(roundCtx, expected, proposals[i])
					results <- result{i, gen, err}
				}(i)
			}
			go func() { wg.Wait(); close(joined) }()
			seen := map[int]bool{}
			for i := 0; i < n; i++ {
				select {
				case id := <-arrived:
					if seen[id] {
						t.Fatalf("duplicate RPC admission %d", id)
					}
					seen[id] = true
				case <-roundCtx.Done():
					t.Fatalf("only %d/%d reached actual UpdateEntry after pre-read: %v", len(seen), n, roundCtx.Err())
				}
			}
			once.Do(func() { close(release) })
			winners := []int{}
			losers := 0
			for i := 0; i < n; i++ {
				select {
				case r := <-results:
					if r.err == nil {
						if r.gen != expected.Gen+1 {
							t.Fatalf("winner gen=%d want %d", r.gen, expected.Gen+1)
						}
						winners = append(winners, r.index)
					} else if errors.Is(r.err, storage.ErrChunkHeadPrecondition) {
						losers++
					} else {
						t.Fatalf("caller %d has non-conflict failure: %v", r.index, r.err)
					}
				case <-roundCtx.Done():
					t.Fatal("CAS results timed out", roundCtx.Err())
				}
			}
			if len(winners) != 1 || losers != n-1 {
				t.Fatalf("same expected %+v: winners=%v typed conflicts=%d; want 1 and %d", expected, winners, losers, n-1)
			}
			rpcOK, rpcConflict := 0, 0
			rpcSeen := map[int]bool{}
			for i := 0; i < n; i++ {
				select {
				case r := <-responses:
					if rpcSeen[r.index] {
						t.Fatalf("duplicate RPC response %d", r.index)
					}
					rpcSeen[r.index] = true
					switch r.code {
					case codes.OK:
						rpcOK++
						if r.index != winners[0] {
							t.Fatalf("RPC winner %d != caller winner %d", r.index, winners[0])
						}
					case codes.FailedPrecondition:
						rpcConflict++
					default:
						t.Fatalf("unexpected real RPC status %s", r.code)
					}
				case <-roundCtx.Done():
					t.Fatal("missing real RPC result", roundCtx.Err())
				}
			}
			if rpcOK != 1 || rpcConflict != n-1 {
				t.Fatalf("real RPC OK=%d FailedPrecondition=%d", rpcOK, rpcConflict)
			}
			winner := proposals[winners[0]]
			winner.Gen = expected.Gen + 1
			wantEntry, err := headEntry(winner)
			if err != nil {
				t.Fatal(err)
			}
			// An independent client verifies exact winner bytes, not just generation.
			fresh := cluster.client(t, root)
			gotEntry, err := fresh.LookupEntry(roundCtx, root, headName)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(gotEntry.Content, wantEntry.Content) {
				t.Fatalf("final HEAD does not equal winner %d", winners[0])
			}
			for _, key := range []string{extGen, extEpoch, extHolder} {
				if !bytes.Equal(gotEntry.Extended[key], wantEntry.Extended[key]) {
					t.Fatalf("winner xattr %s mismatch", key)
				}
			}
			got, err := reader.GetHead(roundCtx)
			if err != nil || got.Gen != winner.Gen || got.Epoch != winner.Epoch || got.Holder != winner.Holder || got.RemoteLSN != winner.RemoteLSN {
				t.Fatalf("decoded final HEAD=%+v err=%v want winner %+v", got, err, winner)
			}
			t.Logf("expected gen=%d epoch=%d: %d actual RPCs, winner=%d gen=%d, %d typed server/caller conflicts, final HEAD exact", expected.Gen, expected.Epoch, n, winners[0], winner.Gen, losers)
		})
	}
}
