package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc"
)

// Automatic GC stays default-off until restart-safe unknown-CAS settlement and
// the 50/200-volume root-scan measurement both pass. Retention remains enabled.
// The CLI fixes the maximum cut/copy lifetime at five minutes and the HEAD
// hydration reader at thirty seconds. A one-minute mark fits strictly inside
// the one-hour grace. No elapsed TTL alone authorizes capture-intent expiry.
func newChunkMaintenance(cat *chunkstore.Catalog, blobs chunkstore.BlobDeleter, collect bool, master string, masterTLS grpc.DialOption) (func(context.Context, string) error, func() error, error) {
	closeFn := func() error { return nil }
	cfg := chunkstore.GCConfig{Grace: defaultChunkGCGrace, IntentTTL: defaultChunkIntentTTL}
	if collect {
		if master == "" {
			return nil, closeFn, errors.New("automatic chunk GC requires blockmaster authority")
		}
		if defaultChunkGCGrace <= max(defaultChunkCutTimeout, chunkstore.DefaultOpDeadline)+defaultChunkMaintenanceTimeout {
			return nil, closeFn, errors.New("automatic GC grace does not cover cut/copy and mark bounds")
		}
		conn, err := link.DialMaster(master, masterTLS)
		if err != nil {
			return nil, closeFn, err
		}
		closeFn = conn.Close
		evidence := control.NewEvidenceServiceClient(conn)
		cfg.IssuedEpoch = func(ctx context.Context, vol string) (uint64, error) {
			reply, err := evidence.QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: vol})
			if err != nil {
				return 0, err
			}
			if reply.GetVolumeId() != vol || !reply.GetAssigned() || reply.GetEpoch() == 0 {
				return 0, fmt.Errorf("blockmaster has no issued authority for %s", vol)
			}
			return reply.Epoch, nil
		}
	}
	return func(ctx context.Context, vol string) error {
		retained, err := cat.Retain(ctx, vol, false)
		if err != nil {
			return fmt.Errorf("retain deleted=%d not_attempted=%d: %w", len(retained.Deleted), len(retained.NotAttempted), err)
		}
		if !collect {
			return nil
		}
		out, err := cat.ExecuteVolumeGC(ctx, vol, blobs, cfg, true)
		log.Printf("blockvolume: maintenance volume=%s mark=%s reclaimed=%d settled=%d unknown=%v", vol, out.MarkOutcome, out.Reclaimed, out.SettledCopies, out.UnknownOutcomes)
		return err
	}, closeFn, nil
}
