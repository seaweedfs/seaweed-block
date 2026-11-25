package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
)

// chunkVolumeDelete: sw-block ops chunk volume-delete --volume v
// --volume-layout-id id --chunk-filer host:port --master host:port
// --request-id r deletes a volume in the layout kernel's order after the
// blockmaster admits it (MarkVolumeDeleting: refused while the volume has
// a live or pending authority line; from then on nothing assigns, places
// or binds it again). The admission is committed once HEAD is deleted,
// and given back if the delete is refused before HEAD is marked deleting.
func chunkVolumeDelete(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("volume-delete", time.Minute, stdout, stderr)
	var vol volumeFlags
	var master, requestID string
	vol.bind(c.fs, "volume", "volume")
	c.fs.StringVar(&master, "master", "", "authoritative blockmaster gRPC address (admits the delete)")
	c.fs.StringVar(&requestID, "request-id", "", "this delete's operation id; a rerun with the same id resumes its admission")
	var ref chunkstore.VolumeRef
	return c.run(args, true, func() (err error) {
		if master == "" || requestID == "" || c.timeout <= 0 {
			return errors.New("--master and --request-id are required; --timeout must be positive")
		}
		ref, err = vol.ref("volume")
		return err
	}, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		conn, err := link.Dial(master)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		auth := &masterAuthority{masterAdmission: masterAdmission{lc: control.NewLifecycleServiceClient(conn), token: requestID}, vol: ref.Name}
		if err := cat.DeleteVolume(ctx, ref, auth); err != nil {
			if h, found, rerr := cat.ReadHead(ctx, ref); rerr == nil && found && h.State == layout.StateLive && auth.reserved {
				if aerr := auth.Abort(ctx, ref.Name); aerr != nil {
					return nil, fmt.Errorf("%w (giving back the admission: %v)", err, aerr)
				}
			}
			return nil, err
		}
		if err := auth.Commit(ctx, ref.Name); err != nil {
			return nil, fmt.Errorf("volume deleted; commit the admission (rerun with the same --request-id): %w", err)
		}
		return map[string]string{"volume": ref.Dir(), "state": "deleted"}, nil
	})
}

// masterAuthority is the delete authority at the blockmaster: the
// reservation is MarkVolumeDeleting, which refuses while any authority
// line is live or pending and after which nothing is granted for the
// volume, so once this run's admission succeeded no live lease can appear.
type masterAuthority struct {
	masterAdmission
	vol      string
	reserved bool
}

func (m *masterAuthority) Reserve(ctx context.Context) error {
	if err := m.Admit(ctx, m.vol); err != nil {
		return err
	}
	m.reserved = true
	return nil
}

func (m *masterAuthority) NoLiveLease(context.Context) error {
	if !m.reserved {
		return errors.New("the blockmaster has not admitted this delete")
	}
	return nil
}

// masterAdmission is the delete's reservation at the blockmaster, held
// under the operation's request id: a rerun with the same id adopts the
// hold an earlier run left (e.g. after a lost response), so its give-back
// releases it, while another operation's hold stays.
type masterAdmission struct {
	lc    control.LifecycleServiceClient
	token string
}

func (m *masterAdmission) req(vol string) *control.MarkVolumeDeletingRequest {
	return &control.MarkVolumeDeletingRequest{VolumeId: vol, Token: m.token}
}

// admissionErr: FailedPrecondition and InvalidArgument are definite
// refusals; anything else (unavailable, deadline, a lost response) leaves
// the outcome unknown.
func admissionErr(err error) error {
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.FailedPrecondition, codes.InvalidArgument:
		return fmt.Errorf("%w: %v", chunkstore.ErrNotAdmitted, err)
	}
	return err
}

func (m *masterAdmission) Admit(ctx context.Context, vol string) error {
	_, err := m.lc.MarkVolumeDeleting(ctx, m.req(vol))
	return admissionErr(err)
}

func (m *masterAdmission) Commit(ctx context.Context, vol string) error {
	_, err := m.lc.CommitVolumeDeleting(ctx, m.req(vol))
	return err
}

func (m *masterAdmission) Abort(ctx context.Context, vol string) error {
	_, err := m.lc.AbortVolumeDeleting(ctx, m.req(vol))
	return err
}
