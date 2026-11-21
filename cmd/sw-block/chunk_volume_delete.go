package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
)

// chunkVolumeDelete: sw-block ops chunk volume-delete <vol> --chunk-filer
// host:port --master host:port deletes a chunk volume (Catalog.DeleteVolume)
// after the blockmaster admits it (MarkVolumeDeleting: refused while the
// volume has live or pending authority; afterwards nothing assigns, places
// or binds it again). A failure prints what was done with the error.
func chunkVolumeDelete(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("volume-delete", stderr)
	var (
		volumeID, filer, base, master, requestID string
		timeout                                  time.Duration
	)
	fs.StringVar(&requestID, "request-id", "", "this delete's operation id; a rerun with the same id resumes (or gives back) its earlier admission")
	fs.StringVar(&volumeID, "volume", "", "volume id (or the first argument)")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.StringVar(&master, "master", "", "authoritative blockmaster gRPC address (admits the delete)")
	fs.DurationVar(&timeout, "timeout", time.Minute, "bound for this delete")
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" && fs.NArg() == 1 {
		volumeID = fs.Arg(0)
	} else if fs.NArg() != 0 {
		return chunkFail("volume-delete", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "give the volume once, as an argument or --volume", false)
	}
	if volumeID == "" || master == "" || requestID == "" || timeout <= 0 {
		return chunkFail("volume-delete", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "a volume, --master and --request-id are required; --timeout must be positive", false)
	}
	cat, closeFn, exit := chunkCatalog("volume-delete", filer, base, true, timeout, stdout, stderr)
	if exit >= 0 {
		return exit
	}
	defer closeFn()
	conn, err := link.Dial(master)
	if err != nil {
		return chunkResult("volume-delete", nil, err, stdout, stderr)
	}
	defer conn.Close()
	admit := &masterAdmission{lc: control.NewLifecycleServiceClient(conn), token: requestID}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := cat.DeleteVolume(ctx, volumeID, admit)
	if err != nil {
		return volumeDeletePartial(res, err, stdout, stderr)
	}
	return chunkResult("volume-delete", res, nil, stdout, stderr)
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

// volumeDeletePartial prints what a failed delete did, with the error, and
// fails.
func volumeDeletePartial(res chunkstore.VolumeDeleteResult, err error, stdout, stderr io.Writer) int {
	body := chunkErrBody{Code: codeInternalCLI, Message: err.Error()}
	var ce *chunkstore.CatalogError
	if errors.As(err, &ce) {
		body = chunkErrBody{Code: ce.Code, Message: ce.Detail(), Retryable: ce.Retryable}
	}
	fmt.Fprintf(stderr, "sw-block ops chunk volume-delete: %s: %s (headMarked=%t completed=%t uncertain=%t)\n", body.Code, body.Message, res.HeadMarked, res.Completed, res.Uncertain)
	out, mErr := json.Marshal(struct {
		chunkstore.VolumeDeleteResult
		Error *chunkErrBody `json:"error"`
	}{res, &body})
	if mErr == nil {
		if wErr := writeAll(stdout, append(out, '\n')); wErr != nil {
			fmt.Fprintf(stderr, "sw-block ops chunk volume-delete: write the result: %v\n", wErr)
		}
	}
	return chunkExitFailed
}
