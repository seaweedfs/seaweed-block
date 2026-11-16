package main

// Workspace CLI is a JSON adapter over the master RPCs. It never creates an
// assignment locally, retries a mutation, or substitutes static cluster files.
import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func workspaceFail(stdout, stderr io.Writer, exit int, code, message string, retryable bool) int {
	fmt.Fprintf(stderr, "sw-block ops workspace: %s: %s\n", code, message)
	b, _ := json.Marshal(chunkErrBody{Code: code, Message: message, Retryable: retryable})
	if e := writeAll(stdout, append(b, '\n')); e != nil {
		fmt.Fprintf(stderr, "sw-block ops workspace: write error result: %v\n", e)
		return 2
	}
	return exit
}
func runOpsWorkspace(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	bad := func(message string) int { return workspaceFail(stdout, stderr, 1, "bad_request", message, false) }
	if len(args) == 0 {
		return bad("expected bind|place|move|journal")
	}
	op := args[0]
	var request proto.Message
	switch op {
	case "bind":
		request = &control.WorkspaceBinding{}
	case "place":
		request = &control.WorkspacePlacement{}
	case "move":
		request = &control.WorkspaceMove{}
	case "journal":
		request = &control.WorkspaceVolume{}
	default:
		return bad("unknown operation " + op)
	}
	fs := flag.NewFlagSet("sw-block ops workspace "+op, flag.ContinueOnError)
	fs.SetOutput(stderr)
	master := fs.String("master", "", "blockmaster gRPC host:port")
	input := fs.String("input", "-", "request JSON file, or - for stdin")
	timeout := fs.Duration("timeout", 30*time.Second, "RPC deadline (positive, at most 5m)")
	volume := fs.String("volume", "", "journal volume id")
	requestID := fs.String("request-id", "", "journal request id filter")
	force := fs.Bool("force", false, "explicitly permit a FORCED move; master still requires expired lease")
	if e := fs.Parse(args[1:]); e != nil {
		return bad(e.Error())
	}
	if fs.NArg() != 0 || strings.TrimSpace(*master) == "" || *input == "" || *timeout <= 0 || *timeout > 5*time.Minute {
		return bad("--master and one JSON request required; timeout must be >0 and <=5m; no positional arguments")
	}
	if *force && op != "move" {
		return bad("--force is only valid for move")
	}
	if op != "journal" && (*volume != "" || *requestID != "") {
		return bad("--volume/--request-id are journal-only")
	}
	reader := stdin
	if *input != "-" {
		f, e := os.Open(*input)
		if e != nil {
			return bad("read input: " + e.Error())
		}
		defer f.Close()
		reader = f
	}
	const limit = 64 << 10
	var body []byte
	var e error
	if op == "journal" && *volume != "" {
		if *input != "-" {
			return bad("journal flags cannot be combined with --input")
		}
		body, e = protojson.Marshal(&control.WorkspaceVolume{VolumeId: *volume, RequestId: *requestID})
	} else {
		if *requestID != "" {
			return bad("--request-id requires --volume")
		}
		body, e = io.ReadAll(io.LimitReader(reader, limit+1))
	}
	if e != nil {
		return bad("read input: " + e.Error())
	}
	if len(body) > limit {
		return bad("request exceeds 64 KiB")
	}
	if e = protojson.Unmarshal(body, request); e != nil {
		return bad("request JSON: " + e.Error())
	}
	switch q := request.(type) {
	case *control.WorkspaceBinding:
		if q.RequestId == "" || q.VolumeId == "" || q.ServerId == "" || q.DataAddr == "" || q.CtrlAddr == "" || q.Retired {
			return bad("bind requires requestId, volumeId, serverId, dataAddr, ctrlAddr; retired is output-only")
		}
	case *control.WorkspacePlacement:
		if q.RequestId == "" || q.VolumeId == "" || q.ServerId == "" || q.ReplicaId == "" {
			return bad("place requires requestId, volumeId, serverId, replicaId")
		}
	case *control.WorkspaceMove:
		if *force {
			if q.Kind != "" && q.Kind != "forced" {
				return bad("--force conflicts with clean kind")
			}
			q.Kind = "forced"
			q.Force = true
		} else {
			if q.Force || q.Kind != "" && q.Kind != "clean" {
				return bad("forced move requires explicit --force")
			}
			q.Kind = "clean"
		}
		if q.RequestId == "" || q.VolumeId == "" || q.ExpectedReplicaId == "" || q.ExpectedEpoch == 0 || q.TargetReplicaId == "" || q.TargetServerId == "" || q.TargetDataAddr == "" || q.TargetCtrlAddr == "" {
			return bad("move requires requestId, volumeId, expectedReplicaId, expectedEpoch and complete target replica/server/pair")
		}
	case *control.WorkspaceVolume:
		if q.VolumeId == "" {
			return bad("journal requires volumeId")
		}
	}
	conn, e := grpc.NewClient(*master, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if e != nil {
		return bad(e.Error())
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client := control.NewWorkspaceServiceClient(conn)
	var reply proto.Message
	switch q := request.(type) {
	case *control.WorkspaceBinding:
		reply, e = client.RegisterWorkspaceBinding(ctx, q)
	case *control.WorkspacePlacement:
		reply, e = client.PlaceWorkspace(ctx, q)
	case *control.WorkspaceMove:
		reply, e = client.MoveWorkspace(ctx, q)
	case *control.WorkspaceVolume:
		reply, e = client.QueryWorkspace(ctx, q)
	}
	if e != nil {
		code, retry := "internal", false
		switch status.Code(e) {
		case codes.InvalidArgument:
			code = "bad_request"
		case codes.FailedPrecondition:
			code = "failed_precondition"
		case codes.NotFound:
			code = "not_found"
		case codes.AlreadyExists:
			code = "already_exists"
		case codes.PermissionDenied:
			code = "permission_denied"
		case codes.Unauthenticated:
			code = "unauthenticated"
		case codes.Aborted:
			code, retry = "conflict", true
		case codes.ResourceExhausted:
			code, retry = "resource_exhausted", true
		case codes.Unavailable:
			code, retry = "remote_unavailable", true
		case codes.DeadlineExceeded, codes.Canceled:
			code, retry = "outcome_unknown", true
			if op == "journal" {
				code = "remote_unavailable"
			}
		}
		exit := 1
		switch status.Code(e) {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown, codes.Internal, codes.DataLoss:
			exit = 2
		}
		return workspaceFail(stdout, stderr, exit, code, status.Convert(e).Message(), retry)
	}
	out, e := protojson.Marshal(reply)
	if e != nil {
		return workspaceFail(stdout, stderr, 2, "internal", e.Error(), false)
	}
	if e = writeAll(stdout, append(out, '\n')); e != nil {
		fmt.Fprintf(stderr, "sw-block ops workspace: write result: %v\n", e)
		return 2
	}
	return 0
}
