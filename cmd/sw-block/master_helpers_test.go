package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func lifecycleClient(t *testing.T, addr string) control.LifecycleServiceClient {
	t.Helper()
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return control.NewLifecycleServiceClient(c)
}

func startDeleteMaster(t *testing.T, cfg master.Config) (*master.Host, func()) {
	t.Helper()
	h, err := master.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	return h, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Close(ctx)
	}
}
