#!/usr/bin/env bash
# Regenerate gRPC Go bindings for core/rpc/proto/control.proto.
# Run via WSL (Ubuntu) because Windows-side Go toolchain is 1.23+
# but protoc + protoc-gen-go + protoc-gen-go-grpc live in WSL.
#   wsl.exe -d Ubuntu -- bash -lc 'cd /mnt/c/work/seaweed_block && bash scripts/genproto.sh'

set -euo pipefail
# Use a narrow PATH to avoid Windows $PATH fragments with parentheses
# (e.g. "Program Files (x86)") breaking bash when the env is inherited
# from Windows under wsl.exe.
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$HOME/go/bin"

mkdir -p core/rpc/control
protoc \
  --proto_path=core/rpc/proto \
  --go_out=core/rpc/control \
  --go_opt=paths=source_relative \
  --go-grpc_out=core/rpc/control \
  --go-grpc_opt=paths=source_relative \
  core/rpc/proto/control.proto

echo "Generated:"
find core/rpc/control -type f -name '*.pb.go'
