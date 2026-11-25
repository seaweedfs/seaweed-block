#!/usr/bin/env bash
# Development certificates for a Kubernetes alpha install: blockmaster listens
# on the pod network only with mutual TLS. Creates (or reuses) a CA in OUT_DIR,
# issues the blockmaster, controller and per-server node certificates, writes
# the role file, and applies the Secrets the chart and the alpha manifests
# mount. For production, issue the certificates from your own CA (see
# docs/operator-guide.md, "Authenticate the control API").
#
# usage: scripts/k8s-alpha-certs.sh NAMESPACE OUT_DIR SERVER_ID...
#   SW_BLOCK_TLS_SECRET_PREFIX   node Secret prefix (default sw-block-node-tls)
#   SW_BLOCK_TLS_APPLY=0         write the files only, apply nothing
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 NAMESPACE OUT_DIR SERVER_ID..." >&2
  exit 2
fi
NS="$1"
OUT="$2"
shift 2
PREFIX="${SW_BLOCK_TLS_SECRET_PREFIX:-sw-block-node-tls}"
APPLY="${SW_BLOCK_TLS_APPLY:-1}"
mkdir -p "$OUT"
umask 077

if [[ ! -s "$OUT/ca.crt" || ! -s "$OUT/ca.key" ]]; then
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
    -subj "/CN=sw-block alpha CA" -keyout "$OUT/ca.key" -out "$OUT/ca.crt" 2>/dev/null
fi

# issue NAME CN [SAN]: NAME.crt/NAME.key signed by the CA (reused when present)
issue() {
  local name="$1" cn="$2" san="${3:-}"
  [[ -s "$OUT/$name.crt" && -s "$OUT/$name.key" ]] && return 0
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=$cn" \
    -keyout "$OUT/$name.key" -out "$OUT/$name.csr" 2>/dev/null
  local ext="$OUT/$name.ext"
  : >"$ext"
  [[ -n "$san" ]] && echo "subjectAltName=$san" >"$ext"
  openssl x509 -req -in "$OUT/$name.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
    -days 825 -extfile "$ext" -out "$OUT/$name.crt" 2>/dev/null
  rm -f "$OUT/$name.csr" "$ext"
}

fingerprint() {
  echo "sha256:$(openssl x509 -in "$1" -outform DER | sha256sum | cut -d' ' -f1)"
}

issue master blockmaster "DNS:blockmaster.$NS.svc.cluster.local,DNS:blockmaster.$NS.svc,DNS:blockmaster,IP:127.0.0.1"
issue controller sw-block-controller
{
  echo "# sw-block alpha role file: sha256:<fingerprint> <role> <id>"
  echo "$(fingerprint "$OUT/controller.crt") admin sw-block-controller"
} >"$OUT/roles"
for id in "$@"; do
  if ! [[ "$id" =~ ^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$ ]]; then
    echo "server id $id cannot name a Kubernetes Secret (lowercase letters, digits, '-' and '.')" >&2
    exit 2
  fi
  issue "node-$id" "$id"
  echo "$(fingerprint "$OUT/node-$id.crt") node $id" >>"$OUT/roles"
done

[[ "$APPLY" == "0" ]] && { echo "certificates and role file written to $OUT (not applied)"; exit 0; }

apply_tls() {
  local secret="$1" name="$2"
  kubectl -n "$NS" create secret generic "$secret" \
    --from-file=tls.crt="$OUT/$name.crt" --from-file=tls.key="$OUT/$name.key" --from-file=ca.crt="$OUT/ca.crt" \
    --dry-run=client -o yaml | kubectl apply -f -
}
apply_tls sw-block-master-tls master
apply_tls sw-block-client-tls controller
for id in "$@"; do
  apply_tls "$PREFIX-$id" "node-$id"
done
kubectl -n "$NS" create secret generic sw-block-roles --from-file=roles="$OUT/roles" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "mutual TLS Secrets applied in namespace $NS (CA and keys in $OUT)"
