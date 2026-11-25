#!/usr/bin/env bash
# Development certificates for a Kubernetes alpha install: blockmaster listens
# on the pod network only with mutual TLS. Creates (or reuses) a CA in OUT_DIR,
# issues the blockmaster certificate, one certificate per in-cluster client
# (each with the least role its calls need) and one per server, writes the
# role file, and applies the Secrets the chart and the alpha manifests mount.
# For production, issue the certificates from your own CA (see
# docs/operator-guide.md, "Authenticate the control API").
#
# usage: scripts/k8s-alpha-certs.sh NAMESPACE OUT_DIR SERVER_ID...
#   Server ids issued into OUT_DIR by an earlier run stay in the role file and
#   are applied again (delete OUT_DIR/node-ID.crt to drop one). A leaf that no
#   longer verifies against OUT_DIR's CA (the CA was regenerated) is reissued.
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

# issue NAME CN [SAN]: NAME.crt/NAME.key signed by the CA (reused when present
# and still signed by this CA)
issue() {
  local name="$1" cn="$2" san="${3:-}"
  if [[ -s "$OUT/$name.crt" && -s "$OUT/$name.key" ]] &&
    openssl verify -CAfile "$OUT/ca.crt" "$OUT/$name.crt" >/dev/null 2>&1; then
    return 0
  fi
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
# The in-cluster clients, "name:role": certificate NAME.crt, CN and role-file
# id sw-block-NAME, Secret sw-block-NAME-tls. The CSI controller deletes
# volumes, the failback executor runs failbacks (both admin); the CSI node and
# the status operator only read.
CLIENTS=(csi-controller:admin csi-node:viewer operator-status:viewer failback-executor:admin)
echo "# sw-block alpha role file: sha256:<fingerprint> <role> <id>" >"$OUT/roles"
for c in "${CLIENTS[@]}"; do
  issue "${c%%:*}" "sw-block-${c%%:*}"
  echo "$(fingerprint "$OUT/${c%%:*}.crt") ${c##*:} sw-block-${c%%:*}" >>"$OUT/roles"
done
IDS=("$@")
for f in "$OUT"/node-*.crt; do
  [[ -e "$f" ]] || continue
  old="${f##*/node-}"
  old="${old%.crt}"
  [[ " ${IDS[*]} " == *" $old "* ]] || IDS+=("$old")
done
# a DNS subdomain: dot-separated labels of 1-63 [a-z0-9-], alphanumeric at both ends
LABEL='[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?'
for id in "${IDS[@]}"; do
  if ! [[ "$id" =~ ^$LABEL(\.$LABEL)*$ ]] || ((${#PREFIX} + 1 + ${#id} > 253)); then
    echo "server id $id cannot name a Kubernetes Secret (dot-separated labels of lowercase letters, digits and '-')" >&2
    exit 2
  fi
  # node certificates are client-only (a host dials blockmaster), so no subjectAltName
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
for c in "${CLIENTS[@]}"; do
  apply_tls "sw-block-${c%%:*}-tls" "${c%%:*}"
done
for id in "${IDS[@]}"; do
  apply_tls "$PREFIX-$id" "node-$id"
done
kubectl -n "$NS" create secret generic sw-block-roles --from-file=roles="$OUT/roles" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "mutual TLS Secrets applied in namespace $NS (CA and keys in $OUT)"
