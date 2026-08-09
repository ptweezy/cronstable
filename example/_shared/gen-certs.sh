#!/bin/sh
# Generate a throwaway cluster CA and one leaf cert per node into /certs.
# Shared by every cluster demo's one-shot `certgen` service: each demo's
# docker-compose.yml bind-mounts this script and names its nodes via env.
#
# Env in:
#   NODES    comma/space list of node names; required. Each name is also the
#            cert file name and the TLS Subject Alternative Name the
#            mutual-TLS hostname check pins against when a peer connects to
#            e.g. https://<node>:8443/peer.
#   CA_CN    subject CN for the throwaway CA   (default cronstable-cluster-ca)
#   PROJECT  the demo's directory, used only in the "to regenerate" hint
#            (e.g. example/cluster)
#
# THESE CERTS ARE FOR LOCAL EXPERIMENTATION ONLY: a single CA key sitting in a
# shared volume, 10-year validity. For real deployments provision per-node
# certificates from your own PKI (cert-manager, a service mesh, an internal
# CA); cronstable only consumes them.
set -eu

CERTS=/certs
CA_CN="${CA_CN:-cronstable-cluster-ca}"

if [ -z "${NODES:-}" ]; then
  echo "NODES is required (comma/space list of node names)" >&2
  exit 1
fi
NODES=$(echo "$NODES" | tr ',' ' ')

# Node names become file paths under /certs; reject anything but letters,
# digits, '.', '_' and '-' so a stray env value cannot traverse paths.
for n in $NODES; do
  case "$n" in
    *..* | *[!A-Za-z0-9._-]*)
      echo "invalid node name: $n" >&2
      exit 1 ;;
  esac
done

if [ -f "$CERTS/ca.pem" ]; then
  echo "certs already present in $CERTS; leaving them in place."
  echo "To regenerate: docker compose -f ${PROJECT:-<this demo>}/docker-compose.yml down -v"
  exit 0
fi

# Alpine ships without openssl; install it on first run.
if ! command -v openssl >/dev/null 2>&1; then
  echo "installing openssl..."
  apk add --no-cache openssl >/dev/null
fi

echo "generating cluster CA ($CA_CN)..."
# basicConstraints + keyUsage are required: OpenSSL 3.x strict verification
# rejects a CA cert that lacks the keyCertSign key-usage extension.
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout "$CERTS/ca.key" -out "$CERTS/ca.pem" \
  -subj "/CN=$CA_CN" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

for n in $NODES; do
  echo "generating certificate for $n..."
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$CERTS/$n.key" -out "/tmp/$n.csr" \
    -subj "/CN=$n"
  # SAN = the service name (peer hostname verification pins against it); the
  # cert both serves /peer (serverAuth) and authenticates as a client when
  # polling peers (clientAuth).
  cat > "/tmp/$n.ext" <<EOF
subjectAltName=DNS:$n
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
EOF
  openssl x509 -req -in "/tmp/$n.csr" \
    -CA "$CERTS/ca.pem" -CAkey "$CERTS/ca.key" -CAcreateserial \
    -out "$CERTS/$n.pem" -days 3650 -extfile "/tmp/$n.ext"
done

# Lock down permissions. The cronstable containers run as uid 65534 (nobody)
# and must read their own leaf key, so hand the private keys to that uid and
# keep them owner-only (0600) instead of world-readable.
#
# This matters especially for the CA SIGNING key (ca.key): anyone who can read
# it can mint a cert for ANY node/SAN and impersonate a peer, defeating the
# whole mTLS scheme. The public material (ca.pem, the node *.pem certs) is
# meant to be shared, so it stays 0644.
#
# DEMO-ONLY CAVEAT: ca.key exists in this shared volume only because the demo
# mints certs in place. In production never ship or mount the CA signing key to
# the nodes; they only need their own leaf key+cert and the CA *public* cert
# (ca.pem) to verify peers; ca.key belongs only on the (offline) signer.
chmod 0644 "$CERTS"/*.pem
chown 65534:65534 "$CERTS"/*.key
chmod 0600 "$CERTS"/*.key
echo "done. generated certs for: $NODES"
