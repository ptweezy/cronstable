# Benchmark TLS fixtures

Throwaway, benchmark-only TLS material for `cluster.*` metrics.
`ClusterManager` construction eagerly builds its mTLS contexts, so the
fixture needs real parseable cert/key files; these are pre-minted with a
100-year validity so the suite never starts failing on expiry, and checked
in so both sides of the paired CI run load identical bytes.

The private key here protects NOTHING: it is never used on any network
(the benchmark never calls `start()`), it signs nothing real, and the CA
is a single-purpose throwaway generated for this directory.  Do not reuse
any of it outside the benchmark suite.

Regenerate (openssl, EC P-256):

```sh
openssl ecparam -name prime256v1 -genkey -noout -out ca-key.pem
openssl req -x509 -new -key ca-key.pem -days 36500 \
    -subj "/CN=cronstable-bench-ca" -out bench-ca.pem
openssl ecparam -name prime256v1 -genkey -noout -out bench-node-key.pem
openssl req -new -key bench-node-key.pem -subj "/CN=node-00" -out node.csr
openssl x509 -req -in node.csr -CA bench-ca.pem -CAkey ca-key.pem \
    -CAcreateserial -days 36500 -out bench-node.pem
rm ca-key.pem node.csr bench-ca.srl
```
