# Push path protocol review: protobuf, gRPC, post-quantum

A review of the end-to-end encrypted push path (daemon → relay → app) against
three candidate technologies. Measured against `cronstable` @ 3303d0d,
`cronstable-relay` @ a36f301, `cronstable-ios` @ 5ad993d.

Every byte figure here was computed against those trees, not estimated: the
envelope by serializing `apnsPayload()`, payload sizes by encoding the field set
`build_payload()` emits, log-line counts by running `fit_payload()`'s own drop
order to convergence.

| Technology | Verdict |
| --- | --- |
| gRPC | **Do not adopt.** Costs the architecture coverage that is a headline feature. |
| Protobuf | **Not worth it.** Saves 177 bytes where 1,202 sit unused; buys ~1 log line. |
| Post-quantum | **Adopt, phased.** Real harvest-now-decrypt-later exposure; app side ready, daemon blocked upstream. |

Sections 3 and 4 measure the path as it stood when the review was written, which
is what the §6 recommendations are argued from. §6 records what each step
resolved to.

## 1. gRPC

cronstable ships for amd64, arm64, armv7, armv6, i686, ppc64le, s390x and
riscv64. `grpcio`/`protobuf` lack wheels for the lean end of that list and need
a C++ toolchain to source-build. The codebase already rejected gRPC in five
places for exactly this reason (`leadership.py:15`, `backends/__init__.py:6,18`,
`backends/etcd.py:11`, `backends/kubernetes.py:16`).

The etcd backend already *consumes* a gRPC service over its v3 gRPC-gateway JSON
interface — deliberately, because "the gateway is etcd's first-class HTTP
interface, so a native grpc client buys little". That read is still correct.

The three candidate surfaces are not bottlenecked on framing or streaming:

- **daemon → relay**: one fire-and-forget POST per (alert, device), no retry.
- **app ↔ daemon**: already an OpenAPI contract with a generated Swift client
  plus SSE for live updates.
- **daemon ↔ daemon**: already mTLS gossip with no shared state.

## 2. Protobuf

Protobuf as an encoding (without gRPC) is a fairer question, since the sealed
payload must fit an APNs notification. Encoding a realistic failure alert (a
nightly billing export with a six-line Python traceback, using the exact field
set `build_payload()` emits):

| Encoding | Bytes | vs JSON | Cost to adopt |
| --- | ---: | ---: | --- |
| JSON (today) | 587 | — | none |
| JSON, 1-char keys | 525 | −62 | rename fields, both sides |
| JSON, short keys + epoch ints | 491 | −96 | rename + timestamp type change |
| Protobuf (no field names, varints) | 410 | −177 | schema, codegen, runtime dep on 8 arches |

177 bytes, against a budget that still leaves 1,202 bytes unused on this alert
*after* adding post-quantum overhead. In the only place the ceiling is
user-visible — the log tail — protobuf buys about one extra line of traceback.
Raising one constant (§3) buys nine.

A protobuf runtime is the same dependency class rejected for etcd and
Kubernetes. Not a good trade for one log line.

## 3. The byte budget is misestimated by 6×

`push.py` caps base64 ciphertext at 3000 chars, reasoning that this leaves the
relay "~1 KB of headroom". The relay's actual envelope — `apnsPayload()` in
`apns.ts`, serialized as `JSON.stringify` emits it, ciphertext removed — is
**172 bytes**.

Of the 4096-byte APNs frame:

| Scenario | Envelope | Crypto | Plaintext | Unused | Log lines |
| --- | ---: | ---: | ---: | ---: | ---: |
| Today — sealed box, cap 3000 | 172 | 48 | 2,202 | 924 | 26 |
| Naive PQ — X-Wing, cap 3000 | 172 | 1,136 | 1,114 | 924 | 11 |
| Tuned PQ — X-Wing, cap 3900 | 172 | 1,136 | 1,789 | 24 | 20 |

This constant decides whether post-quantum is painful here. Adopt X-Wing without
touching it and the log tail collapses 26 → 11 lines, a regression operators feel
on every failure. Raise the cap in the same change and it lands at 20. The APNs
ceiling is unchanged at 4096, so 3900 leaves 24 chars of slack against a fixed,
measured envelope.

## 4. Post-quantum

### Exposure

Sealed alerts carry job names, hostnames, schedules, exit codes, failure reasons
and up to 40 lines of captured stderr — stack traces leak internal topology, file
paths, database hostnames, occasionally credentials. All protected by X25519
alone: a textbook harvest-now-decrypt-later target. The threat model already
says the relay is not a trusted party; today that rests entirely on a pre-quantum
primitive.

### X-Wing

Hybrid ML-KEM-768 + X25519; secure if *either* component holds, so adopting it
cannot weaken the current position. Both crypto stacks converged on it.

| Component | Bytes | Notes |
| --- | ---: | --- |
| Encapsulation key (public) | 1,216 | ML-KEM-768 ek 1,184 + X25519 pk 32 |
| Decapsulation key (private) | 32 | a seed, expanded on use |
| Ciphertext | 1,120 | ct_M 1,088 + ct_X 32 |
| Shared secret | 32 | SHA3-256 combiner |
| **Wire overhead per alert** | **1,136** | ciphertext + 16-byte AEAD tag |

The 32-byte private key keeps the app's Keychain blob's secret half exactly the
size it already is.

### Availability

| Side | Path | Status |
| --- | --- | --- |
| iOS app | CryptoKit `XWingMLKEM768X25519` + PQ-HPKE | **Ready now** |
| Relay | opaque base64 passthrough; two constants | **Trivial** |
| Daemon | PyNaCl 1.6.2 → libsodium 1.0.20, no `crypto_kem` bindings | **Blocked** |
| Daemon (alt) | pyca/cryptography 47+ ML-KEM | **Not viable** |

The app side is free: CryptoKit gained ML-KEM and X-Wing in iOS 26 and
`CronstableKit` already targets iOS 27, so the API is available unconditionally —
no availability guards, no new package. Longer term it could replace swift-sodium
on this path entirely.

The daemon is blocked upstream. libsodium 1.0.22 (April 2026) added
`crypto_kem_*` with X-Wing as the recommended KEM, but PyNaCl 1.6.2 still bundles
1.0.20 and exposes no bindings. The alternative fails on our own constraints:
pyca/cryptography's ML-KEM needs an AWS-LC or BoringSSL backend — its changelog
says "As we ship our wheels with OpenSSL, most users will not have access to
these APIs yet" — and pulls in Rust, precisely why `pyproject.toml` chose PyNaCl
("pure C, no Rust … source-builds with a plain C toolchain on the leaner arches").

A pure-Python ML-KEM was considered and ruled out. The daemon only ever
*encapsulates*, holding no long-term secret on this path, so its side-channel
surface is smaller than a decapsulating peer's — but the leading pure-Python
implementation states it "is not constant time or written to be performant" and
exists as an educational tool. Waiting for PyNaCl keeps the pure-C,
all-architecture property intact.

### Blast radius

Confined to eight constants:

```
cronstable/push.py      DEVICE_PUBLIC_KEY_BYTES = 32
                        _SEALED_OVERHEAD        = 48
                        CIPHERTEXT_B64_MAX      = 3000
relay/src/validate.ts   MAX_CIPHERTEXT_CHARS    = 3000
                        MIN_CIPHERTEXT_CHARS    = 68
ios/DeviceKey.swift     blob.count == 64, subdata(in: 0..<32 / 32..<64)
docs/openapi.yaml       description only — no maxLength, already size-agnostic
docs/relay-protocol.md  the v1 contract text
```

`key_fingerprint()` hashes raw key bytes and is already width-agnostic; the trim
loop is driven by `MAX_PLAINTEXT_BYTES` and needs no logic change. Both sides
already carry independent `v` fields — the seam a migration needs.

## 5. Two gaps found on the way

Neither is about post-quantum; both are cheapest to fix while the sealed format
is already open.

### Anyone holding a device's public key can forge alerts to it

A sealed box is anonymous-sender by construction — there is no sender
authentication. The app cannot distinguish an alert the daemon sealed from one
sealed by anybody else holding the device's public key, and `GET /push/devices`
returns that key in full to any dashboard reader. The fingerprint comparison
closes key *substitution*; it does nothing about forgery.

HPKE PSK mode fixes this at zero ciphertext cost: a secret established at pairing
is mixed into the key schedule rather than transmitted, so only a holder can
produce a payload that opens. CryptoKit exposes PSK and auth-PSK modes, and
pairing is already a place both sides exchange material.

### Replay is bounded only by a 10-minute clock

`relay-protocol.md` is candid that "the payload itself is the only place freshness
can live", leaning entirely on `ts` with a 600-second window. Inside that window
a captured ciphertext replays cleanly, arbitrarily often. A small seen-set of
recent payload identifiers in the NSE closes it — `collapseId` is already a
per-alert identifier and the window is already bounded, so the set stays tiny.

## 6. Recommended sequence

Steps 1 and 2 are independently valuable, ship without any new dependency, and
make step 3 small. Both are implemented; step 3 is blocked upstream.

**1. Derive the ciphertext cap from a measurement.** Implemented. The cap is
`4096 - 189 (measured envelope) - 107 (reserve) → 3800`. The relay's
`tests/apns-size.spec.ts` asserts the 189 against the payload `apnsPayload()`
serializes, and the daemon's
`test_ciphertext_cap_fits_the_apns_frame_with_its_reserve` re-derives the
arithmetic, so neither side can drift onto an estimate. The x25519 plaintext
budget is 2802 bytes: 35 surviving log-tail lines on the sample alert in §2.

3800 rather than the 3900 first sketched. 3900 leaves 7 bytes of slack against
a measured envelope, too thin a margin for a constant whose purpose is to rest
on a measurement.

**2. Make the wire format key-agnostic.** Implemented. A `suite` identifier
rides on the pairing record, the relay envelope and the APNs payload, and drives
key-length validation, sealing dispatch, size fitting and the relay's
minimum-length check. `x25519` and `xwing` are registered; `xwing` is
deliberately unsealable, so pairing under it is refused rather than stored as a
record whose every alert would fail. An absent `suite` reads as `x25519`
everywhere, so a pairing or a daemon that names none keeps working.

Two properties come with it:

- Payload fitting is **per device**, against that device's own suite budget, so
  a device on a wider-ciphertext suite costs the devices beside it no log lines.
- `collapse_id` is derived **before** trimming. It hashes `run_id`, which the fit
  loop's last resort drops, so taking the id off the trimmed payload would make
  an oversized alert coalesce under a different id than the same alert from a
  node whose copy fits — and per-device fitting would widen that to one id per
  suite.

**3. Swap to X-Wing when PyNaCl exposes `crypto_kem_*`.** Blocked. Track the
release bundling libsodium ≥1.0.22; adding the bindings is a small upstream
contribution if worth accelerating. On top of steps 1 and 2, the daemon-side
change is one branch in `seal_to_device()` plus flipping `sealable` on the suite;
the app's is one branch in `SealedBoxCrypto.open()` against CryptoKit's
`XWingMLKEM768X25519`. Fold PSK-mode sender authentication (§5) into the same
revision rather than changing the wire twice.

## Sources

- [libsodium 1.0.22 release notes](https://github.com/jedisct1/libsodium/releases/tag/1.0.22-RELEASE) — `crypto_kem_*` / X-Wing
- [draft-connolly-cfrg-xwing-kem-10](https://datatracker.ietf.org/doc/html/draft-connolly-cfrg-xwing-kem) — construction and sizes
- [WWDC25 — Get ahead with quantum-secure cryptography](https://developer.apple.com/videos/play/wwdc2025/314/) — CryptoKit `XWingMLKEM768X25519`
- [Apple CryptoKit — HPKE.Sender](https://developer.apple.com/documentation/cryptokit/hpke/sender) — PSK and auth-PSK modes
- [pyca/cryptography changelog](https://github.com/pyca/cryptography/blob/main/CHANGELOG.rst) — ML-KEM requires AWS-LC/BoringSSL
- [PyNaCl changelog](https://pynacl.readthedocs.io/en/latest/changelog/) — 1.6.2 bundles libsodium 1.0.20
- [pyca/cryptography ML-KEM docs](https://cryptography.io/en/48.0.1/hazmat/primitives/asymmetric/mlkem/) — parameter sets
- [kyber-py](https://github.com/giacomopope/kyber-py) — pure-Python ML-KEM, not constant-time
- [draft-ietf-hpke-pq-04](https://datatracker.ietf.org/doc/draft-ietf-hpke-pq/) — PQ and hybrid KEMs for HPKE
- [NIST FIPS 203](https://csrc.nist.gov/pubs/fips/203/final) — ML-KEM standard
- [Apple — remote notification payload limits](https://developer.apple.com/library/archive/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/CreatingtheNotificationPayload.html) — 4096-byte cap
