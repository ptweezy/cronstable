# Security policy

## Supported versions

Releases come from a single release line. Security fixes land on the latest
released version. There are no long-term-support branches.

| Version | Supported |
| --- | --- |
| latest `1.2.x` release | yes |
| anything older | no, upgrade first |

If you are pinned to an older version and cannot upgrade, say so in the report.
The advisory then notes the first fixed version so you can backport locally.

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Report it privately through GitHub:

1. Go to <https://github.com/ptweezy/cronstable/security/advisories/new>
   (repository → **Security** → **Report a vulnerability**).
2. Describe the issue and how to reproduce it.

That channel is private between you and the maintainer. It is preferred
because it keeps the report, the fix, and the published advisory in one place.

Helpful things to include, when you have them:

- the cronstable version (`cronstable --version`) and how it was installed
  (pip, Docker image, standalone binary, distro package)
- the relevant part of the config, with secrets redacted
- a minimal reproduction, and what an attacker gains
- whether the daemon was exposed to a network, and to whom

## What is in scope

- the `cronstable` daemon and CLI, including job execution, privilege handling,
  and the state store
- the HTTP control API and web dashboard (`web:`), including authentication,
  token scoping, and the Server-Sent Events stream
- the MCP server
- the encrypted push pipeline in `cronstable/push.py` and the device-pairing
  flow, on both the daemon and companion-app sides, including anything that
  could expose plaintext alert content or a device key
- the published container images and standalone binaries
- the hosted services operated for this project: `relay.cronstable.com` (source
  in [ptweezy/cronstable-relay](https://github.com/ptweezy/cronstable-relay))
  and the public demo at `demo.cronstable.com`

A report about the relay can go to either repository's advisory page. It is
routed to the right one.

## What is out of scope

- the public demo being readable without any credential. `demo.cronstable.com`
  runs with `web.anonymousScopes: [view]` on purpose, so a tokenless request
  reads jobs, run history, logs, and the calendar feeds. Its published
  view-scoped token grants that same read-only view plus `GET /push/devices`,
  which is empty by construction: the demo configures no `push:` section. What
  *is* in scope there is any path by which a credential-less caller does any
  of the following:
  - reaches a `control` or `approve` route
  - reads `GET /push/devices`
  - otherwise acts on the daemon
- configurations that hand cronstable a deliberately dangerous job, such as a
  crontab line that a local user can already edit. Whatever command a config
  names, cronstable runs it. The trust boundary is who may write the config.
- exposing the control API to a hostile network with authentication disabled.
  That is documented as unsupported, not a vulnerability.
- missing hardening headers or similar findings on the static GitHub Pages site
  with no demonstrated impact.
- automated scanner output with no working proof of concept.

## Handling

One person maintains cronstable, so response times are best effort, not
contractual. What you can expect:

- an acknowledgment that the report was received and read
- an assessment of whether it is in scope, and a severity call
- a fix on the latest release line, and a published GitHub Security Advisory
  with a CVE where one is warranted

Unless you ask to stay anonymous, credit is given in the advisory. There is no
bug bounty. This is an MIT-licensed project with no funding behind it.

## Cryptography note

The push pipeline encrypts alert payloads to each paired device's public key
(NaCl sealed box) so the relay forwards ciphertext it cannot read. Findings that
break that property, or that let a relay operator or network observer recover
alert content or link devices, are treated as high severity.
