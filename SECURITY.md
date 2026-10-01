# Security policy

## Supported versions

cronstable has a single release line and no long-term-support branches. Only
the latest release is supported, and a security fix ships as a new release.

If you can't upgrade from an older version, say so in your report. The
advisory then names the first fixed version and links the fix, so you can
backport it.

## Reporting a vulnerability

**Don't open a public issue for a security problem.**

Report it privately through GitHub. Open the
[advisory form](https://github.com/ptweezy/cronstable/security/advisories/new),
or go to the repository's **Security** tab and click **Report a
vulnerability**. Only you and the maintainer can see the report. The report,
the fix, and the published advisory stay in one place.

If you have them, include the following:

- The cronstable version (`cronstable --version`) and how you installed it,
  for example pip, Docker image, standalone binary, or system package
- The relevant part of the config, with secrets redacted
- A minimal reproduction, and what an attacker gains
- Whether the daemon was reachable from a network, and by whom

## In scope

- The `cronstable` daemon and CLI, including job execution, privilege
  handling, and the state store
- The HTTP control API and web dashboard (`web:`), including authentication,
  token scoping, and the Server-Sent Events stream
- The MCP server
- The encrypted push pipeline in `cronstable/push.py` and device pairing, in
  both the daemon and the iOS app, including anything that could expose
  plaintext alert content or a device key
- The published container images and standalone binaries
- The hosted services that the project operates: the push relay at
  `relay.cronstable.com` (source in
  [ptweezy/cronstable-relay](https://github.com/ptweezy/cronstable-relay))
  and the public demo at `demo.cronstable.com`

Report an issue in the relay, the demo, or the iOS app through the same
advisory form.

## Out of scope

- The public demo's intended visitor access. Without a credential, anyone can
  read the jobs, run history, logs, metrics, and calendar feeds on
  `demo.cronstable.com`. Anyone can also start, cancel, pause, and resume the
  sample jobs, and trigger and decide the sample workflows, because a gateway
  in front of the daemon forwards an allowlist of those actions. The
  published view token grants the same access. The
  [demo instance README](https://github.com/ptweezy/cronstable/blob/main/example/demo-instance/README.md)
  lists what the gateway allows. A visitor getting past the gateway is in
  scope: for example, acting on a job outside the allowlist, reaching a route
  that the gateway doesn't forward (device pairing, MCP, backfill, shutdown),
  or recovering the private operator token.
- A config that names a dangerous command, such as a crontab line that a
  local user can already edit. cronstable runs whatever command a config
  names. The trust boundary is who can write the config.
- An API listener that a hostile network can reach while no token is
  configured. The API is unauthenticated by default, and the
  [authentication documentation](https://github.com/ptweezy/cronstable/wiki/HTTP-API#authentication)
  tells you to restrict access or set a token.
- Automated scanner output with no working proof of concept.

## What to expect

One person maintains cronstable, so response times are best effort. You can
expect the following:

- An acknowledgment that the maintainer received and read your report
- An assessment of whether the issue is in scope, and a severity rating
- A fix in a new release, and a published GitHub Security Advisory with a
  CVE where one is warranted

The advisory credits you unless you ask to stay anonymous. There is no bug
bounty: cronstable is an MIT-licensed project with no funding.

## Push encryption

The daemon encrypts each alert to the paired device's public key, so the
relay forwards ciphertext that it can't read. The encryption is X-Wing, a
post-quantum hybrid of ML-KEM-768 and X25519, where the platform supports it,
and an X25519 sealed box elsewhere. The maintainer treats a finding as high
severity if it lets a relay operator or a network observer recover alert
content or link devices.
