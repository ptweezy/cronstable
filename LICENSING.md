# Licensing

This repository is **MIT-licensed by default**. The full text is in
[LICENSE](LICENSE), and it applies to everything in the repository **except** a
directory that ships its own `LICENSE` file, which governs that directory
instead.

This file travels with the code: it is packaged (alongside
[TRADEMARKS.md](TRADEMARKS.md)) into every wheel's `dist-info/licenses/`
directory. Paths in this document are **repository paths**. A distribution will
not contain most of them, because `pro/` and the `docs/` website tree are pruned
from everything published (see `MANIFEST.in`); the paths still identify exactly
which files each statement is about, and all of them can be inspected at
<https://github.com/ptweezy/cronstable>.

## Why this file exists

cronstable is planned to grow.

## The rule

1. The root [LICENSE](LICENSE) (MIT) governs the whole repository by default.
2. A directory that contains its own `LICENSE` file is governed **only** by that
   license, for that directory and everything under it.
3. For any file, the nearest `LICENSE` found walking up the directory tree wins.
   If none is found before the root, the root MIT LICENSE applies.

## Current layout

| Path | License | Notes |
| --- | --- | --- |
| `/` core (`cronstable/`, docs, tests, CI, packaging, ...) | MIT | See [LICENSE](LICENSE). |
| `pro/` | Proprietary | cronstable Pro (the `cronstable-pro` package). See [pro/LICENSE](pro/LICENSE). Not open source. |
| rendered brand artwork under `docs/` | Reserved | The finished logo. See [Brand assets](#brand-assets) below. |

As more proprietary components are added, each gets its own `LICENSE` file under
the same rule, and a row here. Proprietary directories are pruned from the public
MIT sdist (see `MANIFEST.in`), so they are never distributed through PyPI.

## Brand assets

One narrow set of files is **excluded from the MIT grant**: the rendered brand
artwork. These sit in directories that are otherwise MIT (they share `docs/img/`
with dozens of ordinary screenshots), so a directory-level `LICENSE` file would
claim too much. They are listed by name instead, in three places that a reader
can reach from wherever they start:

- [LICENSE](LICENSE) names the exclusion, so the operative grant is not silent
  about its own scope.
- this section holds the authoritative list.
- [docs/img/README.md](docs/img/README.md) repeats it next to the files.

The list:

| Pattern | What it is |
| --- | --- |
| `docs/**/logo-balance.gif`, `docs/**/logo-balance.webp` | the animated wordmark, dark |
| `docs/**/logo-balance-light.gif`, `docs/**/logo-balance-light.webp` | the animated wordmark, light |

These are the finished logo, not source. They may be reproduced **unmodified**
when referring to cronstable itself, which is the same latitude the nominative
fair use section of [TRADEMARKS.md](TRADEMARKS.md) gives the name: write about
the project, link to it, illustrate a post about it. Every other right,
including the right to modify them or to adopt them as the identity of another
product, is reserved by Parker Loflin.

Two things this deliberately does **not** do:

- **It does not touch the logo engine.** The cart-and-double-pendulum simulation
  that draws the wordmark lives in `cronstable/web/index.html` and
  `docs/logo-lab.html`. Both are ordinary product source and stay MIT, so a fork
  keeps a working dashboard and is free to run, study, and modify the physics.
  What a fork may not do is keep calling the result cronstable, and that is a
  trademark question rather than a copyright one.
- **It is not retroactive.** Anyone who already received these files under the
  MIT License keeps that grant for the copies they received. The reservation
  applies going forward, which is the most any license change can do.

## Keeping the boundary clean

- Proprietary code may **import** the MIT core; MIT permits proprietary software
  to build on it.
- Do **not** copy MIT-licensed source *into* a proprietary directory. Importing
  the core is fine; vendoring its source there would pull MIT-covered code (and
  its attribution obligation) into a proprietary tree. Keep the boundary at the
  import level.
- New files in a proprietary directory carry an SPDX header so the license is
  unambiguous even out of context:

  ```text
  # SPDX-License-Identifier: LicenseRef-cronstable-Proprietary
  ```

  Core files rely on the root LICENSE and need no header; they may optionally
  carry `# SPDX-License-Identifier: MIT`.

## Third-party code and dependencies

cronstable is a fork of [yacron](https://github.com/gjcarneiro/yacron) (MIT); the
root LICENSE preserves yacron's copyright alongside cronstable's, as MIT
requires.

Runtime dependencies of the core install are permissive (MIT / BSD / Apache /
PSF / MPL). A CI guard
([.github/scripts/check_licenses.py](.github/scripts/check_licenses.py), run by the
`licenses` job over the runtime plus every distributable extra) fails the build
if a strong-copyleft (GPL / AGPL) or non-open source-available (SSPL / BUSL)
dependency is ever introduced, so the permissive baseline cannot regress by
accident. This matters because the shipped artifacts (the PyInstaller binaries
and Docker images) bundle the whole dependency tree.

One dependency is weak copyleft, bundled deliberately and with its
obligations met explicitly:

- **python-zeroconf** (the `discovery` extra, behind `web.bonjour`) is
  **LGPL-2.1-or-later** and is included, unmodified, in the standalone
  binaries, the Docker images, and the `discovery` pip extra. The LGPL permits
  proprietary-or-MIT applications to bundle the library; in exchange the
  recipient must get the license text, access to the library's source, and a
  real way to use a modified build of the library. cronstable meets each one:

  - **Notice + license text**: every artifact carries
    [`cronstable/licenses/THIRD-PARTY-NOTICES.txt`](cronstable/licenses/THIRD-PARTY-NOTICES.txt)
    (the notice plus the full LGPL-2.1 text) as package data; any binary
    prints it with `cronstable --third-party-licenses`. Docker and pip
    installs additionally keep zeroconf's own `COPYING` in its `dist-info`.
  - **Source access**: each GitHub Release attaches the python-zeroconf
    source archive next to the binaries; it is also on
    [PyPI](https://pypi.org/project/zeroconf/) and
    [GitHub](https://github.com/python-zeroconf/python-zeroconf).
  - **Relink right**: cronstable is fully open source, and the binaries are
    produced by a public recipe
    ([pyinstaller/cronstable.spec](pyinstaller/cronstable.spec) plus the
    `binaries*` jobs in
    [.github/workflows/release.yml](.github/workflows/release.yml)). To run
    the combined work with a modified python-zeroconf, install cronstable
    from PyPI beside your build of the library (pip installs keep it as
    ordinary replaceable files), or rebuild the binary from this repository
    with your modified library in the build environment. Nothing in the MIT
    license restricts modification or the reverse engineering needed to
    debug such modifications.

  The `licenses` CI job reports zeroconf as weak copyleft (allowed) on every
  run, keeping the choice visible; a strong-copyleft dependency would still
  fail the gate.

## Trademarks

The MIT License covers the code, not the brand. The cronstable name and logo are
trademarks; see [TRADEMARKS.md](TRADEMARKS.md).
