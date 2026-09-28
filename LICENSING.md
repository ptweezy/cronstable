# Licensing

cronstable's source code, documentation, tests, and packaging are licensed
under the [MIT License](LICENSE). The rendered brand artwork listed in
[Brand assets](#brand-assets) is excluded from that grant. Third-party code and
dependencies keep their own licenses. For their notices and distribution
details, see [Third-party code and dependencies](#third-party-code-and-dependencies).

This file is packaged alongside [TRADEMARKS.md](TRADEMARKS.md) in every wheel's
`dist-info/licenses/` directory. Paths in this document refer to the
[source repository](https://github.com/ptweezy/cronstable). Most website assets
are excluded from the source distribution; see [MANIFEST.in](MANIFEST.in).

## Brand assets

One small set of files is excluded from the MIT grant: the rendered brand
artwork. These files are in directories that are otherwise MIT-licensed; for
example, they share `docs/img/` with dozens of ordinary screenshots. A
directory-level `LICENSE` file would claim too much, so the files are listed by
name instead, in three places. You can reach all three from any one of them:

- [LICENSE](LICENSE) names the exclusion, so the grant itself states its scope.
- This section has the authoritative list.
- [docs/img/README.md](docs/img/README.md) repeats the list next to the files.

The following files are excluded:

| Pattern | What it is |
| --- | --- |
| `docs/**/logo-balance.gif`, `docs/**/logo-balance.webp` | the animated wordmark, dark |
| `docs/**/logo-balance-light.gif`, `docs/**/logo-balance-light.webp` | the animated wordmark, light |
| `docs/**/ios-icon.png` | the iOS app icon |

These files are the finished logo artwork, not source code. You may reproduce
them **unmodified** when you refer to cronstable itself. This is the same
freedom that the nominative fair use section of [TRADEMARKS.md](TRADEMARKS.md)
gives the name: writing about the project, linking to it, or illustrating a
post about it. Parker Loflin reserves every other right, including the right to
modify these files or to adopt them as the identity of another product.

The reservation deliberately leaves two things alone:

- **It doesn't cover the logo engine.** The cart-and-double-pendulum simulation
  that draws the wordmark is in `cronstable/web/index.html` and
  `docs/logo-lab.html`. Both files are ordinary product source and stay
  MIT-licensed, so a fork keeps a working dashboard and can run, study, and
  modify the physics. A fork can't keep calling the result cronstable, but
  that's a trademark question, not a copyright one.
- **It isn't retroactive.** Anyone who already received these files under the
  MIT License keeps that grant for those copies. The reservation applies only
  going forward, which is the most that any license change can do.

## Third-party code and dependencies

The cronstable project is a fork of
[yacron](https://github.com/gjcarneiro/yacron) (MIT). The root LICENSE preserves
yacron's copyright alongside cronstable's, as MIT requires.

The runtime dependencies of the core install have permissive licenses (MIT,
BSD, Apache, PSF, or MPL). The `licenses` CI job runs a guard,
[.github/scripts/check_licenses.py](.github/scripts/check_licenses.py), over the
runtime dependencies and every distributable extra. If someone adds a
dependency that is strong copyleft (GPL or AGPL) or source-available but not
open source (SSPL or BUSL), the guard fails the build, so the permissive
baseline can't regress by accident. This matters because the shipped artifacts (the PyInstaller
binaries and Docker images) bundle the whole dependency tree.

One dependency is weak copyleft. The project bundles it deliberately and
meets its obligations explicitly:

- **python-zeroconf** (the `discovery` extra, behind `web.bonjour`) is
  licensed under LGPL-2.1-or-later. It's included, unmodified, in the
  standalone binaries, the Docker images, and the `discovery` pip extra. The
  LGPL permits proprietary or MIT-licensed applications to bundle the
  library. In exchange, the recipient must get the license text, access to the
  source, and a practical way to use a modified build of the library. The
  project meets each requirement:

  - **Notice and license text**: every artifact includes
    [`cronstable/licenses/THIRD-PARTY-NOTICES.txt`](cronstable/licenses/THIRD-PARTY-NOTICES.txt),
    which contains the notice and the full LGPL-2.1 text, as package data. Any
    binary prints it with `cronstable --third-party-licenses`. Docker and pip
    installs also keep zeroconf's own `COPYING` file in its `dist-info`
    directory.
  - **Source access**: each GitHub Release attaches the python-zeroconf
    source archive next to the binaries. The source is also on
    [PyPI](https://pypi.org/project/zeroconf/) and
    [GitHub](https://github.com/python-zeroconf/python-zeroconf).
  - **Right to relink**: cronstable is fully open source, and a public recipe
    builds the binaries:
    [pyinstaller/cronstable.spec](pyinstaller/cronstable.spec) and the
    `binaries*` jobs in
    [.github/workflows/release.yml](.github/workflows/release.yml). To run
    the combined work with a modified python-zeroconf, install cronstable
    from PyPI next to your build of the library, because pip installs keep
    the library as ordinary, replaceable files. You can also rebuild the
    binary from this repository with your modified library in the build
    environment. Nothing in the MIT License restricts modification or the
    reverse engineering needed to debug such modifications.

  The `licenses` CI job reports zeroconf as weak copyleft (allowed) on every
  run, so the choice stays visible. A strong copyleft dependency would still
  fail the check.

## Trademarks

The MIT License covers the code, not the brand. The cronstable name and logo are
trademarks; see [TRADEMARKS.md](TRADEMARKS.md).
