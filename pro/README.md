# cronstable Pro (proprietary)

cronstable's proprietary, paid/premium Python package. It is **not** open source
and is **not** covered by the repository's MIT license.

- License: [LICENSE](LICENSE) (all rights reserved).
- Repository licensing policy: [../LICENSING.md](../LICENSING.md).
- Trademarks: [../TRADEMARKS.md](../TRADEMARKS.md).

## Why a separate package

`cronstable-pro` is a **distinct distribution** from the MIT core. It depends on
`cronstable` and builds on the core's public API, so proprietary code never lives
inside the MIT package, and it is never shipped in the core's public sdist/wheel
(`pro/` is pruned from it in the root `MANIFEST.in`). Because MIT is permissive, a
proprietary package is free to build on the core.

## Layout

```text
pro/
  pyproject.toml        cronstable-pro (proprietary; marked "Do Not Upload")
  cronstable_pro/
    __init__.py         public API; imports the core to assert the boundary
    licensing.py        the entitlement gate premium features check (fail-closed)
  tests/
    test_boundary.py    proves Pro imports the core and the gate is fail-closed
```

## Develop and test

To install the core and Pro packages and run Pro tests, run these commands
from the repository root:

```sh
python -m pip install -e . -e "./pro[dev]"
python -m pytest pro/tests
```

Pytest discovers Pro's configuration in `pro/pyproject.toml`. The `pro` CI
job runs these tests separately from the core's tox suite to check that Pro
imports the core and rejects unverified entitlements.

## Conventions

- Every file starts with `# SPDX-License-Identifier: LicenseRef-cronstable-Proprietary`.
- Premium features gate on `cronstable_pro.licensing.is_entitled(...)`, which is
  fail-closed: no entitlement until real server-side verification is wired in.
- Do not copy MIT core source into this tree; import it.
