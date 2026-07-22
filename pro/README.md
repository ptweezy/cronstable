# cronstable Pro (proprietary)

This directory holds cronstable's proprietary, paid/premium components. It is
**not** open source and is **not** covered by the repository's MIT license.

- License: [LICENSE](LICENSE) (all rights reserved).
- Repository licensing policy: [../LICENSING.md](../LICENSING.md).
- Trademarks: [../TRADEMARKS.md](../TRADEMARKS.md).

## Boundary rules

- Code here may depend on / import the MIT core (`cronstable/`). The MIT license
  permits proprietary software to build on it.
- Do **not** copy MIT-licensed source *into* this directory. Importing the core
  is fine; vendoring its source here would pull MIT-covered code (and its
  attribution obligation) into a proprietary tree. Keep the boundary at the
  import level.
- New files here begin with:

  ```
  # SPDX-License-Identifier: LicenseRef-cronstable-Proprietary
  ```

This directory is currently a **scaffold**. It establishes the licensing
boundary before any premium code lands, so there is never a moment where
proprietary code sits under an implied MIT license.
