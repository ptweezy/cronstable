# Documentation style

- All documentation follows the [Google developer documentation style guide](https://developers.google.com/style), including its word list.
- Documentation describes present, holistic behavior only. Never write it as a change
  log relative to past versions ("now supports", "no longer requires", "previously X,
  this was changed to Y"). State what the system does today, as if the current behavior
  had always been the design.
- Remove litotes everywhere: replace understatement-by-negation ("not uncommon",
  "not without merit", "no small feat") with the direct positive form ("common",
  "has merit", "a major feat").
- After adding or editing any documentation (READMEs, docs pages, docstrings, code
  comments, release notes, marketing/App Store copy), run the blader humanizer skill at
  [https://github.com/blader/humanizer](https://github.com/blader/humanizer) or
  (`humanizer:humanizer`) over the new text.
