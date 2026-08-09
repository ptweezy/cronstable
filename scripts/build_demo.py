"""Regenerate the docs-site files that derive from the shipped dashboard.

Two files are generated, never edited by hand:

- docs/demo/index.html: cronstable/web/index.html with exactly two deltas,
  the ``<title>`` and one injected block (docs/demo/_backend.html, the
  fake-backend script plus the note that trails it), spliced in immediately
  before the logo engine's ``<script>``.
- docs/logo-engine.js: the dashboard's inline pendulum logo engine, extracted
  verbatim so docs/logo-lab.html and docs/comparison.html can share it via
  ``<script src>``. The dashboard itself keeps its inline copy; that page
  must stay a self-contained single file.

tests/test_web_demo_mirror.py rebuilds both with the functions here and
fails if a checked-in file differs, so after editing the dashboard or the
backend fragment, run:

    python scripts/build_demo.py
"""

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.join(ROOT, "cronstable", "web", "index.html")
FRAGMENT = os.path.join(ROOT, "docs", "demo", "_backend.html")
DEMO = os.path.join(ROOT, "docs", "demo", "index.html")
ENGINE_JS = os.path.join(ROOT, "docs", "logo-engine.js")

TITLE = "<title>cronstable // live dashboard demo</title>"

# The splice anchors on the logo engine's banner, the same anchor the mirror
# tests slice on: the engine always owns a bare <script> tag of its own and
# the banner comment is the first thing inside it.
_ENGINE_OPEN = re.compile(r"(?m)^<script>\n/\* =+\n \*  logo engine")
# the whole engine block, banner through the close of its own script tag
_ENGINE_BLOCK = re.compile(r"/\* =+\n \*  logo engine.*?\n</script>", re.DOTALL)


def build(web_text, fragment_text):
    """The demo page: web text + <title> swap + fragment before the engine."""
    page, count = re.subn(
        r"<title>.*?</title>", TITLE, web_text, count=1, flags=re.DOTALL
    )
    if count != 1:
        raise SystemExit("no <title> found in the dashboard page")
    match = _ENGINE_OPEN.search(page)
    if match is None:
        raise SystemExit(
            "logo engine banner not found in the dashboard page; the demo "
            "backend is spliced right before it"
        )
    return page[: match.start()] + fragment_text + page[match.start() :]


def extract_engine(web_text):
    """The logo engine's JS, exactly as inlined in the dashboard page."""
    match = _ENGINE_BLOCK.search(web_text)
    if match is None:
        raise SystemExit(
            "logo engine banner not found in the dashboard page; "
            "docs/logo-engine.js is extracted from it"
        )
    return match.group(0)[: -len("</script>")]


def _read(path):
    # newline="" so the LF-only sources round-trip unchanged on Windows
    with open(path, encoding="utf-8", newline="") as fh:
        return fh.read()


def main():
    web = _read(WEB)
    for path, text in (
        (DEMO, build(web, _read(FRAGMENT))),
        (ENGINE_JS, extract_engine(web)),
    ):
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
        print("wrote %s (%d lines)" % (os.path.relpath(path), text.count("\n")))


if __name__ == "__main__":
    main()
