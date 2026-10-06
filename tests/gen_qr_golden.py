"""Regenerate the QR Code golden vectors (tests/data/qr_golden.json).

The web dashboard draws its pairing code with the JavaScript encoder bundled
in ``cronstable/web/index.html``, and the terminal clients draw the same
link with :mod:`cronstable.qr`. ``tests/test_qr.py`` proves the two agree by
replaying the vectors this script records: for each version and error
correction level, the symbol the bundled encoder produces for a fixed input
at the smallest and the largest length that version holds.

A vector stores the input's length, the symbol's version and mask, and the
SHA-256 of its modules. The input bytes come from ``golden_data`` in
``tests/test_qr.py``, which this script imports, so the file stays small.
The bundled encoder picks its mask with its own scoring, so the mask is
recorded and the replay pins it. :func:`cronstable.qr.encode` chooses a mask
by the standard's scoring, which ``tests/test_qr.py`` covers separately.

Usage (needs Node.js, which is not a cronstable dependency)::

    python tests/gen_qr_golden.py

The script reads the encoder straight out of ``index.html``, so the vectors
always describe the code the dashboard ships.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from cronstable import qr  # noqa: E402
from tests.test_qr import golden_data, modules_digest  # noqa: E402

PAGE = os.path.join(os.path.dirname(HERE), "cronstable", "web", "index.html")
OUT = os.path.join(HERE, "data", "qr_golden.json")

#: The comment that opens the bundled encoder's script block.
MARKER = "QR Code Generator for JavaScript (vendored, verbatim)"

DRIVER = """
const fs = require("fs");
const vm = require("vm");
const [source, casesPath] = process.argv.slice(2);
const context = {};
vm.createContext(context);
vm.runInContext(fs.readFileSync(source, "utf8") + "\\nthis.qrcode = qrcode;",
                context);
const out = [];
for (const item of JSON.parse(fs.readFileSync(casesPath, "utf8"))) {
  // Type number 0 picks the smallest version, as the dashboard asks. The
  // default stringToBytes keeps charCode & 0xff, so a latin-1 string is
  // raw bytes.
  const symbol = context.qrcode(0, item.level);
  symbol.addData(Buffer.from(item.hex, "hex").toString("latin1"), "Byte");
  symbol.make();
  const size = symbol.getModuleCount();
  const rows = [];
  for (let r = 0; r < size; r++) {
    let row = "";
    for (let c = 0; c < size; c++) row += symbol.isDark(r, c) ? "1" : "0";
    rows.push(row);
  }
  out.push(rows);
}
process.stdout.write(JSON.stringify(out));
"""


def bundled_encoder() -> str:
    """The encoder's script block, cut out of the dashboard page."""
    with open(PAGE, encoding="utf-8") as fh:
        page = fh.read()
    marker = page.index(MARKER)
    start = page.rindex("<script>", 0, marker) + len("<script>")
    return page[start : page.index("</script>", marker)]


def main() -> None:
    node = shutil.which("node")
    if node is None:
        sys.exit("Node.js is required to run the dashboard's QR encoder")
    cases = []
    for level in qr.LEVELS:
        for version in range(1, qr.MAX_VERSION + 1):
            largest = qr.data_capacity(version, level)
            smallest = (
                qr.data_capacity(version - 1, level) + 1 if version > 1 else 1
            )
            for length in (smallest, largest):
                cases.append(
                    {"level": level, "version": version, "length": length}
                )
    with tempfile.TemporaryDirectory() as tmp:
        paths = [os.path.join(tmp, n) for n in ("qr.js", "cases.json", "d.js")]
        with open(paths[0], "w", encoding="utf-8") as fh:
            fh.write(bundled_encoder())
        with open(paths[1], "w", encoding="utf-8") as fh:
            json.dump(
                [
                    {
                        "level": c["level"],
                        "hex": golden_data(c["length"]).hex(),
                    }
                    for c in cases
                ],
                fh,
            )
        with open(paths[2], "w", encoding="utf-8") as fh:
            fh.write(DRIVER)
        symbols = json.loads(
            subprocess.run(
                [node, paths[2], paths[0], paths[1]],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
    for case, rows in zip(cases, symbols, strict=True):
        matrix = [[cell == "1" for cell in row] for row in rows]
        if len(matrix) != qr.symbol_size(case["version"]):
            sys.exit("version mismatch for {}".format(case))
        data = golden_data(case["length"])
        masks = [
            mask
            for mask in range(8)
            if qr.encode(data, case["level"], mask) == matrix
        ]
        if len(masks) != 1:
            sys.exit("cronstable.qr disagrees with {}".format(case))
        case["mask"] = masks[0]
        case["sha256"] = modules_digest(matrix)
    header = {
        "_comment": "Generated by tests/gen_qr_golden.py from the QR "
        "encoder bundled in cronstable/web/index.html. Do not edit.",
        "encoder_sha256": hashlib.sha256(
            bundled_encoder().encode("utf-8")
        ).hexdigest(),
    }
    # One vector per line keeps a regeneration's diff readable.
    lines = ["  " + json.dumps(case, sort_keys=True) for case in cases]
    text = "{}\n{}\n{}\n".format(
        json.dumps(header, indent=1, sort_keys=True)[:-2] + ',\n "vectors": [',
        ",\n".join(lines),
        " ]\n}",
    )
    with open(OUT, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    print("wrote {} vectors to {}".format(len(cases), OUT))


if __name__ == "__main__":
    main()
