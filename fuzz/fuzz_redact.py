"""Fuzz secret redaction of archived job output.

Each input is captured output, which the job controls. The daemon redacts
it with redact_lines, which applies redact_secrets one line at a time.
Redaction must accept any text and keep one output line per input line,
and a second redact_secrets pass over a line must find nothing the first
pass left behind. A pattern that backtracks badly shows up as a libFuzzer
timeout.
"""

import logging
import sys

import atheris

with atheris.instrument_imports(include=["cronstable"]):
    from cronstable.redact import redact_lines, redact_secrets


def TestOneInput(data: bytes) -> None:
    lines = data.decode("utf-8", "replace").split("\n")
    assert len(redact_lines(lines)) == len(lines)
    for line in lines:
        once = redact_secrets(line)
        assert redact_secrets(once) == once


def main() -> None:
    logging.disable(logging.CRITICAL)
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
