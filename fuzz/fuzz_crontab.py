"""Fuzz the classic crontab importer.

Each input is a crontab file. parse_crontab must lower any text to job
dictionaries or raise CrontabError naming the file, and loading the same
text as configuration must raise nothing but ConfigError.
"""

import logging
import sys

import atheris

with atheris.instrument_imports(include=["cronstable"]):
    from cronstable.config import ConfigError, parse_crontab_string
    from cronstable.crontabs import CrontabError, parse_crontab

PATH = "crontab"


def TestOneInput(data: bytes) -> None:
    text = data.decode("utf-8", "replace")
    try:
        jobs = parse_crontab(text, PATH)
    except CrontabError as err:
        assert str(err).startswith(PATH)
        return
    for job in jobs:
        assert job["name"] and job["command"] is not None
    try:
        parse_crontab_string(text, PATH)
    except ConfigError:
        pass


def main() -> None:
    logging.disable(logging.CRITICAL)
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
