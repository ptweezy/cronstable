"""Fuzz the YAML configuration loader.

Each input is a configuration file. parse_config_string must load any text
or raise ConfigError. Every file the text names (an include, an env_file,
a secret's fromFile) reads as missing, so a run never touches the disk.
"""

import errno
import logging
import sys

import atheris

# strictyaml stays uninstrumented: tracing its scanner slows a run more than
# tenfold.
with atheris.instrument_imports(include=["cronstable"]):
    from cronstable import config


class NoFiles(FileNotFoundError):
    """A file named by the configuration under test."""


def no_files(path: str) -> bytes:
    raise NoFiles(errno.ENOENT, "fuzzing reads no files", path)


config._read_source = no_files


def TestOneInput(data: bytes) -> None:
    text = data.decode("utf-8", "replace")
    try:
        config.parse_config_string(text, "/fuzz/cronstable.yaml")
    except config.ConfigError:
        pass
    except NoFiles:
        # A missing include escapes parse_config_string as OSError;
        # parse_config reports it as ConfigError.
        pass


def main() -> None:
    logging.disable(logging.CRITICAL)
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
