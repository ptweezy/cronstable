"""Print a libFuzzer dictionary for fuzz_config.py.

The entries are every key in cronstable's configuration schema plus YAML
syntax. strictyaml finds keys by dictionary lookup, which gives the fuzzer
no comparison to learn a key from, so without this dictionary it rarely
leaves the keys in its seeds. build.sh writes the output to $OUT.
"""

import sys

from strictyaml.validators import Validator

from cronstable.config import CONFIG_SCHEMA

YAML_TOKENS = (": ", "- ", "\n  ", "|\n", ">\n", "#", "'", '"', "${", "}")


def schema_keys(schema: Validator) -> set[str]:
    keys: set[str] = set()
    seen: set[int] = set()
    stack = [schema]
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        for value in vars(node).values():
            if isinstance(value, Validator):
                stack.append(value)
            elif isinstance(value, (list, tuple)):
                stack.extend(v for v in value if isinstance(v, Validator))
            elif isinstance(value, dict):
                # Map._validator_dict: key, or Optional(key), to validator
                for key, child in value.items():
                    keys.add(str(getattr(key, "key", key)))
                    if isinstance(child, Validator):
                        stack.append(child)
    return keys


def entry(token: str) -> str:
    """One dictionary line, with \\xHH for each byte libFuzzer would
    misread."""
    chars = (
        chr(byte)
        if 0x20 <= byte < 0x7F and byte not in b'\\"'
        else "\\x{:02X}".format(byte)
        for byte in token.encode("utf-8")
    )
    return '"{}"'.format("".join(chars))


def main() -> None:
    keys = schema_keys(CONFIG_SCHEMA)
    if not keys:
        sys.exit("config_dict.py: found no keys in CONFIG_SCHEMA")
    for token in sorted(keys) + sorted(YAML_TOKENS):
        print(entry(token))


if __name__ == "__main__":
    main()
