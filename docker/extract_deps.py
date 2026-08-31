"""Write the Docker dependency layer's requirement lists from pyproject.toml.

All eight Dockerfiles (the root Dockerfile plus the docker/ variants) COPY
this file into /tmp/deps/ beside pyproject.toml and run it inside the
dependency layer, so the extraction logic lives once instead of being
hand-mirrored per image. Given the path to pyproject.toml, it writes two
files next to it:

- requirements.txt: the core dependencies plus the push-pq and discovery
  extras, the exact strings `pip install ".[push-pq,discovery]"` would
  resolve, so the images and pyproject.toml can never drift and a renamed
  extra fails the build loudly (KeyError) instead of silently shipping
  without it.  push-pq rather than push so the images seal post-quantum
  `xwing` push as well as `x25519`; it carries push's PyNaCl too.  Its
  cryptography line keeps its environment marker, which pip evaluates inside
  the image, so a platform with no wheel resolves to PyNaCl alone.
- build-requires.txt: build-system.requires, for the throwaway buildenv
  the project install builds its wheel with.

Both lists are echoed to stdout so the build log shows what was resolved.
Runs on the venv interpreter, which is Python 3.11+ (tomllib) in every
image.
"""

import os
import sys
import tomllib

EXTRAS = ("push-pq", "discovery")


def main(pyproject_path):
    with open(pyproject_path, "rb") as fobj:
        data = tomllib.load(fobj)
    project = data["project"]
    requirements = list(project["dependencies"])
    for extra in EXTRAS:
        requirements += project["optional-dependencies"][extra]
    out_dir = os.path.dirname(os.path.abspath(pyproject_path))
    for name, lines in (
        ("requirements.txt", requirements),
        ("build-requires.txt", data["build-system"]["requires"]),
    ):
        body = "".join(line + "\n" for line in lines)
        with open(os.path.join(out_dir, name), "w", encoding="utf-8") as fobj:
            fobj.write(body)
        sys.stdout.write(body)


if __name__ == "__main__":
    main(sys.argv[1])
