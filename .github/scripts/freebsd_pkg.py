"""Build the FreeBSD package, inside the FreeBSD VM, with FreeBSD's own tool.

    python3 .github/scripts/freebsd_pkg.py <version> <binary> <outdir>

Runs in the `binaries-freebsd` VM right after the binary it packages is built,
and shells out to ``pkg create``.  That is a deliberate choice over fpm, which
the roadmap proposed because it writes ``+MANIFEST`` itself and therefore runs on
the Linux runner: fpm maps ``aarch64`` to ``arm64`` and emits the ABI string
``FreeBSD:14:arm64``, which ``pkg`` rejects, so the arm64 package needs a
monkey-patch to come out valid at all.  Building here instead means the ABI comes
from the machine the package targets, no Ruby toolchain enters the pipeline, and
the result can be installed and started for real before it ships.

The manifest is assembled here rather than in shell because the install scripts
are embedded in it as JSON strings, and quoting those by hand in sh is how a
package ends up with a truncated post-install nobody notices.

Layout follows the ports convention rather than the Linux one: PREFIX is
/usr/local, the rc.d script goes under ``/usr/local/etc/rc.d``, and the
configuration ships as a ``.sample`` marked ``@sample`` in the plist, so pkg
creates the real file on install and removes it on deinstall only when the
administrator has not edited it.

``@sample`` is defined by the ports tree (Keywords/sample.ucl), which the lean
build VM does not carry, so a vendored copy ships in packaging/freebsd/Keywords
and PLIST_KEYWORDS_DIR points pkg at it.
"""

import json
import os
import shutil
import subprocess
import sys

PREFIX = "/usr/local"
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PACKAGING = os.path.join(ROOT, "packaging", "freebsd")

# Relative to the staging root, so the plist entries and the staged paths cannot
# drift apart.
BINARY = "usr/local/bin/cronstable"
RC_SCRIPT = "usr/local/etc/rc.d/cronstable"
CONFIG_SAMPLE = "usr/local/etc/cronstable.d/cronstable.yaml.sample"


def read(name):
    with open(os.path.join(PACKAGING, name), encoding="utf-8") as handle:
        return handle.read()


def stage(binary, root):
    """Lay the payload out under root exactly as it installs."""
    for rel, src, mode in (
        (BINARY, binary, 0o755),
        (RC_SCRIPT, os.path.join(PACKAGING, "cronstable.rc"), 0o755),
        (CONFIG_SAMPLE, os.path.join(PACKAGING, "cronstable.yaml"), 0o644),
    ):
        dst = os.path.join(root, rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)
        os.chmod(dst, mode)


def plist(path):
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("/" + BINARY + "\n")
        handle.write("/" + RC_SCRIPT + "\n")
        # @sample is the ports idiom for a configuration file: pkg installs the
        # .sample, copies it into place when the real file is absent, and on
        # deinstall removes the real file only if it still matches the sample.
        handle.write("@sample /" + CONFIG_SAMPLE + "\n")


def manifest(path, version):
    data = {
        "name": "cronstable",
        "version": version,
        "origin": "sysutils/cronstable",
        "comment": "Cron daemon with a schedule model you can inspect",
        "desc": (
            "cronstable runs scheduled jobs from a declarative configuration "
            "and answers questions cron cannot: what runs next, why a job did "
            "not run, which schedules collide, and what happened on the last "
            "run. It has a dashboard, an HTTP API, durable run history, "
            "dependency graphs, clustering and reporting.\n\n"
            "This package carries a self-contained binary with Python embedded, "
            "so it needs no Python on the host."
        ),
        "maintainer": "parker@cronstable.dev",
        "www": "https://github.com/ptweezy/cronstable",
        "prefix": PREFIX,
        "categories": ["sysutils"],
        "licenselogic": "single",
        "licenses": ["MIT"],
        # No "abi" or "arch" key on purpose: pkg fills both from the machine
        # running this, which is the machine the package targets. Spelling them
        # by hand is exactly where fpm goes wrong on aarch64.
        "scripts": {
            "post-install": read("postinstall.sh"),
            "pre-deinstall": read("preremove.sh"),
        },
    }
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")


def main(argv):
    if len(argv) != 4:
        return "usage: freebsd_pkg.py <version> <binary> <outdir>"
    version, binary, outdir = argv[1:]

    work = os.path.abspath("freebsd-pkg-work")
    root = os.path.join(work, "stage")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(root)
    stage(binary, root)

    meta = os.path.join(work, "meta")
    os.makedirs(meta)
    manifest(os.path.join(meta, "+MANIFEST"), version)
    plist_path = os.path.join(work, "plist")
    plist(plist_path)

    os.makedirs(outdir, exist_ok=True)
    # -m metadatadir with -p plist is the documented form for packaging files
    # that were never installed by pkg; -M (a bare manifest) does not accept a
    # plist and would need the file list inlined instead.
    subprocess.run(
        [
            "pkg", "create",
            "-m", meta,
            "-p", plist_path,
            "-r", root,
            "-o", outdir,
            "-v",
        ],
        check=True,
        env=dict(os.environ, PLIST_KEYWORDS_DIR=os.path.join(PACKAGING, "Keywords")),
    )
    built = [name for name in os.listdir(outdir) if name.startswith("cronstable-")]
    if len(built) != 1:
        return "expected one package in {}, got {}".format(outdir, built)
    print(os.path.join(outdir, built[0]))
    return None


if __name__ == "__main__":
    sys.exit(main(sys.argv))
