#!/bin/sh
# Best-effort: bundle orjson (the `speedups` extra) into the image's /opt/venv
# to accelerate the durable-state and cluster-gossip JSON paths. cronstable
# falls back to the stdlib json whenever orjson is absent (cronstable/_json),
# so no install path here may fail the image build: given its argument the
# script always exits 0 and just logs, per arch, which way this image went
# (grep the build log for "orjson").
#
# Almost every published arch installs a prebuilt wheel and needs no
# toolchain. On a wheel-less arch the first install attempt fails, and when
# the caller provides RUST_SETUP it is run to install a Rust toolchain and the
# install is retried as a source build. A source build (especially under QEMU)
# can import yet be miscompiled, so orjson_ok round-trips the result after
# either path and a failure uninstalls it: a broken orjson is never shipped.
# The toolchain stays in the builder stage; only the small compiled orjson .so
# rides along in /opt/venv.
#
# Usage: sh install_orjson.sh "orjson>=X.Y"
#
#   $1          the requirement to install. An argument rather than a constant
#               here, so every Dockerfile keeps spelling the version floor
#               that tests/test_extra_pins_parity.py checks against
#               pyproject's canonical speedups floor.
#   RUST_SETUP  optional env: shell command run when no wheel installs
#               cleanly, providing a Rust able to build orjson. The rustup
#               callers (Debian/Ubuntu/Alpine, for riscv64) install a CURRENT
#               stable into /opt/cargo + /opt/rustup because the distro rustc
#               is older than orjson's MSRV; the dnf/zypper callers
#               (Fedora/RHEL/openSUSE, for ppc64le/s390x) install the
#               distro's own rust + cargo, new enough there. Callers whose
#               whole published arch set has wheels (Amazon Linux,
#               distroless: amd64/arm64 only) leave it unset, which skips
#               the source-build fallback.
#
# This is the container sibling of pyinstaller/install_orjson.sh, which does
# the same job for the binary lanes. The two stay separate scripts on purpose:
#   - the requirement is an argument here (see above); the pyinstaller script
#     hardcodes it, and tests/test_extra_pins_parity.py checks both spellings
#     against pyproject's canonical speedups floor.
#   - verification is inline here; the pyinstaller script delegates to its
#     sibling verify_extra.py, which the image builds do not COPY (the builder
#     layers above the source COPY read only pyproject.toml and the docker/
#     helper scripts). The inline probe round-trips the same sample as
#     verify_extra.py's _verify_orjson (compact bytes, OPT_SORT_KEYS,
#     non-ASCII: the case a QEMU-miscompiled build typically fails); keep
#     the two probes in step.
#   - paths are fixed: all eight builder stages install into /opt/venv, so the
#     PIP/PY indirection the binary lanes need (uv vs pip) has no use here,
#     and the image pip flags (--no-cache-dir, the 120s wheel / 300s
#     source-build timeouts) are baked in.
#
# Deliberately NOT `set -e`: every step is best-effort and handled inline.
set -u

spec="${1:?usage: install_orjson.sh \"orjson>=X.Y\"}"
pip=/opt/venv/bin/pip

# The \u escapes spell verify_extra.py's non-ASCII sample while keeping this
# file ASCII, so no build-stage locale can mangle the argv bytes.
orjson_ok() {
    /opt/venv/bin/python -c 'import orjson,sys; s={"schemaVersion":"v1","z":1,"a":"caf\u00e9 \u2603 \u65e5\u672c","n":[1,2.5,True,None]}; b=orjson.dumps(s,option=orjson.OPT_SORT_KEYS); sys.exit(0 if isinstance(b,bytes) and orjson.loads(b)==s else 1)'
}

# The /opt/cargo PATH entry and the two HOME vars serve the rustup callers;
# for the distro-rust callers the prefix is inert (no /opt/cargo/bin exists,
# cargo just caches under /opt/cargo instead of ~/.cargo, builder-stage only).
if "$pip" install --no-cache-dir --timeout 120 "$spec" && orjson_ok; then
    echo "orjson: bundled (wheel)"
elif [ -n "${RUST_SETUP:-}" ] && sh -c "$RUST_SETUP" \
    && env PATH="/opt/cargo/bin:$PATH" CARGO_HOME=/opt/cargo \
        RUSTUP_HOME=/opt/rustup \
        "$pip" install --no-cache-dir --timeout 300 "$spec" && orjson_ok; then
    echo "orjson: bundled (source build)"
else
    echo "orjson: unavailable on this arch; using stdlib json"
    "$pip" uninstall -y orjson || true
fi
