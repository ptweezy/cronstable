"""Update OS packages and remove build tools from released container images."""

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

APT = "apt-get -o Acquire::Retries=5"
DNF = "dnf --setopt=retries=10"
ZYPPER = "zypper --non-interactive"
UPDATES = {
    "debian": (
        f"{APT} update",
        f"{APT} upgrade -y --with-new-pkgs --no-install-recommends",
        f"{APT} clean",
    ),
    "alpine": ("apk upgrade --no-cache",),
    "amazonlinux": (
        f"{DNF} --releasever=latest upgrade -y",
        f"{DNF} clean all",
    ),
    "fedora": (f"{DNF} upgrade -y", f"{DNF} clean all"),
    "rhel": ("microdnf upgrade -y", "microdnf clean all"),
    "opensuse": (
        f"{ZYPPER} refresh",
        f"{ZYPPER} update --no-recommends",
        f"{ZYPPER} clean --all",
    ),
    # Distroless receives OS updates through its freshly pulled base image.
    "distroless": (),
}
UPDATES["ubuntu"] = UPDATES["debian"]
PREFIXES = ("opt/venv", "opt/python-runtime", "usr/local")
PACKAGING = (
    "pip",
    "pip-*.dist-info",
    "setuptools",
    "setuptools-*.dist-info",
    "setuptools-*.egg-info",
    "pkg_resources",
    "_distutils_hack",
    "distutils-precedence.pth",
    "wheel",
    "wheel-*.dist-info",
)
SCRIPTS = ("pip", "pip3", "pip3.*", "easy_install", "easy_install-*", "wheel")


def recipe(source, distro):
    distro = distro.removesuffix("-amd64v3")
    if distro not in UPDATES:
        raise ValueError(f"Unsupported refresh distro: {distro}")
    final_stage = re.split(r"(?im)^FROM\s+", source)[-1]
    users = re.findall(r"(?im)^USER\s+([^\r\n]+)", final_stage)
    if not users:
        raise ValueError("The released runtime must declare its USER")
    command = [
        "/opt/venv/bin/python",
        "/tmp/refresh-tools/refresh_runtime.py",
        "refresh",
        "--distro",
        distro,
    ]
    return (
        source.rstrip()
        + "\n\nUSER 0:0\n"
        + "RUN --mount=type=bind,from=refresh-tools,target=/tmp/refresh-tools "
        + json.dumps(command)
        + f"\nUSER {users[-1]}\n"
        + "COPY --from=dependency-sources / "
        + "/usr/share/doc/cronstable/third-party/\n"
    )


def run(command):
    args = shlex.split(command)
    for attempt in range(5):
        print("+ " + command, flush=True)
        result = subprocess.run(
            args,
            check=False,
            env=dict(os.environ, DEBIAN_FRONTEND="noninteractive"),
        )
        # Zypper's 102 requests a reboot; 103 requires another update pass.
        if result.returncode == 0 or (
            args[0] == "zypper" and result.returncode == 102
        ):
            return
        if attempt == 4:
            raise subprocess.CalledProcessError(result.returncode, args)
        time.sleep(5 * (attempt + 1))


def build_tools(root):
    """Find build tools in unmanaged Python installations."""
    paths = set()
    for relative in PREFIXES:
        prefix = root / relative
        for library in ("lib", "lib64"):
            for python in (prefix / library).glob("python*"):
                for directory in ("site-packages", "dist-packages"):
                    for pattern in PACKAGING:
                        paths.update((python / directory).glob(pattern))
                if (python / "ensurepip").exists():
                    paths.add(python / "ensurepip")
        for pattern in SCRIPTS:
            paths.update((prefix / "bin").glob(pattern))
    return sorted(paths)


def remove(root, path):
    if not path.parent.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"Cleanup path escapes the image root: {path}")
    if path.is_symlink():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def clean(root, distro):
    for path in build_tools(root):
        print(f"Removing {path}", flush=True)
        remove(root, path)
    if distro == "ubuntu":
        remove(root, root / "usr/bin/pebble")
    if distro in {"debian", "ubuntu"}:
        for path in (root / "var/lib/apt/lists").glob("*"):
            remove(root, path)


def check(root, distro):
    remaining = build_tools(root)
    if distro == "ubuntu" and (root / "usr/bin/pebble").exists():
        remaining.append(root / "usr/bin/pebble")
    if remaining:
        raise RuntimeError(
            "Runtime contains build tools: " + ", ".join(map(str, remaining))
        )
    print("Runtime cleanup verified", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("recipe", "refresh", "check"))
    parser.add_argument("--distro", required=True)
    parser.add_argument("--dockerfile", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    distro = args.distro.removesuffix("-amd64v3")
    if distro not in UPDATES:
        parser.error(f"Unsupported refresh distro: {distro}")
    if args.command == "recipe":
        if args.dockerfile is None or args.output is None:
            parser.error("recipe requires --dockerfile and --output")
        args.output.write_text(
            recipe(args.dockerfile.read_text(encoding="utf-8"), distro),
            encoding="utf-8",
        )
        return
    root = Path("/")
    if args.command == "refresh":
        if sys.platform != "linux" or sys.prefix != "/opt/venv":
            parser.error("refresh must run inside the container's /opt/venv")
        if os.getuid() != 0:
            parser.error("refresh requires root inside the container")
        for command in UPDATES[distro]:
            run(command)
        clean(root, distro)
    check(root, distro)


if __name__ == "__main__":
    main()
