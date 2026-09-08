"""Validate and assemble OCI archives offline, without rebuilding."""

import argparse
import hashlib
import json
import re
import shutil
import tarfile
import tempfile
from pathlib import Path

INDEX = "application/vnd.oci.image.index.v1+json"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"


def unpack(archive, directory):
    """Allow only regular OCI files; verify blobs while streaming them."""
    with tarfile.open(archive) as tar:
        seen = set()
        for member in tar:
            name = member.name.removeprefix("./")
            if member.isdir() and name.rstrip("/") in {
                "",
                ".",
                "blobs",
                "blobs/sha256",
            }:
                continue
            if not member.isfile() or not (
                name in {"index.json", "oci-layout"}
                or re.fullmatch(r"blobs/sha256/[a-f0-9]{64}", name)
            ):
                raise ValueError(f"Unexpected OCI archive member: {name}")
            if name in seen:
                raise ValueError(f"Duplicate OCI archive member: {name}")
            seen.add(name)
            target = directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            with tar.extractfile(member) as source, target.open("wb") as dest:
                while block := source.read(1024 * 1024):
                    digest.update(block)
                    dest.write(block)
            if name.startswith("blobs/") and digest.hexdigest() != target.name:
                raise ValueError(f"Corrupt OCI blob: {name}")


def blob(root, descriptor):
    digest = descriptor["digest"]
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
        raise ValueError("Unsupported OCI digest")
    path = root / "blobs/sha256" / digest.split(":")[1]
    if path.stat().st_size != descriptor["size"]:
        raise ValueError(f"Incorrect OCI blob size: {digest}")
    return path


def manifests(root, descriptor, depth=0):
    if depth > 4:
        raise ValueError("Excessively nested OCI index")
    body = json.loads(blob(root, descriptor).read_bytes())
    if descriptor["mediaType"] == INDEX:
        return [
            item
            for child in body["manifests"]
            for item in manifests(root, child, depth + 1)
        ]
    if descriptor["mediaType"] != MANIFEST:
        raise ValueError("Expected an OCI image manifest")
    for layer in body["layers"]:
        blob(root, layer)
    config = json.loads(blob(root, body["config"]).read_bytes())
    return [(descriptor, config)]


def verify(root, platform, version, revision):
    layout = json.loads((root / "oci-layout").read_bytes())
    if layout != {"imageLayoutVersion": "1.0.0"}:
        raise ValueError("Unsupported OCI layout")
    index = json.loads((root / "index.json").read_bytes())
    images = [
        item for child in index["manifests"] for item in manifests(root, child)
    ]
    if len(images) != 1:
        raise ValueError("Expected exactly one image per platform artifact")
    descriptor, config = images[0]
    os_name, arch, *variant = platform.split("/")
    if config["os"] != os_name or config["architecture"] != arch:
        raise ValueError(f"OCI architecture does not match {platform}")
    actual_variant = config.get("variant") or descriptor.get(
        "platform", {}
    ).get("variant", "")
    if variant and actual_variant != variant[0]:
        raise ValueError(f"OCI variant does not match {platform}")
    labels = config.get("config", {}).get("Labels", {})
    if labels.get("org.opencontainers.image.version") != version:
        raise ValueError("OCI version does not match this build")
    if labels.get("org.opencontainers.image.revision") != revision:
        raise ValueError("OCI revision does not match this build")
    result = dict(descriptor, platform={"os": os_name, "architecture": arch})
    result.pop("annotations", None)
    if variant:
        result["platform"]["variant"] = variant[0]
    return result


def merge(inputs, distro, platforms, output, version, revision):
    expected = platforms.split(",")
    if len(set(expected)) != len(expected):
        raise ValueError("Duplicate expected platform")
    output.mkdir(parents=True)
    (output / "blobs/sha256").mkdir(parents=True)
    descriptors = []
    for platform in expected:
        archive = (
            inputs
            / f"image-{distro}-{platform.replace('/', '-')}"
            / "image.tar"
        )
        # download-artifact extracts a single match directly into inputs.
        if len(expected) == 1 and not archive.exists():
            archive = inputs / "image.tar"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            unpack(archive, root)
            descriptors.append(verify(root, platform, version, revision))
            shutil.copytree(
                root / "blobs", output / "blobs", dirs_exist_ok=True
            )
    index = json.dumps(
        {"schemaVersion": 2, "mediaType": INDEX, "manifests": descriptors},
        sort_keys=True,
    ).encode()
    digest = hashlib.sha256(index).hexdigest()
    (output / "blobs/sha256" / digest).write_bytes(index)
    (output / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}')
    (output / "index.json").write_text(
        json.dumps(
            {
                "schemaVersion": 2,
                "manifests": [
                    {
                        "mediaType": INDEX,
                        "digest": "sha256:" + digest,
                        "size": len(index),
                        "annotations": {
                            "org.opencontainers.image.ref.name": "release"
                        },
                    }
                ],
            }
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--distro", required=True)
    parser.add_argument("--platforms", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    merge(
        args.inputs,
        args.distro,
        args.platforms,
        args.output,
        args.version,
        args.revision,
    )


if __name__ == "__main__":
    main()
