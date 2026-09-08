"""Verify that WinGet manifests match the release MSI metadata."""

import copy
import importlib.util
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from strictyaml.ruamel import YAML

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "render_winget", ROOT / ".github/scripts/render_winget.py"
)
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)


@pytest.fixture
def metadata():
    return {
        arch: {
            "Architecture": architecture,
            "ProductVersion": "1.2.50",
            "ProductCode": "{12345678-1234-1234-1234-1234567890A"
            + str(i)
            + "}",
            "UpgradeCode": "{B995CBA8-16CD-48F1-A13B-C4C4B927E7BE}",
            "ProductName": "cronstable",
            "Manufacturer": "cronstable",
            "Sha256": str(i) * 64,
        }
        for i, (arch, architecture) in enumerate(
            renderer.ARCHES.items(), start=1
        )
    }


def read_manifests(tmp_path):
    yaml = YAML(typ="safe")
    return {
        data["ManifestType"]: data
        for path in tmp_path.glob("*.yaml")
        for data in [yaml.load(path.read_text("utf-8"))]
    }


def test_published_msi_metadata_and_identity(metadata, tmp_path):
    renderer.render("1.2.50", metadata, tmp_path)
    manifests = read_manifests(tmp_path)
    assert set(manifests) == {"version", "installer", "defaultLocale"}
    for manifest in manifests.values():
        assert manifest["PackageIdentifier"] == "ptweezy.cronstable"
        assert manifest["PackageVersion"] == "1.2.50"
    installer = manifests["installer"]
    assert installer["InstallerType"] == "wix"
    assert installer["Scope"] == "machine"
    assert installer["ElevationRequirement"] == "elevationRequired"
    assert installer["UpgradeBehavior"] == "install"
    assert len(installer["Installers"]) == 2
    for entry, (arch, source) in zip(
        installer["Installers"], metadata.items(), strict=True
    ):
        assert entry["Architecture"] == source["Architecture"]
        assert entry["InstallerUrl"] == (
            "https://github.com/ptweezy/cronstable/releases/download/1.2.50/"
            f"cronstable-windows-{arch}.msi"
        )
        assert entry["InstallerSha256"] == source["Sha256"]
        assert entry["ProductCode"] == source["ProductCode"]
        arp = entry["AppsAndFeaturesEntries"][0]
        assert arp["ProductCode"] == source["ProductCode"]
        assert arp["UpgradeCode"] == source["UpgradeCode"]
        assert arp["DisplayVersion"] == source["ProductVersion"]
        assert arp["Publisher"] == source["Manufacturer"]
        assert arp["DisplayName"] == source["ProductName"]
        assert arp["InstallerType"] == "msi"


@pytest.mark.parametrize(
    "field,value",
    [
        ("ProductVersion", "1.2.49"),
        ("Architecture", "arm64"),
        ("Sha256", "missing"),
        ("ProductCode", "not-a-guid"),
        ("UpgradeCode", "not-a-guid"),
    ],
)
def test_invalid_metadata_writes_no_manifest(metadata, tmp_path, field, value):
    metadata["amd64"][field] = value
    with pytest.raises(ValueError):
        renderer.render("1.2.50", metadata, tmp_path)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("version", ["../bad", "1.2.50-beta", "1.2"])
def test_invalid_version(metadata, tmp_path, version):
    with pytest.raises(ValueError, match="Invalid release version"):
        renderer.render(version, metadata, tmp_path)


def test_missing_architecture_is_not_silently_dropped(metadata, tmp_path):
    del metadata["arm64"]
    with pytest.raises(ValueError, match="amd64 and arm64"):
        renderer.render("1.2.50", metadata, tmp_path)


def test_rebuild_uses_new_product_codes_and_hashes(metadata, tmp_path):
    renderer.render("1.2.50", metadata, tmp_path)
    rebuilt = copy.deepcopy(metadata)
    rebuilt["amd64"]["ProductCode"] = metadata["arm64"]["ProductCode"]
    rebuilt["amd64"]["Sha256"] = "a" * 64
    renderer.render("1.2.50", rebuilt, tmp_path)
    entry = read_manifests(tmp_path)["installer"]["Installers"][0]
    assert entry["ProductCode"] == rebuilt["amd64"]["ProductCode"]
    assert entry["InstallerSha256"] == "A" * 64


def test_submission_requires_signed_scanned_validated_msis():
    workflow = YAML(typ="safe").load(
        (ROOT / ".github/workflows/release.yml").read_text("utf-8")
    )
    job = workflow["jobs"]["winget"]
    assert {"release", "sign-windows"} <= set(job["needs"])
    steps = {s.get("name"): s for s in job["steps"]}
    names = list(steps)
    gates = [
        "Download signed winget installers",
        "Verify and Defender-scan winget installers",
        "Generate and validate winget manifests",
        "Validate with the winget client",
        "Submit winget manifest",
    ]
    assert [names.index(n) for n in gates] == sorted(
        names.index(n) for n in gates
    )
    for name in gates:
        assert steps[name].get("if", "success()") == "success()"
        assert not steps[name].get("continue-on-error", False)
    download = steps[gates[0]]
    assert "needs.sign-windows.outputs.signed" in download["env"]["SIGNED"]
    assert '"$SIGNED" != true' in download["run"]
    assert "cronstable-windows-amd64.msi" in download["run"]
    assert "cronstable-windows-arm64.msi" in download["run"]
    assert "SHA256SUMS" in download["run"]
    evidence = steps["Preserve winget validation evidence"]
    assert evidence["if"] == "always()"
    assert "*.log" in evidence["with"]["path"]
    assert "wingetcreate.exe update" not in steps[gates[-1]]["run"]
    extractor = steps["Install the MSI extractor"]["run"]
    assert "WIX_TOOL_VERSION" in extractor
    assert ".github/scripts/build_msi.sh" in extractor


def test_preflight_payload_id_matches_msi_authoring():
    root = ET.parse(ROOT / "packaging/msi/cronstable.wxs").getroot()
    files = root.findall(".//{http://wixtoolset.org/schemas/v4/wxs}File")
    assert len([f for f in files if f.get("Id") == "CronstableExe"]) == 1
    script = (ROOT / ".github/scripts/prepare_winget.ps1").read_text("utf-8")
    assert "@Id='CronstableExe'" in script
