"""The MSI's service authoring stays in lockstep with winservice.py.

``packaging/msi/cronstable.wxs`` registers the service declaratively with
the same settings ``cronstable service install`` writes.  Nothing at run
time ties the two together, so this fence does: every mirrored value in the
``.wxs`` is asserted against the winservice function that owns it, and an
AST walk pins the set of configuration calls ``install()`` makes, so a new
setting added there fails here with a message naming the ``.wxs``.
"""

import argparse
import ast
import os
import xml.etree.ElementTree as ET

from cronstable import _cliargs, winservice
from cronstable._cliargs import SERVICE_NAME_DEFAULT

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WXS = os.path.join(ROOT, "packaging", "msi", "cronstable.wxs")
WINSERVICE = os.path.join(ROOT, "cronstable", "winservice.py")

NS = {
    "wxs": "http://wixtoolset.org/schemas/v4/wxs",
    "util": "http://wixtoolset.org/schemas/v4/wxs/util",
}

# Changing the UpgradeCode makes Windows Installer treat every already
# installed copy as an unrelated product, so upgrades stop upgrading and
# machines end up with two cronstables. It is fixed for the product's life.
UPGRADE_CODE = "b995cba8-16cd-48f1-a13b-c4c4b927e7be"


def _root():
    return ET.parse(WXS).getroot()


def _service_install(root):
    node = root.find(".//wxs:ServiceInstall", NS)
    assert node is not None, "cronstable.wxs lost its ServiceInstall"
    return node


def _service_controls(root):
    return root.findall(".//wxs:ServiceControl", NS)


def test_service_identity_matches_winservice():
    node = _service_install(_root())
    assert node.get("Name") == SERVICE_NAME_DEFAULT
    assert node.get("DisplayName") == winservice.display_name(
        SERVICE_NAME_DEFAULT
    )
    assert node.get("Description") == winservice.SERVICE_DESCRIPTION
    assert node.get("Type") == "ownProcess"
    assert node.get("Start") == "auto"
    assert node.get("ErrorControl") == "normal"
    # Absent on both sides means LocalSystem: install() passes a NULL
    # lpServiceStartName and the MSI omits Account. Spelling either out
    # would be a second string to keep in sync for the same default.
    assert node.get("Account") is None


def test_arguments_match_host_argv_shape(monkeypatch):
    # host_argv's one impurity is os.path.abspath on the config; identity
    # keeps the placeholder recognizable. The space in it forces the
    # list2cmdline quoting that closes the unquoted-service-path hole, so
    # this asserts the quoting too, not just the flag order.
    monkeypatch.setattr(winservice.os.path, "abspath", lambda p: p)
    argv = winservice.host_argv(
        config="C:\\config dir",
        name=SERVICE_NAME_DEFAULT,
        log_file=None,
        no_log_file=False,
        console=False,
        log_level="INFO",
        executable="X",
        frozen=True,
    )
    expected = winservice.image_path(argv[1:]).replace(
        '"C:\\config dir"', '"[CONFIGDIR]"'
    )
    assert '"[CONFIGDIR]"' in expected
    node = _service_install(_root())
    assert node.get("Arguments") == expected


def test_recovery_matches_failure_actions_plan():
    parser = argparse.ArgumentParser()
    _cliargs.add_service_command(parser.add_subparsers(dest="command"))
    args = parser.parse_args(["service", "install", "-c", "x"])
    actions, reset_s = winservice.failure_actions_plan(args.restart_delay)

    node = _root().find(".//util:ServiceConfig", NS)
    assert node is not None, "cronstable.wxs lost its util:ServiceConfig"
    type_names = {
        winservice.SC_ACTION_RESTART: "restart",
        winservice.SC_ACTION_NONE: "none",
    }
    assert [
        node.get("FirstFailureActionType"),
        node.get("SecondFailureActionType"),
        node.get("ThirdFailureActionType"),
    ] == [type_names[code] for code, _delay in actions]
    assert (
        int(node.get("RestartServiceDelayInSeconds")) * 1000 == actions[0][1]
    )
    assert int(node.get("ResetPeriodInDays")) * 86_400 == reset_s


def test_nonzero_exit_triggers_recovery():
    root = _root()
    node = root.find(".//wxs:ServiceConfig", NS)
    assert node is not None, "cronstable.wxs lost its native ServiceConfig"
    # SERVICE_CONFIG_FAILURE_ACTIONS_FLAG: without it the recovery actions
    # only fire on a crash, and this host reports failure as a clean stop
    # carrying an exit code (see winservice.set_failure_actions_flag).
    assert node.get("FailureActionsWhen") == "failedToStopOrReturnedError"
    assert node.get("OnInstall") == "yes"
    assert node.get("OnReinstall") == "yes"
    # The MsiServiceConfigFailureActions table is documented broken in the
    # Windows Installer SDK; the util extension's ChangeServiceConfig2
    # custom action is the working spelling.
    assert root.find(".//wxs:ServiceConfigFailureActions", NS) is None


def test_lifecycle_controls():
    root = _root()
    controls = _service_controls(root)
    stops = [c for c in controls if c.get("Stop") is not None]
    starts = [c for c in controls if c.get("Start") is not None]
    assert len(stops) == 1
    assert stops[0].get("Stop") == "both"
    assert stops[0].get("Remove") == "uninstall"
    assert stops[0].get("Wait") == "yes"
    assert stops[0].get("Start") is None
    # Exactly one start, and only under the explicit ask or the
    # upgrade-with-config condition: a first install has no configuration,
    # and starting an unconfigured service burns its recovery retries.
    assert len(starts) == 1
    assert starts[0].get("Start") == "install"
    holder = [
        c for c in root.findall(".//wxs:Component", NS) if starts[0] in list(c)
    ]
    assert len(holder) == 1
    condition = holder[0].get("Condition") or ""
    assert "STARTSERVICE" in condition
    assert "WIX_UPGRADE_DETECTED" in condition


def test_upgrade_scheduling_stays_early():
    root = _root()
    upgrade = root.find(".//wxs:MajorUpgrade", NS)
    assert upgrade is not None
    # Early RemoveExistingProducts is what makes the harvested payload's
    # auto-generated component GUIDs safe across releases; a later schedule
    # reintroduces MSI component rules for files that churn every release.
    assert upgrade.get("Schedule") == "afterInstallInitialize"
    package = root.find(".//wxs:Package", NS)
    assert package.get("UpgradeCode") == UPGRADE_CODE


def test_configdir_and_addpath_are_remembered_across_upgrades():
    # Windows Installer forgets command-line properties between
    # transactions: without the remember-property pattern, an upgrade
    # run without CONFIGDIR=... re-registers the service against the
    # default directory, and an ADDPATH=0 install grows its PATH entry
    # back. Fence every leg: the registry write, the AppSearch
    # read-back, and the save/restore pair that keeps an explicit
    # command-line value winning.
    root = _root()
    values = {
        v.get("Name"): v for v in root.findall(".//wxs:RegistryValue", NS)
    }
    properties = {
        p.get("Id"): p for p in root.findall(".//wxs:Property", NS)
    }
    setprops = root.findall(".//wxs:SetProperty", NS)
    for prop, reg_name in [("CONFIGDIR", "ConfigDir"), ("ADDPATH", "AddPath")]:
        # Written with the resolved value from an unconditional
        # component (PathEntry vanishes under ADDPATH=0, exactly when
        # AddPath most needs remembering).
        value = values[reg_name]
        assert value.get("Root") == "HKLM"
        assert value.get("Key") == "SOFTWARE\\cronstable"
        assert value.get("Value") == "[{}]".format(prop)
        holder = [
            c
            for c in root.findall(".//wxs:Component", NS)
            if value in list(c)
        ]
        assert len(holder) == 1
        assert holder[0].get("Condition") is None
        # Read back by AppSearch on the next transaction.
        search = properties[prop].find("wxs:RegistrySearch", NS)
        assert search is not None, "{} lost its read-back".format(prop)
        assert search.get("Root") == "HKLM"
        assert search.get("Key") == "SOFTWARE\\cronstable"
        assert search.get("Name") == reg_name
        # Command-line precedence: saved before AppSearch can clobber it,
        # restored after.
        save = [
            s
            for s in setprops
            if s.get("Id") == "CMDLINE_" + prop
            and s.get("Before") == "AppSearch"
        ]
        restore = [
            s
            for s in setprops
            if s.get("Id") == prop
            and s.get("After") == "AppSearch"
            and s.get("Value") == "[CMDLINE_{}]".format(prop)
        ]
        assert len(save) == 1, "{} lost its command-line save".format(prop)
        assert save[0].get("Condition") == prop
        assert len(restore) == 1, "{} lost its restore".format(prop)
    # ADDPATH's default must NOT be a declared Value="1":
    # indistinguishable from an explicit ADDPATH=1, whose save would
    # clobber a remembered 0.
    assert properties["ADDPATH"].get("Value") is None
    defaults = [
        s
        for s in setprops
        if s.get("Id") == "ADDPATH" and s.get("Condition") == "NOT ADDPATH"
    ]
    assert len(defaults) == 1 and defaults[0].get("Value") == "1"


def test_config_probe_follows_the_remembered_configdir():
    # The start-on-upgrade condition asks "does the PREVIOUS install's
    # config directory exist", not "does the default location exist",
    # so an upgrade with a custom CONFIGDIR gets its restart too. The
    # probe goes through the same remembered registry value as the
    # CONFIGDIR read-back.
    root = _root()
    found = next(
        p
        for p in root.findall(".//wxs:Property", NS)
        if p.get("Id") == "CRONSTABLE_CONFIG_FOUND"
    )
    search = found.find("wxs:RegistrySearch", NS)
    assert search is not None, "the config probe lost its registry hop"
    assert search.get("Root") == "HKLM"
    assert search.get("Key") == "SOFTWARE\\cronstable"
    assert search.get("Name") == "ConfigDir"
    assert search.get("Type") == "directory"
    # The nested DirectorySearch turns "the value exists" into "the
    # directory it names exists".
    assert search.find("wxs:DirectorySearch", NS) is not None


def test_install_sets_nothing_the_msi_does_not():
    with open(WINSERVICE, encoding="utf-8") as fobj:
        tree = ast.parse(fobj.read())
    install = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "install"
    )
    api_calls = {
        node.func.attr
        for node in ast.walk(install)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "api"
    }
    assert api_calls == {
        "create_service",
        "set_description",
        "set_delayed_auto_start",
        "set_failure_actions",
        "set_failure_actions_flag",
    }, (
        "winservice.install() gained or lost a configuration call; mirror "
        "the change in packaging/msi/cronstable.wxs and update this fence."
    )
