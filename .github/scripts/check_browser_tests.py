"""Check browser test results in the full session's JUnit report."""

import argparse
import xml.etree.ElementTree as ET
from pathlib import Path


def check(report, modules):
    expected = {Path(module).stem for module in modules}
    if not expected:
        raise ValueError("No browser test modules were specified")
    cases = {module: [] for module in expected}
    for case in ET.parse(report).getroot().iter("testcase"):
        names = set(case.get("classname", "").split("."))
        if case.get("file"):
            names.add(Path(case.get("file")).stem)
        for module in expected & names:
            cases[module].append(case)
    problems = []
    for module, tests in sorted(cases.items()):
        if not tests:
            problems.append(f"{module}: no tests were collected")
        for case in tests:
            label = f"{module}::{case.get('name', '')}"
            if (
                case.find("failure") is not None
                or case.find("error") is not None
            ):
                problems.append(f"{label}: failed")
            skipped = case.find("skipped")
            if skipped is not None and skipped.get("type") != "pytest.xfail":
                problems.append(f"{label}: skipped")
    if problems:
        raise ValueError(
            "Browser test enforcement failed:\n" + "\n".join(problems)
        )
    return sum(map(len, cases.values()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("modules", nargs="+")
    args = parser.parse_args()
    count = check(args.report, args.modules)
    print(f"Verified {count} browser test results; no unexpected skips")
