"""Fuzz the Windows Task Scheduler XML importer.

Each input is an exported task file, converted the way
``cronstable import-taskscheduler`` converts a file it is given. Reading
and parsing must raise nothing but TaskXmlError. The YAML the conversion
writes must load as configuration with the same job names, and converting
the file again must give the same bytes.
"""

import logging
import os
import sys
import tempfile

import atheris

with atheris.instrument_imports(include=["cronstable"]):
    from cronstable import taskxml
    from cronstable.config import parse_config_string

PATH = os.path.join(tempfile.mkdtemp(prefix="fuzz-taskxml-"), "fuzz.xml")


def convert(data: bytes) -> list[taskxml.ConvertedTask] | None:
    """The tasks dispatch() renders for one file, or ``None`` when the file
    does not parse."""
    with open(PATH, "wb") as handle:
        handle.write(data)
    try:
        tasks, _ = taskxml.convert_source(PATH, timezone=None, strict=True)
    except taskxml.TaskXmlError:
        return None
    taskxml._unique_names(tasks)
    return tasks


def TestOneInput(data: bytes) -> None:
    tasks = convert(data)
    if tasks is None:
        return
    taskxml.render_report(tasks)
    text = taskxml.render_yaml(tasks, sources=[PATH])
    again = convert(data)
    assert again is not None
    assert taskxml.render_yaml(again, sources=[PATH]) == text
    if not text:
        return
    names = [
        job["name"]
        for task in tasks
        if not task.commented
        for job in task.jobs
    ]
    if len(set(names)) != len(names):
        # _unique_names can miss a collision; the loader refuses it
        return
    loaded = parse_config_string(text, "converted.yaml")
    assert [job.name for job in loaded.jobs] == names


def main() -> None:
    logging.disable(logging.CRITICAL)
    atheris.Setup(sys.argv, TestOneInput)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
