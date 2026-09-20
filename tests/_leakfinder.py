"""Report resource leaks detected after each test.

Python can report an unclosed socket or event loop, or an unawaited
coroutine, when the object's finalizer runs during a later test. This plugin
records finalizer errors during each test, then collects garbage and records
warnings after the test. Reports include the test ID, message, and available
allocation trace. The test ID identifies when the report was collected;
it does not establish which test caused the leak.

Enable the plugin explicitly::

    python -X tracemalloc=25 -m pytest -p tests._leakfinder -p no:randomly

Reports are appended to ``leaks.txt``. Set ``CRONSTABLE_LEAK_OUT`` to use
another path. Without ``-X tracemalloc``, allocation traces are unavailable.
"""

import gc
import os
import sys
import tracemalloc
import warnings

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _allocation(obj):
    if obj is None or not tracemalloc.is_tracing():
        return ""
    trace = tracemalloc.get_object_traceback(obj)
    if trace is None:
        return "(no allocation trace)"
    ours = [
        "{}:{}".format(os.path.relpath(frame.filename, ROOT), frame.lineno)
        for frame in trace
        if frame.filename.startswith(ROOT) and ".tox" not in frame.filename
    ]
    if not ours:
        return "(allocated outside the repo: {})".format(trace[-1])
    return " <- ".join(reversed(ours[-5:]))


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_protocol(item, nextitem):
    found = []

    def record(unraisable):
        found.append(
            (repr(unraisable.exc_value)[:160], _allocation(unraisable.object))
        )

    # Record finalizer errors throughout the test, including those raised
    # during garbage collection. After the test, collect remaining objects
    # before the next test starts.
    previous = sys.unraisablehook
    sys.unraisablehook = record
    try:
        yield
        # Override warning filters to record finalizer warnings. The record's
        # source object lets tracemalloc look up its allocation trace.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            for _ in range(3):
                gc.collect()
        for warning in caught:
            found.append(
                (repr(warning.message)[:160], _allocation(warning.source))
            )
    finally:
        sys.unraisablehook = previous
    if found:
        out = os.environ.get("CRONSTABLE_LEAK_OUT", "leaks.txt")
        with open(out, "a", encoding="utf-8") as fobj:
            for message, where in found:
                fobj.write("{}\t{}\t{}\n".format(item.nodeid, message, where))
