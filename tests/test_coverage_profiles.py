"""The pragma vocabulary and the two per-OS coverage profiles.

Every Windows branch in the tree used to carry a bare ``# pragma: no
cover``, so on the Windows CI rows the coverage gate could not see the code
that actually ran there, while the POSIX code that can never run there
stayed in the denominator and counted as missed.  The file's score was
inverted from reality.

The fix is three pragma tokens and two profiles.  A bare ``# pragma: no
cover`` is hidden on every OS; ``# pragma: no cover (windows)`` is hidden on
POSIX and MEASURED on Windows; ``# pragma: no cover (posix)`` is the mirror.
``pyproject.toml``'s ``exclude_lines`` picks the profile from
``CRONSTABLE_COVERAGE_SKIP``, which ``tox.ini`` sets from a ``windows`` /
``posix`` factor.

Four things here are easy to get wrong and quiet when you do, which is why
each one has a test rather than a comment:

* **The tox platform selector.**  tox matches ``platform`` against
  ``sys.platform`` with ``re.fullmatch``, so the POSIX arm has to be
  ``(?!win32).*``.  The bare zero-width lookahead ``(?!win32)`` can only
  fullmatch the empty string, so it matches no platform at all and the
  POSIX arm skips everywhere: the suite would stop running on Linux
  entirely.  (Measured against tox 4.58.0: that is loud rather than quiet.
  A skip counts as success only when at least one selected env actually
  ran, so an invocation whose every env skips prints ``evaluation failed``
  and exits 1.)  The assertions below are on what the regexes MATCH, never
  on their text, precisely so that a test can never freeze a broken form
  in.
* **The profile inversion.**  Each arm names the platform whose branches
  CANNOT run in it.  Wired backwards, the number moves by tenths and the
  gate still passes.
* **``exclude_lines`` replaces coverage's defaults** rather than appending
  to them (that is ``exclude_also``, and its catch-all pragma rule would
  swallow the tagged pragmas before the OS rule ever saw them).  So the two
  defaults this project still wants have to be copied in verbatim, and rule
  1 has to stay coverage's own pragma rule plus a lookahead rather than a
  hand-rolled third spelling.
* **``commands`` stays unconditional.**  A fully factored ``commands``
  resolves to nothing in an unfactored env, so ``tox -e py`` and ``tox -e
  py312`` would build a venv, run zero tests and report OK.
"""

import ast
import configparser
import os
import re

import pytest

import coverage.config

tomllib = pytest.importorskip("tomllib")  # py3.11+; the other cells enforce

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE = os.path.join(ROOT, "cronstable")

#: The whole vocabulary.  A fourth token would need a third profile (and a
#: matrix row to measure it in), so the set is closed on purpose;
#: ``cronstable/tui.py``'s macOS branch stays a bare pragma for that reason.
TOKENS = ("windows", "posix")

#: Derived from the rule it guards rather than restated, so the scan and the
#: gate share one vocabulary by construction.  A hand-rolled spelling drifts:
#: the obvious ``no\s*cover`` with a mandatory colon misses ``# pragma no
#: cover (windwos)``, which coverage excludes and these tests would then
#: never see.  No IGNORECASE, because coverage does not use it either.
_PRAGMA = re.compile(coverage.config.DEFAULT_EXCLUDE[0])
_PARENTHESIZED = re.compile(r"\(([A-Za-z0-9_]+)\)")
#: The placeholder pyproject.toml hands to coverage's ``${VAR-default}``
#: substitution.  Its default is the historical POSIX-shaped measurement, so
#: a bare ``pytest --cov`` or an IDE run reports what it always did.
_PLACEHOLDER = "${CRONSTABLE_COVERAGE_SKIP-windows}"


def _source_files():
    for dirpath, _dirs, names in os.walk(SOURCE):
        for name in sorted(names):
            if name.endswith(".py"):
                yield os.path.join(dirpath, name)


def _read(path):
    with open(path, encoding="utf-8") as fobj:
        return fobj.read()


def _pragma_lines():
    """Every ``(relative path, lineno, line)`` carrying a pragma."""
    for path in sorted(_source_files()):
        rel = os.path.relpath(path, ROOT).replace(os.sep, "/")
        for num, line in enumerate(_read(path).splitlines(), 1):
            if _PRAGMA.search(line):
                yield rel, num, line


def _pyproject():
    with open(os.path.join(ROOT, "pyproject.toml"), "rb") as fobj:
        return tomllib.load(fobj)


def _exclude_lines():
    return _pyproject()["tool"]["coverage"]["report"]["exclude_lines"]


def _tox():
    parser = configparser.RawConfigParser()
    parser.read(os.path.join(ROOT, "tox.ini"), encoding="utf-8")
    return parser


def _conditional(value):
    """A tox factor-conditional block -> ``{factor or None: [values]}``."""
    out = {}
    for raw in value.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"([A-Za-z0-9_,-]+):\s*(.*)$", line)
        if match and not match.group(2).startswith("//"):
            out.setdefault(match.group(1), []).append(match.group(2))
        else:
            out.setdefault(None, []).append(line)
    return out


def _profile_rules(profile):
    """``exclude_lines`` compiled for one profile."""
    return [
        re.compile(rule.replace(_PLACEHOLDER, profile))
        for rule in _exclude_lines()
    ]


def _excluded(line, profile):
    return any(rule.search(line) for rule in _profile_rules(profile))


# --- the vocabulary in the source tree ------------------------------------


def test_every_pragma_uses_the_declared_vocabulary():
    """No fourth token creeps in unannounced.

    ``(win32)``, ``(darwin)`` or a typo'd ``(window)`` would silently mean
    "hidden everywhere": rule 1's lookahead only spares the two declared
    tokens, so an unknown one falls through to the catch-all and the branch
    goes back to being invisible on both OSes.
    """
    strays = []
    for rel, num, line in _pragma_lines():
        tail = line[_PRAGMA.search(line).end() :]
        for token in _PARENTHESIZED.findall(tail):
            if token not in TOKENS:
                strays.append("{}:{}: ({})".format(rel, num, token))
    assert not strays, "undeclared pragma tokens: {}".format(strays)


def test_a_pragma_may_carry_at_most_one_token():
    """One line, one profile.  Both tokens would exclude it everywhere."""
    doubled = []
    for rel, num, line in _pragma_lines():
        tail = line[_PRAGMA.search(line).end() :]
        found = [t for t in _PARENTHESIZED.findall(tail) if t in TOKENS]
        if len(found) > 1:
            doubled.append("{}:{}: {}".format(rel, num, found))
    assert not doubled, "pragmas naming two profiles: {}".format(doubled)


def test_windows_only_pragmas_carry_the_windows_token():
    """A pragma on a Windows test must say so, or it hides on Windows too.

    Judged on the CODE the pragma sits on, not on the trailing prose: a
    ``(posix)`` clause is often explained by saying what Windows lacks, and
    that sentence must not be read as the guard.  The negated form (``if
    not IS_WINDOWS``) selects the POSIX clause and so takes the other
    token; that is the one case where a line whose code names Windows
    legitimately carries ``(posix)``.
    """
    names_windows = re.compile(r"IS_WINDOWS|win32|windows", re.IGNORECASE)
    negation = re.compile(r"not\s+IS_WINDOWS|platform\s*!=\s*.win32.")
    untagged = []
    for rel, num, line in _pragma_lines():
        code = line[: _PRAGMA.search(line).start()]
        if not names_windows.search(code):
            # No guard to judge.  Prose that mentions Windows still must
            # not ride a BARE pragma, which would hide the site on Windows
            # too; a site that already picked a token picked deliberately.
            if names_windows.search(line) and not _PARENTHESIZED.findall(
                line[_PRAGMA.search(line).end() :]
            ):
                untagged.append("{}:{}: wants a token".format(rel, num))
            continue
        wanted = "(posix)" if negation.search(code) else "(windows)"
        if wanted not in line:
            untagged.append("{}:{}: wants {}".format(rel, num, wanted))
    assert not untagged, "untagged OS pragmas: {}".format(untagged)


def test_every_tagged_os_branch_tags_its_other_side():
    """A tagged platform branch is tagged on BOTH sides, in every module.

    The rule the docs state is scoped: a platform branch takes a token
    where its clause genuinely cannot execute on the other OS (it reaches
    for ``msvcrt``, ``fcntl``, ``grp``/``pwd``, ``os.nice``, ``os.killpg``,
    ``ctypes.windll``).  Branches whose bodies are plain Python that the
    tests drive from either box by injecting ``IS_WINDOWS`` stay untagged
    on purpose, because they really are measured on both.

    What is enforceable, and what this checks, is that the decision is not
    taken by halves.  Tagging one side and not the other is the failure
    mode that has actually happened twice here: the untagged half stays in
    a denominator it can never be executed in, the number quietly drops,
    and nothing says so.  Both shapes are covered, an ``if``/``else`` pair
    and a fall-through tail (a body that returns or raises, with the other
    OS's code as the next statement rather than inside an ``else``).
    """
    missing = []
    for path in sorted(_source_files()):
        rel = os.path.relpath(path, ROOT).replace(os.sep, "/")
        lines = _read(path).splitlines()
        for parent in ast.walk(ast.parse("\n".join(lines))):
            for body in _bodies(parent):
                missing.extend(_untagged_halves(rel, lines, body))
    assert not missing, "half-tagged platform branches: {}".format(missing)


def _bodies(node):
    """Every statement list hanging off ``node`` (its blocks)."""
    for field in ("body", "orelse", "finalbody"):
        block = getattr(node, field, None)
        if isinstance(block, list) and block:
            yield block
    for handler in getattr(node, "handlers", []) or []:
        if handler.body:
            yield handler.body


def _untagged_halves(rel, lines, body):
    """The half-tagged platform branches in one statement list."""
    out = []
    for index, node in enumerate(body):
        if not isinstance(node, ast.If):
            continue
        side = _platform_side(node.test)
        if side is None:
            continue
        other = "posix" if side == "windows" else "windows"
        header = lines[node.lineno - 1]
        header_tagged = "({})".format(side) in header
        else_line = _else_line(lines, node)
        if else_line is not None:
            other_line, other_tagged = else_line, (
                "({})".format(other) in lines[else_line - 1]
            )
        elif _exits(node.body) and index + 1 < len(body):
            # A fall-through tail: the next statement IS the other clause.
            other_line = body[index + 1].lineno
            other_tagged = "({})".format(other) in lines[other_line - 1]
        else:
            continue
        if header_tagged and not other_tagged:
            out.append("{}:{}: wants ({})".format(rel, other_line, other))
        elif other_tagged and not header_tagged:
            out.append("{}:{}: wants ({})".format(rel, node.lineno, side))
    return out


def _exits(body):
    """Whether a clause always leaves, so what follows is the other arm."""
    return isinstance(body[-1], (ast.Return, ast.Raise))


def _platform_side(test):
    """Which profile an ``if`` test selects, or None if it is not one."""
    negated = False
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
        test, negated = test.operand, True
    source = ast.unparse(test)
    if source in ("IS_WINDOWS", "platform.IS_WINDOWS"):
        return "posix" if negated else "windows"
    if source in ("sys.platform == 'win32'", "platform.system() == 'Windows'"):
        return "posix" if negated else "windows"
    if source == "sys.platform != 'win32'":
        return "windows" if negated else "posix"
    return None


def _else_line(lines, node):
    """The ``else:`` line number of ``node``, or None (no else / elif)."""
    if not node.orelse:
        return None
    for index in range(node.orelse[0].lineno - 2, node.lineno - 1, -1):
        if lines[index].strip() == "else:" or lines[index].strip().startswith(
            "else:  #"
        ):
            return index + 1
    return None


# --- the two profiles in pyproject.toml -----------------------------------


def test_exclude_lines_keeps_coverages_own_pragma_rule():
    """Rule 1 is coverage's DEFAULT_EXCLUDE[0] plus a lookahead.

    Hand-rolling a third spelling is how the uppercase alternatives get
    dropped and the colon becomes mandatory; pinning the relationship means
    an upstream change to coverage's own vocabulary fails here instead of
    quietly narrowing ours.
    """
    rules = _exclude_lines()
    default = coverage.config.DEFAULT_EXCLUDE
    assert len(default) == 3, "coverage's defaults moved: {}".format(default)
    assert len(rules) == 4, rules
    assert rules[0].startswith(default[0])
    assert rules[0] == default[0] + r"(?!.*\((windows|posix)\))"


def test_exclude_lines_keeps_the_two_defaults_it_replaces():
    """The stub-body and TYPE_CHECKING rules survive the replacement.

    ``exclude_lines`` REPLACES coverage's defaults, so the two this project
    still wants have to be copied in.  Asserted by what they DO, not by
    their text: coverage rewords those two regexes between patch releases
    (7.14.1 spells the stub rule ``?\\)(\\s*->``, 7.14.3 ``?[\\])]+(\\s*->``),
    and a verbatim pin turns an upstream cosmetic edit into a red matrix on
    every cell whose resolved coverage is one patch out of step.
    """
    for profile in TOKENS:
        assert _excluded("    def f() -> int: ...", profile)
        assert _excluded("    def f(self): ...", profile)
        assert _excluded("class P(Protocol):", profile) is False
        assert _excluded("if TYPE_CHECKING:", profile)
        assert _excluded("if typing.TYPE_CHECKING:", profile)


def test_the_os_rule_defaults_to_the_historical_profile():
    rules = _exclude_lines()
    assert _PLACEHOLDER in rules[1], rules[1]
    # Default `windows` means "skip the Windows branches", i.e. exactly what
    # a bare pragma did before this existed.
    assert rules[1].startswith(coverage.config.DEFAULT_EXCLUDE[0])


@pytest.mark.parametrize(
    "line",
    [
        pytest.param("    x = 1  # pragma: no cover", id="bare"),
        pytest.param("    x = 1  # pragma: no cover - why", id="bare-prose"),
        pytest.param("    x = 1  # PRAGMA: NO COVER", id="uppercase"),
        pytest.param("    x = 1  # pragma no cover", id="no-colon"),
    ],
)
def test_a_bare_pragma_is_hidden_under_both_profiles(line):
    assert _excluded(line, "windows")
    assert _excluded(line, "posix")


def test_a_tagged_pragma_is_hidden_only_in_the_other_profile():
    windows_only = '    if IS_WINDOWS:  # pragma: no cover (windows)'
    posix_only = "    else:  # pragma: no cover (posix)"
    # Profile `windows` is the POSIX run: it skips the Windows branches and
    # measures the POSIX ones.
    assert _excluded(windows_only, "windows")
    assert not _excluded(posix_only, "windows")
    # Profile `posix` is the Windows run: the mirror.
    assert _excluded(posix_only, "posix")
    assert not _excluded(windows_only, "posix")


def test_the_token_may_ride_after_the_prose():
    """Both rules accept the token anywhere after ``cover``.

    Requiring it to sit immediately after ``cover`` would force the trailing
    explanation off every site that has one and still fits in 79 columns.
    """
    trailing = "    if IS_WINDOWS:  # pragma: no cover - the fast path "
    trailing += "(windows)"
    assert _excluded(trailing, "windows")
    assert not _excluded(trailing, "posix")


# --- the two profiles in tox.ini ------------------------------------------


def test_envlist_carries_both_arms():
    envlist = _tox()["tox"]["envlist"]
    assert re.search(r"\{[^}]*windows[^}]*posix[^}]*\}", envlist) or re.search(
        r"\{[^}]*posix[^}]*windows[^}]*\}", envlist
    ), envlist


def test_the_platform_selectors_match_the_right_platforms():
    """Asserted by behavior, never by text.

    tox uses ``re.fullmatch``.  A test that pinned the literal
    ``(?!win32)`` would enshrine a selector that fullmatches only the empty
    string, so the POSIX arm skips on every platform and the suite stops
    running on Linux.
    """
    arms = _conditional(_tox()["testenv"]["platform"])
    windows = arms["windows"][0]
    posix = arms["posix"][0]

    assert re.fullmatch(windows, "win32")
    assert re.fullmatch(windows, "linux") is None
    assert re.fullmatch(windows, "darwin") is None

    assert re.fullmatch(posix, "linux")
    assert re.fullmatch(posix, "darwin")
    assert re.fullmatch(posix, "freebsd13")
    assert re.fullmatch(posix, "win32") is None


def test_each_arm_skips_the_other_platforms_branches():
    """The inversion: an arm names the platform that CANNOT run there."""
    env = _conditional(_tox()["testenv"]["setenv"])
    selected = {}
    for factor, entries in env.items():
        for entry in entries:
            key, _, value = entry.partition("=")
            if key.strip() == "CRONSTABLE_COVERAGE_SKIP":
                selected[factor] = value.strip()
    assert selected["windows"] == "posix"
    assert selected["posix"] == "windows"
    # The unconditional default keeps an unfactored env on the historical
    # POSIX-shaped measurement.
    assert selected[None] == "windows"


def test_the_floor_is_a_factor_conditional_env_var():
    tox = _tox()
    env = _conditional(tox["testenv"]["setenv"])
    floors = {}
    for factor, entries in env.items():
        for entry in entries:
            key, _, value = entry.partition("=")
            if key.strip() == "CRONSTABLE_COV_FLOOR":
                floors[factor] = int(value.strip())
    assert set(floors) == {None, "windows", "posix"}
    assert all(0 < floor <= 100 for floor in floors.values())
    assert "--cov-fail-under={env:CRONSTABLE_COV_FLOOR}" in (
        tox["testenv"]["commands"]
    )


def test_commands_is_unconditional():
    """An unfactored ``tox -e py312`` must still run the suite.

    A factored ``commands`` resolves to the empty string there, and tox
    reports OK for an env that ran nothing at all.
    """
    commands = _conditional(_tox()["testenv"]["commands"])
    assert set(commands) == {None}, commands
    assert len(commands[None]) == 1, commands[None]
    assert "--cov=cronstable" in commands[None][0]


def test_ci_runs_both_arms_and_names_the_right_envdir():
    """The workflow's two touchpoints, which fail silently if missed.

    The matrix step needs both arms (the non-matching one skips), and the
    web-engine differential re-drives pytest through the tox env's own
    interpreter by path, so the envdir rename has to reach it or the one
    step that compares the dashboard's cron engine against the daemon's
    stops running.
    """
    workflow = _read(
        os.path.join(ROOT, ".github", "workflows", "release.yml")
    )
    assert "tox -e py-windows,py-posix" in workflow
    assert ".tox/py/bin/python" not in workflow
    assert workflow.count(".tox/py-posix/bin/python") == 2
