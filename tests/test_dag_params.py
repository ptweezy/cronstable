"""Workflow run parameters.

Covers the feature layer by layer:
  * ``cronstable.params``: the pure model (text conversion, the value
    check, resolution against a declaration, the environment form).
  * the ``params:`` declaration at config load, with every rejection.
  * the state machine: the run engine level and ``new_run_body``.
  * ``DagScheduler``: trigger, ``requestId``, backfill, scheduled runs, and
    delivery to a real task process, against a real backend and the real
    loopback endpoint (the harness of tests/test_state_dag_run.py).
  * the HTTP routes, with their statuses and the ``params`` scope.
  * recovery: the stored map is reused under the caller's scopes, and the
    declaration is part of the configuration revision.
"""

import json
import logging
import math
import sys
import types

import pytest

from cronstable import config as config_module
from cronstable import dag, dagrun, params, recovery
from cronstable.config import (
    ConfigError,
    _validate_cross_sections,
    parse_config_string,
)
from cronstable.cron import (
    _WEB_ALL_SCOPES,
    WEB_PARAMS_SCOPE,
    WEB_TOKEN_REQUEST_KEY,
    _effective_web_scopes,
    _WebToken,
)
from tests._configs import _STATE
from tests.conftest import Req
from tests.test_state_dag_run import _drive, _set_cmd, _start_web

_PY = sys.executable

_DEPLOY = """
dags:
  - name: deploy
    params:
      - name: target
        type: string
        default: staging
        allowed:
          - staging
          - prod
        description: Environment to deploy to
      - name: batch_size
        type: integer
        default: 500
        minimum: 1
        maximum: 10000
      - name: dry
        type: boolean
        default: no
      - name: ratio
        type: number
        default: 0.5
      - name: ticket
        required: true
        pattern: "OPS-[0-9]+"
        maxLength: 32
    tasks:
      - id: release
        command: 'x'
"""

# a scheduled workflow: every parameter has a default
_NIGHTLY = """
dags:
  - name: nightly
    schedule: '0 * * * *'
    params:
      - name: mode
        default: incremental
        allowed:
          - incremental
          - full
      - name: limit
        type: integer
        default: 10
    tasks:
      - id: load
        command: 'x'
"""

_PLAIN = """
dags:
  - name: plain
    tasks:
      - id: a
        command: 'x'
"""

_DEFAULTS = {
    "target": "staging",
    "batch_size": 500,
    "dry": False,
    "ratio": 0.5,
}


def _specs(yaml=_DEPLOY, name=None):
    config = parse_config_string(_STATE + yaml, "")
    return config.dags[0].spec.params


def _spec(**over):
    return params.ParamSpec(name="p", **over)


# --------------------------------------------------------------------------
# cronstable.params: text conversion
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind,text,expected",
    [
        ("string", " 5 ", " 5 "),
        ("string", "", ""),
        ("integer", "42", 42),
        ("integer", " -7 ", -7),
        ("integer", "+3", 3),
        ("number", "5", 5),
        ("number", "0.5", 0.5),
        ("number", "-1e3", -1000.0),
        ("number", ".25", 0.25),
        ("boolean", "true", True),
        ("boolean", "Yes", True),
        ("boolean", "off", False),
        ("boolean", "0", False),
    ],
)
def test_parse_text_converts_to_the_declared_type(kind, text, expected):
    value = params.parse_text(kind, text)
    assert value == expected and type(value) is type(expected)


@pytest.mark.parametrize(
    "kind,text",
    [
        ("integer", "4.0"),
        ("integer", "1e3"),
        ("integer", "1_000"),
        ("integer", ""),
        ("number", "nan"),
        ("number", "inf"),
        ("number", "1,5"),
        ("boolean", "maybe"),
        ("boolean", ""),
    ],
)
def test_parse_text_refuses_text_of_another_type(kind, text):
    with pytest.raises(ValueError, match="must be"):
        params.parse_text(kind, text)


# --------------------------------------------------------------------------
# cronstable.params: the value check
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec,value",
    [
        (_spec(), "anything"),
        (_spec(), ""),
        (_spec(pattern="OPS-[0-9]+"), "OPS-12"),
        (_spec(max_length=3), "abc"),
        (_spec(allowed=("a", "b")), "b"),
        (_spec(type="integer"), -(2**53 - 1)),
        (_spec(type="integer", minimum=1, maximum=3), 3),
        (_spec(type="number"), 2),
        # the integer ceiling is the integer type's: a number is any double
        (_spec(type="number"), 10**16),
        (_spec(type="number"), -(2**60)),
        (_spec(type="number", minimum=0.5), 0.5),
        (_spec(type="number", allowed=(1, 2.5)), 2.5),
        (_spec(type="boolean"), False),
    ],
)
def test_check_value_accepts(spec, value):
    assert params.check_value(spec, value) is None


@pytest.mark.parametrize(
    "spec,value,problem",
    [
        (_spec(), 5, "must be a string"),
        (_spec(), None, "must be a string"),
        (_spec(), "a\nb", "must not contain control or line-separator"),
        (_spec(), "a\x7fb", "must not contain control or line-separator"),
        # C1 controls and the Unicode separators end a line too
        (_spec(), "a\x85b", "must not contain control or line-separator"),
        (_spec(), "a\x9bb", "must not contain control or line-separator"),
        (_spec(), "a\u2028b", "must not contain control or line-separator"),
        (_spec(), "a\u2029b", "must not contain control or line-separator"),
        (_spec(), "\ud800", "must be valid Unicode text"),
        (_spec(), "x" * 4097, "must be at most 4096 bytes"),
        # the limit counts bytes of UTF-8, not characters
        (_spec(), "é" * 2049, "must be at most 4096 bytes"),
        (_spec(max_length=3), "abcd", "must be at most 3 characters"),
        # a full match: a prefix or a suffix around the pattern is refused
        (_spec(pattern="OPS-[0-9]+"), "xOPS-1", "must match the pattern"),
        (_spec(pattern="OPS-[0-9]+"), "OPS-1; rm", "must match the pattern"),
        (_spec(allowed=("a", "b")), "c", "must be one of: a, b"),
        (_spec(type="integer"), "5", "must be an integer"),
        (_spec(type="integer"), 5.0, "must be an integer"),
        (_spec(type="integer"), True, "must be an integer"),
        (_spec(type="integer"), 2**53, "must be between"),
        (_spec(type="integer", minimum=1), 0, "must be at least 1"),
        (_spec(type="integer", maximum=9), 10, "must be at most 9"),
        (_spec(type="number"), "0.5", "must be a number"),
        (_spec(type="number"), True, "must be a number"),
        (_spec(type="number"), math.inf, "must be a finite number"),
        (_spec(type="number"), math.nan, "must be a finite number"),
        # an integer no double can hold
        (_spec(type="number"), 10**400, "must be a finite number"),
        (_spec(type="number", maximum=1.5), 1.6, "must be at most 1.5"),
        (_spec(type="boolean"), 1, "must be true or false"),
        (_spec(type="boolean"), "true", "must be true or false"),
    ],
)
def test_check_value_refuses(spec, value, problem):
    assert problem in params.check_value(spec, value)


# --------------------------------------------------------------------------
# cronstable.params: resolution
# --------------------------------------------------------------------------


def test_resolve_fills_defaults_in_declaration_order():
    resolved = params.resolve(
        _specs(), {"ticket": "OPS-1", "target": "prod"}, "workflow 'deploy'"
    )
    assert resolved == {**_DEFAULTS, "target": "prod", "ticket": "OPS-1"}
    assert list(resolved) == ["target", "batch_size", "dry", "ratio", "ticket"]


def test_a_number_beyond_the_integer_ceiling_is_stored_as_a_float():
    # every stored number is one a JSON double carries, whichever way the
    # caller wrote it
    specs = (params.ParamSpec(name="n", type="number", default=1),)
    resolved = params.resolve(specs, {"n": 10**16}, "workflow 'w'")
    assert resolved == {"n": 1e16} and type(resolved["n"]) is float
    small = params.resolve(specs, {"n": 7}, "workflow 'w'")
    assert type(small["n"]) is int
    # a configured default takes the same form
    (spec,) = _specs(
        _one_param(
            "      - name: n\n        type: number\n"
            "        default: 10000000000000000\n"
        )
    )
    assert spec.default == 1e16 and type(spec.default) is float
    # the integer type keeps its ceiling, at load too
    with pytest.raises(ConfigError, match="must be between"):
        _specs(
            _one_param(
                "      - name: n\n        type: integer\n"
                "        default: 10000000000000000\n"
            )
        )


def test_a_whole_number_has_one_stored_form():
    # 5 and 5.0 are one value, so a run stores it, and a task reads it, the
    # same way whichever client sent it
    specs = (params.ParamSpec(name="n", type="number", default=1),)

    def stored(sent):
        return params.resolve(specs, {"n": sent}, "workflow 'w'")["n"]

    for sent, whole in (
        (5, 5),
        (5.0, 5),
        (-0.0, 0),
        (1e3, 1000),
        (2.0**53 - 1, params.MAX_INTEGER),
    ):
        assert stored(sent) == whole and type(stored(sent)) is int, sent
    assert params.environment({"n": stored(5.0)}) == {
        "CRONSTABLE_PARAM_N": "5"
    }
    # a fraction stays a float, and so does a whole number beyond the
    # largest integer a JSON double carries exactly
    for sent in (0.5, -2.25, 2.0**53, 1e300):
        assert stored(sent) == sent and type(stored(sent)) is float, sent
    # a default and an allowed value take the same form at load
    (spec,) = _specs(
        _one_param(
            "      - name: n\n        type: number\n        default: 5.0\n"
            "        allowed:\n          - 5.0\n          - 0.5\n"
        )
    )
    assert spec.default == 5 and type(spec.default) is int
    assert [type(value) for value in spec.allowed] == [int, float]
    assert params.declaration((spec,))[0]["default"] == 5
    # the integer type takes no float, whole or not
    assert params.check_value(_spec(type="integer"), 5.0) == (
        "must be an integer"
    )


def test_resolve_reports_every_offending_name():
    with pytest.raises(params.ParamError) as raised:
        params.resolve(
            _specs(),
            {"batch_size": 20000, "colour": "red", "dry": "yes"},
            "workflow 'deploy'",
        )
    assert str(raised.value) == "invalid parameters for workflow 'deploy'"
    assert raised.value.errors == {
        "batch_size": "must be at most 10000",
        "colour": "is not a declared parameter",
        "dry": "must be true or false",
        "ticket": "is required",
    }


def test_resolve_refuses_values_for_an_empty_declaration():
    assert params.resolve((), {}, "workflow 'plain'") == {}
    with pytest.raises(params.ParamError) as raised:
        params.resolve((), {"x": 1}, "workflow 'plain'")
    assert str(raised.value) == "workflow 'plain' declares no parameters"
    assert raised.value.errors == {"x": "is not a declared parameter"}


def test_resolve_refuses_an_oversized_map():
    specs = tuple(
        params.ParamSpec(name="p{}".format(i), default="") for i in range(5)
    )
    big = {spec.name: "x" * 4000 for spec in specs}
    with pytest.raises(params.ParamError, match="larger than 16384 bytes"):
        params.resolve(specs, big, "workflow 'w'")
    # each value passes by itself, so the fault belongs to no single name
    assert all(params.check_value(s, big[s.name]) is None for s in specs)
    with pytest.raises(params.ParamError, match="at most 32 parameters"):
        params.resolve(specs, {str(i): 1 for i in range(33)}, "workflow 'w'")


def test_resolve_escapes_an_undeclared_name_it_echoes():
    # a name is caller input: the error keys stay printable ASCII and short
    with pytest.raises(params.ParamError) as raised:
        params.resolve(
            _specs(), {"a\nb": 1, "\ud800": 1, "x" * 500: 1}, "workflow 'd'"
        )
    names = set(raised.value.errors) - {"ticket"}
    assert names == {"a\\nb", "\\ud800", "x" * 64}
    json.dumps(raised.value.errors).encode("utf-8")


def test_check_stored_wants_a_complete_valid_map():
    specs = _specs()
    stored = {**_DEFAULTS, "ticket": "OPS-1"}
    assert params.check_stored(specs, stored) == {}
    assert params.check_stored((), {}) == {}
    del stored["dry"]
    stored["target"] = "qa"
    stored["old"] = 1
    assert params.check_stored(specs, stored) == {
        "dry": "has no stored value",
        "target": "must be one of: staging, prod",
        "old": "is not a declared parameter",
    }


# --------------------------------------------------------------------------
# cronstable.params: the environment form and the declaration export
# --------------------------------------------------------------------------


def test_environment_exports_each_value_as_text():
    env = params.environment(
        {"target": "prod", "batch_size": 500, "dry": False, "ratio": 0.5}
    )
    assert env == {
        "CRONSTABLE_PARAM_TARGET": "prod",
        "CRONSTABLE_PARAM_BATCH_SIZE": "500",
        "CRONSTABLE_PARAM_DRY": "false",
        "CRONSTABLE_PARAM_RATIO": "0.5",
    }
    assert params.env_text(True) == "true"
    assert params.env_text("a b; $(c)") == "a b; $(c)"


def test_environment_leaves_out_what_a_declaration_cannot_hold():
    # a document from elsewhere cannot inject a variable name or a value
    # that is not a scalar
    env = params.environment(
        {"ok": 1, "bad name": 1, "PATH=x": 1, "nested": {"a": 1}, "nil": None}
    )
    assert env == {"CRONSTABLE_PARAM_OK": "1"}


def test_declaration_is_the_shape_the_configuration_writes():
    assert params.declaration(_specs()) == [
        {
            "name": "target",
            "type": "string",
            "default": "staging",
            "allowed": ["staging", "prod"],
            "description": "Environment to deploy to",
        },
        {
            "name": "batch_size",
            "type": "integer",
            "default": 500,
            "minimum": 1,
            "maximum": 10000,
        },
        {"name": "dry", "type": "boolean", "default": False},
        {"name": "ratio", "type": "number", "default": 0.5},
        {
            "name": "ticket",
            "type": "string",
            "required": True,
            "pattern": "OPS-[0-9]+",
            "maxLength": 32,
        },
    ]


# --------------------------------------------------------------------------
# The declaration at config load
# --------------------------------------------------------------------------


def _one_param(body, schedule=""):
    return (
        "dags:\n  - name: d\n{}    params:\n{}    tasks:\n"
        "      - id: a\n        command: 'x'\n"
    ).format(schedule, body)


def test_declaration_loads_typed():
    target, batch, dry, ratio, ticket = _specs()
    assert (target.type, target.default) == ("string", "staging")
    assert target.allowed == ("staging", "prod")
    assert (batch.default, batch.minimum, batch.maximum) == (500, 1, 10000)
    assert dry.default is False
    assert ratio.default == 0.5
    assert ticket.required and ticket.default is None
    assert (ticket.type, ticket.max_length) == ("string", 32)


def test_a_string_default_keeps_text_that_reads_as_another_type():
    (spec,) = _specs(_one_param("      - name: v\n        default: '007'\n"))
    assert spec.default == "007"
    (spec,) = _specs(_one_param("      - name: v\n        default: ''\n"))
    assert spec.default == ""


@pytest.mark.parametrize(
    "body,match",
    [
        pytest.param(
            "      - name: 9lives\n        default: a\n",
            "param '9lives': a name starts with a letter",
            id="name-charset",
        ),
        pytest.param(
            "      - name: {}\n        default: a\n".format("n" * 65),
            "holds at most 64 letters",
            id="name-length",
        ),
        pytest.param(
            "      - name: Target\n        default: a\n"
            "      - name: target\n        default: b\n",
            "param 'target': the name repeats 'Target'",
            id="duplicate-ignoring-case",
        ),
        pytest.param(
            "      - name: db_password\n        default: a\n",
            "param 'db_password': the name reads as a secret",
            id="secret-like-name",
        ),
        pytest.param(
            "      - name: API_KEY\n        default: a\n",
            "the name reads as a secret",
            id="secret-like-name-any-case",
        ),
        pytest.param(
            "      - name: v\n",
            "param 'v': needs a default, or `required: true`",
            id="no-default",
        ),
        pytest.param(
            "      - name: v\n        required: true\n        default: a\n",
            "a required parameter takes no default",
            id="required-with-default",
        ),
        pytest.param(
            "      - name: v\n        default: qa\n"
            "        allowed:\n          - staging\n          - prod\n",
            "default 'qa' must be one of: staging, prod",
            id="default-outside-allowed",
        ),
        pytest.param(
            "      - name: v\n        type: integer\n        default: lots\n",
            "default 'lots' must be an integer",
            id="default-wrong-type",
        ),
        pytest.param(
            "      - name: v\n        type: integer\n        default: 11\n"
            "        maximum: 10\n",
            "default '11' must be at most 10",
            id="default-out-of-range",
        ),
        pytest.param(
            "      - name: v\n        default: abcd\n        maxLength: 3\n",
            "default 'abcd' must be at most 3 characters",
            id="default-too-long",
        ),
        pytest.param(
            "      - name: v\n        type: integer\n        default: 1\n"
            "        allowed:\n          - 1\n          - two\n",
            "allowed value 'two' must be an integer",
            id="allowed-wrong-type",
        ),
        pytest.param(
            "      - name: v\n        default: a\n        minimum: 1\n",
            "minimum does not apply to the type string",
            id="minimum-on-string",
        ),
        pytest.param(
            "      - name: v\n        type: integer\n        default: 1\n"
            "        pattern: '[0-9]'\n",
            "pattern does not apply to the type integer",
            id="pattern-on-integer",
        ),
        pytest.param(
            "      - name: v\n        type: boolean\n        default: true\n"
            "        allowed:\n          - true\n",
            "allowed does not apply to the type boolean",
            id="allowed-on-boolean",
        ),
        pytest.param(
            "      - name: v\n        type: integer\n        default: 1\n"
            "        minimum: 0.5\n",
            "minimum must be an integer",
            id="fractional-bound-on-integer",
        ),
        pytest.param(
            "      - name: v\n        type: number\n        default: 1\n"
            "        minimum: 5\n        maximum: 2\n",
            "minimum is greater than maximum",
            id="inverted-range",
        ),
        pytest.param(
            "      - name: v\n        default: a\n        pattern: '('\n",
            "pattern is not a valid regular expression",
            id="bad-pattern",
        ),
        pytest.param(
            "      - name: v\n        default: a\n"
            "        pattern: 'a{1,99999999999}'\n",
            "pattern is not a valid regular expression: the repetition "
            "number is too large",
            id="pattern-repeat-count-too-large",
        ),
        pytest.param(
            "      - name: v\n        default: a\n"
            "        pattern: '{}a{}'\n".format("(" * 1000, ")" * 1000),
            "pattern is not a valid regular expression: maximum recursion",
            id="pattern-nested-too-deep",
        ),
        pytest.param(
            "      - name: v\n        default: a\n        maxLength: 0\n",
            "maxLength must be from 1 to 4096",
            id="max-length-floor",
        ),
        pytest.param(
            "".join(
                "      - name: p{}\n        default: a\n".format(i)
                for i in range(33)
            ),
            "declares 33 params; the limit is 32",
            id="too-many",
        ),
        pytest.param(
            "".join(
                "      - name: p{}\n        default: {}\n".format(
                    i, "x" * 4000
                )
                for i in range(5)
            ),
            "the param defaults are larger than 16384 bytes",
            id="defaults-too-large",
        ),
    ],
)
def test_declaration_rejections(body, match):
    with pytest.raises(ConfigError, match=match):
        _specs(_one_param(body))


def test_a_secret_word_earlier_in_a_name_loads():
    # the name the secret-name error itself suggests, and a common counter
    for name in ("secret_name", "max_tokens"):
        (spec,) = _specs(
            _one_param("      - name: {}\n        default: a\n".format(name))
        )
        assert spec.name == name


def test_a_scheduled_workflow_cannot_require_a_parameter():
    body = "      - name: v\n        required: true\n"
    # a manual-only workflow can
    (spec,) = _specs(_one_param(body))
    assert spec.required
    with pytest.raises(
        ConfigError, match="a workflow with a schedule cannot require"
    ):
        _specs(_one_param(body, schedule="    schedule: '0 * * * *'\n"))


def test_params_is_not_a_task_or_a_defaults_key():
    # strictyaml refuses the key anywhere but on a workflow
    with pytest.raises(ConfigError):
        parse_config_string(
            _STATE + "defaults:\n  params:\n    - name: v\n      default: a\n",
            "",
        )


@pytest.mark.parametrize(
    "pattern,nests",
    [
        (r"(a+)+", True),
        (r"(a*)*", True),
        (r"([a-z0-9]+-?)+", True),
        (r"(\w+\s*)*", True),
        (r"(?:a+)+", True),
        (r"(a+?)+?", True),
        (r"(a+){2,}", True),
        # the inner repeat below an alternative or an optional group
        (r"(a+|b)+", True),
        (r"((a+)?)+", True),
        # an optional possessive repeat beside the inner repeat
        (r"(a+b*+)+", True),
        # inside a lookahead or a conditional
        (r"(?=(a+)+b)a*", True),
        (r"(a)(?(1)(b+)+c|d)", True),
        # the inner repeat has an upper bound and its count still varies
        (r"([0-9]{1,3}\.?)+", True),
        (r"([a-z0-9-]{1,63}\.?)+", True),
        (r"(a{2,3})+", True),
        (r"(a{0,2})*", True),
        (r"((ab){1,3})+", True),
        # a required item separates the repeats
        (r"[a-z0-9]+(-[a-z0-9]+)*", False),
        (r"OPS-[0-9]+", False),
        (r"[0-9.]+(-rc[0-9]+)?", False),
        (r"(\w+\s+)*", False),
        (r"[a-z0-9]{1,63}(\.[a-z0-9]{1,63})*", False),
        (r"(\d{1,3}\.){3}\d{1,3}", False),
        # an anchor counts as required
        (r"(\w+\b\s*)*", False),
        # the inner repeat runs once at most, or a fixed number of times
        (r"(a?)*", False),
        (r"(a{2})+", False),
        # varying counts with no unbounded repeat around them
        (r"[A-Z]{2,5}-[0-9]{1,6}", False),
        (r"(ab){1,3}c", False),
        # an outer repeat with an upper bound is outside the check
        (r"(a+){1,3}", False),
        # possessive repeats and atomic groups
        (r"(a++)+", False),
        (r"(a+)++", False),
        (r"(?:[a-z0-9]++-?)++", False),
        (r"(?>a+)+", False),
        (r"(?>(a+)+)", False),
        # alternatives that overlap are outside the check
        (r"(a|aa)+", False),
    ],
)
def test_nested_repetition(pattern, nests):
    assert params.nested_repetition(pattern) is nests


@pytest.mark.parametrize(
    "pattern,short",
    [
        # the two repeats are the only choices
        (r"(a+)+", False),
        (r"([a-z0-9]+-?)+", False),
        (r"([0-9]{1,3}\.?)+", False),
        (r"((a+)?)+", False),
        (r"(a+b*+)+", False),
        (r"(a)(?(1)(b+)+c|d)", False),
        # a third repeat
        (r"((a+)+)+", True),
        (r"(\w*\d*)*", True),
        (r"(?=((a+)+)+b)a*", True),
        # an alternative
        (r"(a+|a)+", True),
        (r"(\w+|\d+)+", True),
    ],
)
def test_nested_repetition_under_a_short_max_length(pattern, short):
    assert params.SHORT_MAX_LENGTH == 16
    assert params.nested_repetition(pattern, 16) is short
    for longer in (17, None):
        assert params.nested_repetition(pattern, longer) is True


_SLUG = (
    "      - name: slug\n        default: a\n"
    "        pattern: '([a-z0-9]+-?)+'\n"
)


def _nested_repeat_warnings(caplog, yaml):
    """The config ``yaml`` loads to, and each nested-repeat warning."""
    with caplog.at_level(logging.WARNING, logger="cronstable.config"):
        config = parse_config_string(_STATE + yaml, "")
        _validate_cross_sections(config)
    messages = [record.getMessage() for record in caplog.records]
    return config, [m for m in messages if "unbounded repeat" in m]


def test_a_nested_repeat_pattern_warns_at_load_and_still_loads(caplog):
    # the second parameter separates the same repeats with a required `-`
    body = _SLUG + (
        "      - name: tag\n        default: a\n"
        "        pattern: '[a-z0-9]+(-[a-z0-9]+)*'\n"
    )
    job = (
        "jobs:\n  - name: publish\n    command: 'x'\n"
        "    schedule: '0 0 1 1 *'\n    params:\n"
    )
    config, warned = _nested_repeat_warnings(
        caplog, _one_param(body) + job + body
    )
    assert warned == [
        "{}: param 'slug': the pattern holds a repeat inside an unbounded "
        "repeat with nothing required between them, as (a+)+ does. "
        "Checking a value that does not match can take time that grows "
        "exponentially with the length of the value, and the daemon "
        "schedules nothing until the check ends. Require a character "
        "between the repeats, or make the inner repeat possessive, as in "
        "(a++)+".format(owner)
        for owner in ("job 'publish'", "dag 'd'")
    ]
    for declared in (config.jobs[0].params, config.dags[0].spec.params):
        slug = declared[0]
        assert slug.pattern == "([a-z0-9]+-?)+"
        assert params.check_value(slug, "release-2") is None


def _warned_params(caplog, body):
    """The name of each parameter of ``body`` that draws the warning."""
    _config, warned = _nested_repeat_warnings(caplog, _one_param(body))
    return [message.split("'")[3] for message in warned]


def test_a_short_max_length_silences_the_nested_repeat_warning(caplog):
    # two repeats and no other choice: quiet up to 16 characters
    body = "".join(
        "      - name: {}\n        default: a\n"
        "        pattern: '([a-z0-9]+-?)+'\n{}".format(name, limit)
        for name, limit in (
            ("at16", "        maxLength: 16\n"),
            ("at17", "        maxLength: 17\n"),
            ("unlimited", ""),
        )
    )
    assert _warned_params(caplog, body) == ["at17", "unlimited"]


def test_a_short_max_length_keeps_the_warning_for_a_further_choice(caplog):
    # a third repeat or an alternative makes a 16-character check slow
    body = "".join(
        "      - name: {}\n        default: a\n        maxLength: 16\n"
        "        pattern: '{}'\n".format(name, pattern)
        for name, pattern in (
            ("deeper", "((a+)+)+"),
            ("either", "(a+|a+)+"),
            ("plain", "(a+)+"),
        )
    )
    assert _warned_params(caplog, body) == ["deeper", "either"]


def _parse_fails(pattern):
    raise RecursionError


@pytest.mark.parametrize(
    "parser",
    [
        None,
        types.SimpleNamespace(parse=_parse_fails),
        types.SimpleNamespace(parse=lambda pattern: [("repeat",)]),
    ],
    ids=["missing", "raises", "another-tree-shape"],
)
def test_a_parser_failure_loses_only_the_warning(caplog, monkeypatch, parser):
    monkeypatch.setattr(params, "_PARSER", parser)
    assert params.nested_repetition("(a+)+") is False
    config, warned = _nested_repeat_warnings(caplog, _one_param(_SLUG))
    assert warned == []
    (slug,) = config.dags[0].spec.params
    assert slug.pattern == "([a-z0-9]+-?)+"
    assert params.check_value(slug, "release-2") is None
    assert params.check_value(slug, "Release") is not None


# One job and one workflow whose commands read parameters. The job names
# the Windows default shell, so it loads the same on every host.
_READERS = """
jobs:
  - name: report
    command: {job_command}
    shell: ''
    schedule: '0 0 1 1 *'
    params:
      - name: region
        default: eu
dags:
  - name: deploy
    params:
      - name: target
        default: staging
      - name: env
        default: staging
        allowed:
          - staging
          - prod
      - name: ticket
        default: OPS-1
        pattern: 'OPS-[0-9]+'
      - name: batch_size
        type: integer
        default: 5
    tasks:
      - id: release
{task}
      - id: gate
        type: approval
"""


def _load_warnings(caplog, yaml, check, **kwargs):
    """Each warning ``check`` logs for the config ``yaml`` loads to."""
    config = parse_config_string(_STATE + yaml, "")
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="cronstable.config"):
        check(config, **kwargs)
    return [record.getMessage() for record in caplog.records]


def _readers(job_command="'x'", task="        command: 'x'"):
    return _READERS.format(job_command=job_command, task=task)


def test_a_reference_to_an_undeclared_parameter_warns_at_load(caplog):
    yaml = _readers(
        # one typo, one declared name, and a prefix that names no variable
        job_command=(
            "'run $CRONSTABLE_PARAM_REGOIN ${CRONSTABLE_PARAM_REGION} "
            "CRONSTABLE_PARAM_*'"
        ),
        task=(
            "        command:\n          - ./release.sh\n"
            "          - '$CRONSTABLE_PARAM_TARGET'\n"
            "          - '$CRONSTABLE_PARAM_TRAGET'\n"
            "          - '$CRONSTABLE_PARAM_TRAGET'"
        ),
    )
    assert _load_warnings(
        caplog, yaml, config_module._warn_if_param_references_misfire
    ) == [
        "job 'report': the command reads CRONSTABLE_PARAM_REGOIN, and the "
        "job declares no parameter of that name, so the variable is unset "
        "when the command runs",
        "dag 'deploy': task 'release': the command reads "
        "CRONSTABLE_PARAM_TRAGET, and the workflow declares no parameter "
        "of that name, so the variable is unset when the command runs",
    ]
    # the whole load logs the same lines, and still loads
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="cronstable.config"):
        _validate_cross_sections(parse_config_string(_STATE + yaml, ""))
    assert sum("is unset when" in r.getMessage() for r in caplog.records) == 2


def test_a_job_with_no_declaration_warns_for_any_reference(caplog):
    yaml = (
        "jobs:\n  - name: plain\n    schedule: '0 0 1 1 *'\n"
        "    command: 'echo $CRONSTABLE_PARAM_REGION'\n"
    )
    (warned,) = _load_warnings(
        caplog, yaml, config_module._warn_if_param_references_misfire
    )
    assert warned.startswith(
        "job 'plain': the command reads CRONSTABLE_PARAM_REGION"
    )


_CMD_WARNING = (
    "{}: cmd.exe replaces %CRONSTABLE_PARAM_{}% with the value before it "
    "parses the command, and param {!r} takes any string. A value that "
    "holds &, |, <, >, or a double quote runs as part of the command line. "
    "Give the parameter an allowed list or a pattern, or read the variable "
    "from PowerShell or a script file"
)


def test_cmd_expansion_of_an_unconstrained_string_warns_at_load(caplog):
    # the reference names the variable in any case, as cmd.exe reads it
    task = (
        "        shell: ''\n"
        "        command: 'deploy %CRONSTABLE_PARAM_TARGET% "
        "%cronstable_param_target% %CRONSTABLE_PARAM_ENV% "
        "%CRONSTABLE_PARAM_TICKET% %CRONSTABLE_PARAM_BATCH_SIZE%'"
    )
    yaml = _readers(
        job_command="'report %CRONSTABLE_PARAM_REGION%'", task=task
    )
    check = config_module._warn_if_param_references_misfire
    # each string with no allowed list and no pattern draws it
    assert _load_warnings(caplog, yaml, check, windows=True) == [
        _CMD_WARNING.format("job 'report'", "REGION", "region"),
        _TARGET_WARNING,
    ]
    # off Windows a string command runs under another shell
    assert _load_warnings(caplog, yaml, check, windows=False) == []


_TARGET_WARNING = _CMD_WARNING.format(
    "dag 'deploy': task 'release'", "TARGET", "target"
)
_PERCENT = "        command: 'd %CRONSTABLE_PARAM_TARGET%'"


@pytest.mark.parametrize(
    "task, windows, warns",
    [
        ("        shell: cmd\n" + _PERCENT, True, True),
        (
            "        shell: 'C:\\Windows\\System32\\cmd.exe'\n" + _PERCENT,
            True,
            True,
        ),
        ("        shell: powershell\n" + _PERCENT, True, False),
        ("        shell: cmd\n" + _PERCENT, False, False),
        # an argument list that starts with cmd.exe reaches it on any host
        (
            "        command:\n          - cmd.exe\n          - /c\n"
            "          - 'd %CRONSTABLE_PARAM_TARGET%'",
            False,
            True,
        ),
        (
            "        command:\n          - deploy.exe\n"
            "          - '%CRONSTABLE_PARAM_TARGET%'",
            True,
            False,
        ),
        # delayed expansion does not parse the value again
        (
            "        shell: ''\n"
            "        command: 'd !CRONSTABLE_PARAM_TARGET!'",
            True,
            False,
        ),
    ],
    ids=[
        "shell-cmd",
        "cmd-path",
        "powershell",
        "cmd-off-windows",
        "argv-cmd",
        "argv-program",
        "delayed-expansion",
    ],
)
def test_cmd_expansion_warning_follows_the_shell(
    caplog, task, windows, warns
):
    warned = _load_warnings(
        caplog,
        _readers(task=task),
        config_module._warn_if_param_references_misfire,
        windows=windows,
    )
    assert warned == ([_TARGET_WARNING] if warns else [])


def test_skip_exit_codes_that_failures_return_warn_at_load(caplog):
    yaml = (
        "dags:\n  - name: etl\n    tasks:\n"
        "      - id: load\n        command: 'x'\n"
        "        skipExitCodes:\n          - 127\n          - 99\n"
        "          - 1\n"
        "      - id: clean\n        command: 'x'\n"
        "        skipExitCodes:\n          - 99\n          - 3\n"
        "      - id: plain\n        command: 'x'\n"
    )
    assert _load_warnings(
        caplog, yaml, config_module._warn_if_skip_exit_codes_hide_failures
    ) == [
        "dag 'etl': task 'load': skipExitCodes holds 1, 127. A shell or a "
        "failing program exits with 1 or 2 for an ordinary error, 126 for "
        "a file it cannot run, and 127 for a command it cannot find, so a "
        "broken command would end the task skipped. Use a code that only "
        "the command's own skip decision returns, such as 99"
    ]
    # the load logs it, and the task keeps the codes it lists
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="cronstable.config"):
        config = parse_config_string(_STATE + yaml, "")
        _validate_cross_sections(config)
    logged = [record.getMessage() for record in caplog.records]
    assert sum("skipExitCodes holds" in message for message in logged) == 1
    assert config.dags[0].tasks[0].spec.skip_exit_codes == (1, 99, 127)


# --------------------------------------------------------------------------
# The state machine: engine level and the run document
# --------------------------------------------------------------------------


def test_a_declaration_needs_engine_level_two():
    assert dag.BRANCHING_PARAMS_ENGINE_LEVEL == 2 <= dag.ENGINE_LEVEL
    tasks = [dag.TaskSpec(id="a")]
    declared = (params.ParamSpec(name="v", default="a"),)
    assert dag.DagSpec.build("d", tasks).engine == dag.BASE_ENGINE_LEVEL
    assert dag.DagSpec.build("d", tasks).params == ()
    spec = dag.DagSpec.build("d", tasks, declared)
    assert spec.engine == dag.BRANCHING_PARAMS_ENGINE_LEVEL
    assert spec.params == declared
    # a declaration and a branching key need the same level
    branching = [dag.TaskSpec(id="a", skip_exit_codes=(99,))]
    assert (
        dag.DagSpec.build("d", branching, declared).engine
        == dag.DagSpec.build("d", branching).engine
        == dag.BRANCHING_PARAMS_ENGINE_LEVEL
    )


def test_new_run_body_stores_params_only_when_given():
    def body(spec, **over):
        return dag.new_run_body(
            dag="d",
            run_key="k",
            run_id="r",
            logical_date=None,
            kind="manual",
            now=1.0,
            spec=spec,
            **over,
        )

    plain = dag.DagSpec.build("d", [dag.TaskSpec(id="a")])
    assert "params" not in body(plain)
    assert "engine" not in body(plain)
    declared = dag.DagSpec.build(
        "d", [dag.TaskSpec(id="a")], (params.ParamSpec(name="v", default="a"),)
    )
    stored = body(declared, params={"v": "b"})
    assert stored["params"] == {"v": "b"}
    assert stored["engine"] == dag.BRANCHING_PARAMS_ENGINE_LEVEL
    # this build advances the run and leaves one above its level alone
    assert dag.supports_run(stored)
    assert not dag.supports_run({**stored, "engine": dag.ENGINE_LEVEL + 1})


# --------------------------------------------------------------------------
# DagScheduler: trigger
# --------------------------------------------------------------------------


async def test_trigger_stores_the_resolved_map(dag_cron, caplog):
    cron = await dag_cron(_DEPLOY)
    with caplog.at_level(logging.INFO, logger="cronstable.dagrun"):
        result = await cron._dag.trigger(
            "deploy",
            params={"ticket": "OPS-4412", "target": "prod"},
            triggered_by="ops-laptop",
        )
    expected = {**_DEFAULTS, "target": "prod", "ticket": "OPS-4412"}
    assert result["created"] is True
    assert result["params"] == expected
    body = await cron._dag.get_run("deploy", result["runKey"])
    assert body["params"] == expected
    assert body["triggeredBy"] == "ops-laptop"
    assert body["engine"] == dag.BRANCHING_PARAMS_ENGINE_LEVEL
    assert body["kind"] == "manual"
    # the log names the supplied parameters and never a value
    line = next(
        r.getMessage() for r in caplog.records if "parameters" in r.msg
    )
    assert "target, ticket" in line and "ops-laptop" in line
    assert "prod" not in line and "OPS-4412" not in line


async def test_trigger_refuses_values_and_creates_no_run(dag_cron):
    cron = await dag_cron(_DEPLOY)
    with pytest.raises(params.ParamError) as raised:
        await cron._dag.trigger("deploy", params={"batch_size": 0})
    assert raised.value.errors == {
        "batch_size": "must be at least 1",
        "ticket": "is required",
    }
    # the required parameter has no default, so a bare trigger is refused too
    with pytest.raises(params.ParamError, match="invalid parameters"):
        await cron._dag.trigger("deploy")
    assert await cron._dag.list_runs("deploy") == []
    assert await cron._dag.trigger("ghost", params={"x": 1}) is None


async def test_trigger_of_a_workflow_without_a_declaration(dag_cron):
    cron = await dag_cron(_PLAIN)
    result = await cron._dag.trigger("plain", triggered_by="ci")
    assert set(result) == {"runKey", "created"}
    body = await cron._dag.get_run("plain", result["runKey"])
    assert "params" not in body and "engine" not in body
    assert body["triggeredBy"] == "ci"
    # with nobody to name, the document carries no triggeredBy
    bare = await cron._dag.trigger("plain", params={})
    doc = await cron._dag.get_run("plain", bare["runKey"])
    assert "triggeredBy" not in doc
    with pytest.raises(params.ParamError, match="declares no parameters"):
        await cron._dag.trigger("plain", params={"x": 1})


async def test_trigger_logical_date_is_stored_in_utc(dag_cron):
    cron = await dag_cron(_PLAIN)
    result = await cron._dag.trigger(
        "plain", logical_date="2026-10-01T09:00:00-05:00"
    )
    body = await cron._dag.get_run("plain", result["runKey"])
    assert body["logicalDate"] == "2026-10-01T14:00:00+00:00"
    assert result["runKey"].startswith("manual-")
    with pytest.raises(dagrun.TriggerInputError, match="ISO 8601"):
        await cron._dag.trigger("plain", logical_date="yesterday")
    # an instant at the edge of the calendar has no UTC form
    for edge in ("0001-01-01T00:00:00+00:01", "9999-12-31T23:59:59-05:00"):
        with pytest.raises(
            dagrun.TriggerInputError, match="outside the supported"
        ):
            await cron._dag.trigger("plain", logical_date=edge)
    # every RFC 3339 form reads the same on every Python version
    for text, stored in (
        ("2026-10-01T00:00:00Z", "2026-10-01T00:00:00+00:00"),
        ("2026-10-01T00:00:00.5Z", "2026-10-01T00:00:00.500000+00:00"),
        (
            "2026-10-01T00:00:00.123456789Z",
            "2026-10-01T00:00:00.123456+00:00",
        ),
        ("2026-10-01T00:00:00+0000", "2026-10-01T00:00:00+00:00"),
    ):
        request = {"logical_date": text, "request_id": text}
        made = await cron._dag.trigger("plain", **request)
        body = await cron._dag.get_run("plain", made["runKey"])
        assert body["logicalDate"] == stored, text
        # a retry that carries the same text names the same run
        again = await cron._dag.trigger("plain", **request)
        assert again == {"runKey": made["runKey"], "created": False}, text
    assert dagrun._parse_iso("2026-10-01T00:00:00z") == dagrun._parse_iso(
        "2026-10-01T00:00:00+00:00"
    )
    # the trigger refuses a form that only a newer datetime.fromisoformat
    # reads
    with pytest.raises(dagrun.TriggerInputError, match="ISO 8601"):
        await cron._dag.trigger("plain", logical_date="20261001T000000Z")


async def test_trigger_checks_the_arguments_a_request_carried(dag_cron):
    cron = await dag_cron(_PLAIN)
    for bad in (5, ["2026-10-01"]):
        with pytest.raises(dagrun.TriggerInputError, match="ISO 8601"):
            await cron._dag.trigger("plain", logical_date=bad)
    for bad in ("", "x" * 201, 7):
        with pytest.raises(
            dagrun.TriggerInputError, match="1 to 200 characters"
        ):
            await cron._dag.trigger("plain", request_id=bad)
    assert await cron._dag.list_runs("plain") == []
    assert (await cron._dag.trigger("plain", request_id="x" * 200))["created"]


async def test_trigger_request_id_returns_the_first_run(dag_cron):
    cron = await dag_cron(_DEPLOY)
    request = {"params": {"ticket": "OPS-1"}, "request_id": "retry-me"}
    first = await cron._dag.trigger("deploy", **request)
    again = await cron._dag.trigger("deploy", **request)
    assert first["created"] is True and again["created"] is False
    assert again["runKey"] == first["runKey"]
    assert again["params"] == first["params"]
    assert len(await cron._dag.list_runs("deploy")) == 1
    # the key is derived from the id, and another id is another run
    assert len(first["runKey"]) == len("manual-") + 32
    other = await cron._dag.trigger(
        "deploy", params={"ticket": "OPS-1"}, request_id="another"
    )
    assert other["created"] is True and other["runKey"] != first["runKey"]
    # the same id with other values, or another date, is a conflict
    with pytest.raises(dagrun.TriggerConflict, match="'retry-me'"):
        await cron._dag.trigger(
            "deploy", params={"ticket": "OPS-2"}, request_id="retry-me"
        )
    with pytest.raises(dagrun.TriggerConflict):
        await cron._dag.trigger(
            "deploy",
            params={"ticket": "OPS-1"},
            request_id="retry-me",
            logical_date="2026-10-01T00:00:00+00:00",
        )
    # a JSON string can hold a lone surrogate
    odd = await cron._dag.trigger(
        "deploy", params={"ticket": "OPS-1"}, request_id="\ud800"
    )
    assert odd["created"] is True


async def test_trigger_without_a_backend_raises(dag_cron):
    cron = await dag_cron(_DEPLOY)
    cron.state_backend = None
    with pytest.raises(RuntimeError, match="could not be recorded"):
        await cron._dag.trigger("deploy", params={"ticket": "OPS-1"})
    with pytest.raises(RuntimeError, match="could not be recorded"):
        await cron._dag.trigger(
            "deploy", params={"ticket": "OPS-1"}, request_id="r"
        )


# --------------------------------------------------------------------------
# DagScheduler: scheduled runs and backfill
# --------------------------------------------------------------------------


async def test_a_run_created_without_values_stores_the_defaults(dag_cron):
    # the create transform every scheduled and catch-up run goes through
    cron = await dag_cron(_NIGHTLY)
    dagcfg = cron.cron_dags["nightly"]
    assert await cron._dag._create_doc(dagcfg, "k", None, "scheduled")
    body = await cron._dag.get_run("nightly", "k")
    assert body["params"] == {"mode": "incremental", "limit": 10}
    assert "triggeredBy" not in body


async def test_backfill_applies_params_to_the_runs_it_creates(dag_cron):
    cron = await dag_cron(_NIGHTLY)
    window = ("2026-01-01T00:00:00+00:00", "2026-01-01T02:30:00+00:00")
    first = await cron._dag.backfill(
        "nightly", window[0], "2026-01-01T00:30:00+00:00"
    )
    assert first["created"] == 1
    res = await cron._dag.backfill(
        "nightly", *window, params={"mode": "full"}, triggered_by="ops"
    )
    assert (res["created"], res["existing"]) == (2, 1)
    # the run that existed keeps its values, and the response names it
    assert res["existingRunKeys"] == first["runKeys"]
    kept = await cron._dag.get_run("nightly", first["runKeys"][0])
    assert kept["params"] == {"mode": "incremental", "limit": 10}
    assert "triggeredBy" not in kept
    for key in res["runKeys"]:
        body = await cron._dag.get_run("nightly", key)
        assert body["params"] == {"mode": "full", "limit": 10}
        assert body["triggeredBy"] == "ops"
        assert body["kind"] == "backfill"


async def test_backfill_checks_params_before_it_creates_anything(dag_cron):
    cron = await dag_cron(_NIGHTLY)
    with pytest.raises(params.ParamError) as raised:
        await cron._dag.backfill(
            "nightly",
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T02:30:00+00:00",
            params={"mode": "sideways"},
        )
    assert raised.value.errors == {
        "mode": "must be one of: incremental, full"
    }
    assert await cron._dag.list_runs("nightly") == []


async def test_list_dags_exports_the_declaration(dag_cron):
    cron = await dag_cron(_DEPLOY + _PLAIN.replace("dags:\n", ""))
    by_name = {entry["name"]: entry for entry in await cron._dag.list_dags()}
    assert by_name["deploy"]["params"] == params.declaration(
        cron.cron_dags["deploy"].spec.params
    )
    assert "params" not in by_name["plain"]


# --------------------------------------------------------------------------
# Delivery to the task
# --------------------------------------------------------------------------

_INTENT = dag.LaunchIntent(
    task_id="release",
    taskkey="release",
    map_index=None,
    map_item=None,
    attempt=0,
    is_sensor=False,
    poke_number=0,
)


async def test_prepare_task_run_exports_the_stored_map(dag_cron):
    cron = await dag_cron(_DEPLOY)
    dagcfg = cron.cron_dags["deploy"]
    template = dagcfg.task_templates["release"]
    stored = {**_DEFAULTS, "ticket": "OPS-1", "dry": True}
    token, env = await cron._dag._prepare_task_run(
        dagcfg, "rid", "manual-1", _INTENT, template, stored
    )
    assert env["CRONSTABLE_PARAM_TARGET"] == "staging"
    assert env["CRONSTABLE_PARAM_BATCH_SIZE"] == "500"
    assert env["CRONSTABLE_PARAM_DRY"] == "true"
    assert env["CRONSTABLE_PARAM_RATIO"] == "0.5"
    assert env["CRONSTABLE_PARAM_TICKET"] == "OPS-1"
    assert cron._job_api._runs[token].params == stored
    await cron._job_api.finish_run(token)
    # a run with no stored map: no variable, and an empty map to serve
    token, env = await cron._dag._prepare_task_run(
        dagcfg, "rid", "manual-1", _INTENT, template
    )
    assert not [name for name in env if name.startswith(params.ENV_PREFIX)]
    assert cron._job_api._runs[token].params == {}
    await cron._job_api.finish_run(token)
    # with the loopback API down the variables still reach the task
    cron._job_api, api = None, cron._job_api
    token, env = await cron._dag._prepare_task_run(
        dagcfg, "rid", "manual-1", _INTENT, template, stored
    )
    cron._job_api = api
    assert token is None
    assert env["CRONSTABLE_PARAM_TICKET"] == "OPS-1"


async def test_a_task_reads_its_parameters(tmp_path, dag_cron, monkeypatch):
    # end to end with a real process: the variables, and the same values
    # through `cronstable param` on the loopback endpoint
    cron = await dag_cron(_DEPLOY)
    # a parameter variable in the daemon's own environment is not one of
    # this run's, so the task never sees it
    monkeypatch.setenv("CRONSTABLE_PARAM_STRAY", "from-the-daemon")
    out = tmp_path / "seen.json"
    script = (
        "import json, os, subprocess, sys\n"
        "def cli(*a):\n"
        "    return subprocess.run(\n"
        "        [sys.executable, '-m', 'cronstable', 'param', *a],\n"
        "        capture_output=True, text=True)\n"
        "seen = {\n"
        "    'env': {k: v for k, v in os.environ.items()\n"
        "            if k.startswith('CRONSTABLE_PARAM_')},\n"
        "    'get': cli('get', 'target').stdout,\n"
        "    'getbool': cli('get', 'dry').stdout,\n"
        "    'missing': cli('get', 'nope').returncode,\n"
        "    'list': cli('list').stdout,\n"
        "    'dump': json.loads(cli('dump').stdout),\n"
        "}\n"
        "open(" + repr(str(out)) + ", 'w').write(json.dumps(seen))\n"
    )
    # the task's own environment loses to the run's parameter
    _set_cmd(
        cron,
        "deploy",
        "release",
        [_PY, "-c", script],
        env={"CRONSTABLE_PARAM_TARGET": "from-task-environment"},
    )
    result = await cron._dag.trigger(
        "deploy", params={"ticket": "OPS-7", "target": "prod", "dry": True}
    )
    body = await _drive(cron, "deploy", result["runKey"])
    assert body["state"] == dag.SUCCESS, body["tasks"]["release"]
    seen = json.loads(out.read_text())
    assert seen["env"] == {
        "CRONSTABLE_PARAM_TARGET": "prod",
        "CRONSTABLE_PARAM_BATCH_SIZE": "500",
        "CRONSTABLE_PARAM_DRY": "true",
        "CRONSTABLE_PARAM_RATIO": "0.5",
        "CRONSTABLE_PARAM_TICKET": "OPS-7",
    }
    assert seen["get"] == "prod\n"
    assert seen["getbool"] == "true\n"
    assert seen["missing"] == 4
    assert seen["list"].split() == [
        "batch_size",
        "dry",
        "ratio",
        "target",
        "ticket",
    ]
    assert seen["dump"] == result["params"]


async def test_a_run_keeps_its_values_across_a_reload(dag_cron):
    # the declaration changes after the run exists: tasks still receive
    # what the run stored
    cron = await dag_cron(_NIGHTLY)
    result = await cron._dag.trigger("nightly", params={"mode": "full"})
    reloaded = parse_config_string(
        _STATE + _NIGHTLY.replace("default: incremental", "default: full"),
        "",
    )
    cron.cron_dags = {d.name: d for d in reloaded.dags}
    body = await cron._dag.get_run("nightly", result["runKey"])
    assert body["params"] == {"mode": "full", "limit": 10}
    later = await cron._dag.trigger("nightly")
    assert later["params"] == {"mode": "full", "limit": 10}


# --------------------------------------------------------------------------
# HTTP: the trigger and backfill routes
# --------------------------------------------------------------------------


async def test_http_trigger_with_params(dag_cron):
    import aiohttp

    cron = await dag_cron(_DEPLOY + _PLAIN.replace("dags:\n", ""))
    base = await _start_web(cron)
    url = base + "/dags/deploy/trigger"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(base + "/dags") as r:
                declared = {d["name"]: d for d in await r.json()}
                assert [p["name"] for p in declared["deploy"]["params"]] == [
                    "target",
                    "batch_size",
                    "dry",
                    "ratio",
                    "ticket",
                ]
            body = {
                "params": {"target": "prod", "ticket": "OPS-4412"},
                "logicalDate": "2026-10-01T00:00:00+00:00",
                "requestId": "0b6c2f0e",
            }
            async with s.post(url, json=body) as r:
                assert r.status == 200
                first = await r.json()
            assert first["created"] is True
            assert first["dag"] == first["name"] == "deploy"
            assert first["params"] == {
                **_DEFAULTS,
                "target": "prod",
                "ticket": "OPS-4412",
            }
            # a retried request returns the first run
            async with s.post(url, json=body) as r:
                assert r.status == 200
                again = await r.json()
            assert again["created"] is False
            assert again["runKey"] == first["runKey"]
            # the same requestId with other values
            body["params"]["target"] = "staging"
            async with s.post(url, json=body) as r:
                assert r.status == 409
                assert "requestId" in (await r.json())["error"]
            # the run document carries the values
            async with s.get(
                base + "/dags/deploy/runs/" + first["runKey"]
            ) as r:
                doc = await r.json()
            assert doc["params"] == first["params"]
            assert doc["logicalDate"] == "2026-10-01T00:00:00+00:00"
            assert doc["engine"] == dag.BRANCHING_PARAMS_ENGINE_LEVEL
            # no token is configured, so nobody is named
            assert "triggeredBy" not in doc
            # refused values: the envelope plus the reason per name
            async with s.post(
                url, json={"params": {"batch_size": 20000, "colour": "red"}}
            ) as r:
                assert r.status == 400
                assert await r.json() == {
                    "error": "invalid parameters for workflow 'deploy'",
                    "paramErrors": {
                        "batch_size": "must be at most 10000",
                        "colour": "is not a declared parameter",
                        "ticket": "is required",
                    },
                }
            for bad in (
                {"params": ["a"]},
                {"logicalDate": 5},
                {"logicalDate": "tomorrow", "params": {"ticket": "OPS-1"}},
                {
                    "logicalDate": "0001-01-01T00:00:00+00:01",
                    "params": {"ticket": "OPS-1"},
                },
                {"requestId": ""},
                {"requestId": "x" * 201},
                {"parmas": {}},
            ):
                async with s.post(url, json=bad) as r:
                    assert r.status == 400, bad
                    assert (await r.json())["error"]
            async with s.post(url, data=b"{not json") as r:
                assert r.status == 400
            async with s.post(
                base + "/dags/ghost/trigger", json={"params": {"a": 1}}
            ) as r:
                assert r.status == 404
            # a workflow with no declaration: no body starts a run, and a
            # value is refused
            plain = base + "/dags/plain/trigger"
            async with s.post(plain) as r:
                assert r.status == 200
                assert set(await r.json()) == {
                    "dag",
                    "name",
                    "runKey",
                    "created",
                }
            async with s.post(plain, json={"params": {}}) as r:
                assert r.status == 200
            async with s.post(plain, json={"params": {"x": 1}}) as r:
                assert r.status == 400
                assert (await r.json())["paramErrors"] == {
                    "x": "is not a declared parameter"
                }
            assert len(await cron._dag.list_runs("deploy")) == 1
    finally:
        await cron.start_stop_web_app(None)


async def test_http_backfill_with_params(dag_cron):
    import aiohttp

    cron = await dag_cron(_NIGHTLY)
    base = await _start_web(cron)
    url = base + "/dags/nightly/backfill"
    window = {
        "from": "2026-01-01T00:00:00+00:00",
        "to": "2026-01-01T01:30:00+00:00",
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                url, json={**window, "params": {"limit": "ten"}}
            ) as r:
                assert r.status == 400
                assert (await r.json())["paramErrors"] == {
                    "limit": "must be an integer"
                }
            async with s.post(url, json={**window, "params": "full"}) as r:
                assert r.status == 400
            # a misspelled key must not backfill with the defaults
            async with s.post(
                url, json={**window, "parms": {"limit": 3}}
            ) as r:
                assert r.status == 400
                assert '"parms"' in (await r.json())["error"]
            # a range that starts at the edge of the calendar
            async with s.post(
                url, json={"from": "0001-01-01", "to": "0001-01-02"}
            ) as r:
                assert r.status == 400
            assert await cron._dag.list_runs("nightly") == []
            async with s.post(
                url, json={**window, "params": {"limit": 3}}
            ) as r:
                assert r.status == 200
                created = (await r.json())["runKeys"]
            assert len(created) == 2
            for key in created:
                doc = await cron._dag.get_run("nightly", key)
                assert doc["params"] == {"mode": "incremental", "limit": 3}
    finally:
        await cron.start_stop_web_app(None)


async def test_http_params_scope(dag_cron):
    import aiohttp

    cron = await dag_cron(_NIGHTLY)
    await cron.start_stop_web_app(
        {
            "listen": ["http://127.0.0.1:0"],
            "ui": False,
            "authToken": {"value": "root"},
            "authTokens": [
                {"value": "ctl", "scopes": ["control"], "label": "ci"},
                {
                    "value": "ops",
                    "scopes": ["control", "params"],
                    "label": "ops-laptop",
                },
                {"value": "par", "scopes": ["params"], "label": "picker"},
            ],
        }
    )
    base = "http://127.0.0.1:{}".format(cron.web_runner.addresses[0][1])
    trigger = base + "/dags/nightly/trigger"
    backfill = base + "/dags/nightly/backfill"
    window = {
        "from": "2026-01-01T00:00:00+00:00",
        "to": "2026-01-01T00:30:00+00:00",
    }

    def bearer(token):
        return {"Authorization": "Bearer " + token}

    try:
        async with aiohttp.ClientSession() as s:
            # `control` alone starts a run with the defaults
            async with s.post(trigger, headers=bearer("ctl")) as r:
                assert r.status == 200
                assert (await r.json())["params"]["mode"] == "incremental"
            async with s.post(
                trigger, headers=bearer("ctl"), json={"params": {}}
            ) as r:
                assert r.status == 200
            # and is refused the moment it chooses a value
            before = len(await cron._dag.list_runs("nightly"))
            for url, body in (
                (trigger, {"params": {"mode": "full"}}),
                (backfill, {**window, "params": {"mode": "full"}}),
            ):
                async with s.post(url, headers=bearer("ctl"), json=body) as r:
                    assert r.status == 403
                    error = (await r.json())["error"]
                    assert "'ci'" in error and "'params'" in error
            assert len(await cron._dag.list_runs("nightly")) == before
            # the check runs before the values are read, so a token without
            # the scope learns nothing about the declaration
            async with s.post(
                trigger, headers=bearer("ctl"), json={"params": {"x": 1}}
            ) as r:
                assert r.status == 403
            # `control` and `params` together supply values, and the run
            # names the token
            async with s.post(
                trigger,
                headers=bearer("ops"),
                json={"params": {"mode": "full"}},
            ) as r:
                assert r.status == 200
                made = await r.json()
            assert made["params"]["mode"] == "full"
            doc = await cron._dag.get_run("nightly", made["runKey"])
            assert doc["triggeredBy"] == "ops-laptop"
            async with s.post(
                backfill,
                headers=bearer("ops"),
                json={**window, "params": {"mode": "full"}},
            ) as r:
                assert r.status == 200
            # `params` alone reads, and cannot reach the route
            async with s.get(base + "/dags", headers=bearer("par")) as r:
                assert r.status == 200
            async with s.post(
                trigger, headers=bearer("par"), json={"params": {"limit": 1}}
            ) as r:
                assert r.status == 403
            # the scalar token holds every scope
            async with s.post(
                trigger, headers=bearer("root"), json={"params": {"limit": 1}}
            ) as r:
                assert r.status == 200
            async with s.get(base + "/whoami", headers=bearer("root")) as r:
                who = await r.json()
            assert who["allScopes"] is True and "params" in who["scopes"]
            async with s.get(base + "/whoami", headers=bearer("ops")) as r:
                who = await r.json()
            assert who["scopes"] == ["control", "params", "view"]
            assert who["allScopes"] is False
    finally:
        await cron.start_stop_web_app(None)


def test_params_scope_model():
    assert WEB_PARAMS_SCOPE == params.SCOPE == "params"
    assert WEB_PARAMS_SCOPE in _WEB_ALL_SCOPES
    # like `control` and `approve`, it implies `view` and nothing else
    assert _effective_web_scopes(["params"]) == {"params", "view"}
    # a token that lists the other three scopes does not hold it
    assert "params" not in _effective_web_scopes(
        ["view", "control", "approve"]
    )


def test_for_run_checks_the_callers_scopes_before_it_reads_the_values():
    specs = _specs(_NIGHTLY)
    subject = "workflow 'nightly'"
    defaults = {"mode": "incremental", "limit": 10}
    # no token restricts the caller: every action is open, this one included
    assert params.for_run(specs, {"limit": 3}, subject) == {
        **defaults,
        "limit": 3,
    }
    # a token without the scope starts runs with the defaults
    control = _effective_web_scopes(["control"])
    assert params.for_run(specs, None, subject, control) == defaults
    assert params.for_run(specs, {}, subject, control) == defaults
    # and is refused before a value is read, so an undeclared name answers
    # like a declared one
    for supplied in ({"limit": 3}, {"x": 1}):
        with pytest.raises(params.ParamScopeError) as err:
            params.for_run(specs, supplied, subject, control)
        assert "'params' scope" in str(err.value) and not err.value.errors
    both = _effective_web_scopes(["control", "params"])
    assert params.for_run(specs, {"limit": 3}, subject, both)["limit"] == 3
    # a `params` member that is not an object is refused for every caller
    for scopes in (None, control, both):
        with pytest.raises(params.ParamError, match="must be an object"):
            params.for_run(specs, ["a"], subject, scopes)
    # nothing declared and nothing supplied stores no map
    assert params.for_run((), None, "job 'plain'") is None
    assert params.for_run((), {}, "job 'plain'", control) is None


def test_check_reuse_holds_a_caller_without_the_scope_to_the_defaults():
    specs = _specs(_NIGHTLY)
    defaults = {"mode": "incremental", "limit": 10}
    chosen = {**defaults, "limit": 3}
    control = _effective_web_scopes(["control"])
    both = _effective_web_scopes(["control", "params"])
    # the map a run stores when nobody chooses is open to `control`, in
    # whatever order the document holds it
    params.check_reuse(specs, defaults, control)
    params.check_reuse(specs, dict(reversed(defaults.items())), control)
    # reusing a chosen value is choosing it
    with pytest.raises(params.ParamScopeError) as err:
        params.check_reuse(specs, chosen, control)
    assert "'params' scope" in str(err.value) and not err.value.errors
    # the scope, or a caller that no token restricts, reuses any map
    params.check_reuse(specs, chosen, both)
    params.check_reuse(specs, chosen, None)
    # values compare as the run document writes them, and the map holds
    # the declared names and no others
    ratio = (params.ParamSpec(name="ratio", type=params.NUMBER, default=5),)
    params.check_reuse(ratio, {"ratio": 5}, control)
    for stored in ({"ratio": 5.0}, {}, {"ratio": 5, "extra": 1}):
        with pytest.raises(params.ParamScopeError):
            params.check_reuse(ratio, stored, control)
    # a required parameter has no default, so its value is always chosen
    stored = {**_DEFAULTS, "ticket": "OPS-9"}
    assert not params.check_stored(_specs(), stored)
    with pytest.raises(params.ParamScopeError):
        params.check_reuse(_specs(), stored, control)
    params.check_reuse(_specs(), stored, both)
    # the spec holds None in place of a default, and a stored null counts
    # as chosen all the same
    with pytest.raises(params.ParamScopeError):
        params.check_reuse(_specs(), {**_DEFAULTS, "ticket": None}, control)
    # nothing declared and nothing stored
    params.check_reuse((), {}, control)


async def test_web_param_error_answers_403_for_the_scope_and_400_otherwise(
    dag_cron,
):
    from aiohttp import web

    cron = await dag_cron(_NIGHTLY, web=True)
    token = _WebToken(b"t", _effective_web_scopes(["control"]), "ci")
    request = Req(storage={WEB_TOKEN_REQUEST_KEY: token})
    assert cron._web_scopes(request) == {"control", "view"}
    assert cron._web_scopes(Req()) is None
    with pytest.raises(web.HTTPForbidden) as err:
        cron._web_param_error(request, params.ParamScopeError("refused"))
    assert json.loads(err.value.text)["error"] == (
        "token 'ci' does not grant the 'params' permission required to "
        "supply run parameters"
    )
    # a route names the action that needs the scope
    with pytest.raises(web.HTTPForbidden) as err:
        cron._web_param_error(
            request, params.ParamScopeError("refused"), "recover a run"
        )
    assert json.loads(err.value.text)["error"] == (
        "token 'ci' does not grant the 'params' permission required to "
        "recover a run"
    )
    refused = params.ParamError("invalid", {"limit": "must be an integer"})
    response = cron._web_param_error(request, refused)
    assert response.status == 400
    assert json.loads(response.body) == {
        "error": "invalid",
        "paramErrors": {"limit": "must be an integer"},
    }
    with pytest.raises(web.HTTPBadRequest) as bad:
        cron._web_known_fields(
            {"parmas": {}, "to": 1}, "backfill", "from", "to"
        )
    assert json.loads(bad.value.text)["error"] == (
        'unknown backfill field "parmas"; the body accepts from, to'
    )


# --------------------------------------------------------------------------
# Recovery
# --------------------------------------------------------------------------


async def _failed_deploy(dag_cron, tmp_path):
    cron = await dag_cron(_DEPLOY)
    marker = tmp_path / "ready"
    check = "from pathlib import Path; assert Path({!r}).exists()".format(
        str(marker)
    )
    _set_cmd(cron, "deploy", "release", [_PY, "-c", check])
    result = await cron._dag.trigger(
        "deploy", params={"ticket": "OPS-9", "target": "prod"}
    )
    source = await _drive(cron, "deploy", result["runKey"])
    assert source["state"] == dag.FAILED
    return cron, source, marker


async def test_recovery_reuses_the_source_parameters(dag_cron, tmp_path):
    cron, source, marker = await _failed_deploy(dag_cron, tmp_path)
    preview = await cron._dag.recover("deploy", source["runKey"])
    assert preview["params"] == source["params"]
    assert preview["configurationChanged"] is False
    marker.touch()
    result = await cron._dag.recover(
        "deploy", source["runKey"], plan_token=preview["planToken"]
    )
    finished = await _drive(cron, "deploy", result["runKey"])
    assert finished["state"] == dag.SUCCESS
    assert finished["params"] == source["params"]
    assert finished["engine"] == dag.BRANCHING_PARAMS_ENGINE_LEVEL
    # the source run is untouched
    assert await cron._dag.get_run("deploy", source["runKey"]) == source


async def _nightly_run(cron, tmp_path, limit, logical=None, fail=True):
    """A finished run of ``nightly`` that stores ``limit``.

    ``load`` appends the limit it reads to ``seen.txt`` and fails until
    ``ready`` exists, so a failed source recovers with no configuration
    change. Returns with ``ready`` in place and ``seen.txt`` empty.
    """
    seen, ready = tmp_path / "seen.txt", tmp_path / "ready"
    load = (
        "import os; from pathlib import Path; "
        "limit = os.environ['CRONSTABLE_PARAM_LIMIT']; "
        "open({!r}, 'a').write(limit + '\\n'); "
        "assert Path({!r}).exists()"
    ).format(str(seen), str(ready))
    _set_cmd(cron, "nightly", "load", [_PY, "-c", load])
    if fail:
        ready.unlink(missing_ok=True)
    else:
        ready.touch()
    started = await cron._dag.trigger(
        "nightly", params={"limit": limit}, logical_date=logical
    )
    source = await _drive(cron, "nightly", started["runKey"])
    assert source["state"] == (dag.FAILED if fail else dag.SUCCESS)
    ready.touch()
    seen.write_text("")
    return source


async def test_recovery_of_chosen_values_needs_the_params_scope(
    dag_cron, tmp_path
):
    cron = await dag_cron(_NIGHTLY)
    key = (await _nightly_run(cron, tmp_path, 3))["runKey"]
    control = _effective_web_scopes(["control"])
    both = _effective_web_scopes(["control", "params"])
    # the preview is the same for every caller
    preview = await cron._dag.recover("nightly", key, scopes=control)
    assert preview == await cron._dag.recover("nightly", key)
    assert preview["params"] == {"mode": "incremental", "limit": 3}
    rerun = await cron._dag.recover(
        "nightly", key, mode="from", tasks=["load"], scopes=control
    )
    before = await cron._dag.list_runs("nightly")
    # execution reuses a chosen value, whichever tasks it selects
    for selection, plan in (
        ({}, preview),
        ({"mode": "from", "tasks": ["load"]}, rerun),
    ):
        with pytest.raises(params.ParamScopeError):
            await cron._dag.recover(
                "nightly",
                key,
                plan_token=plan["planToken"],
                allow_config_change=True,
                scopes=control,
                **selection,
            )
    assert await cron._dag.list_runs("nightly") == before
    # the refusal released the source run's lease
    result = await cron._dag.recover(
        "nightly", key, plan_token=preview["planToken"], scopes=both
    )
    assert result["created"] is True
    finished = await _drive(cron, "nightly", result["runKey"])
    assert finished["state"] == dag.SUCCESS
    assert (tmp_path / "seen.txt").read_text().split() == ["3"]
    # a repeat of an accepted plan creates nothing, whoever sends it
    again = await cron._dag.recover(
        "nightly", key, plan_token=preview["planToken"], scopes=control
    )
    assert again["created"] is False and again["runKey"] == result["runKey"]


async def test_recovery_of_the_defaults_stays_a_control_action(
    dag_cron, tmp_path
):
    cron = await dag_cron(_NIGHTLY)
    key = (await _nightly_run(cron, tmp_path, 10))["runKey"]
    control = _effective_web_scopes(["control"])
    preview = await cron._dag.recover("nightly", key, scopes=control)
    result = await cron._dag.recover(
        "nightly", key, plan_token=preview["planToken"], scopes=control
    )
    finished = await _drive(cron, "nightly", result["runKey"])
    assert finished["state"] == dag.SUCCESS
    assert finished["params"] == {"mode": "incremental", "limit": 10}
    # the defaults are the current declaration's: once one moves, the
    # stored value is a chosen one
    moved = parse_config_string(
        _STATE + _NIGHTLY.replace("default: 10", "default: 11"), ""
    )
    cron.cron_dags = {d.name: d for d in moved.dags}
    preview = await cron._dag.recover(
        "nightly", key, mode="from", tasks=["load"], scopes=control
    )
    with pytest.raises(params.ParamScopeError):
        await cron._dag.recover(
            "nightly",
            key,
            mode="from",
            tasks=["load"],
            plan_token=preview["planToken"],
            allow_config_change=True,
            scopes=control,
        )


async def test_failed_dates_recovery_refuses_before_it_records_a_batch(
    dag_cron, tmp_path
):
    cron = await dag_cron(_NIGHTLY)
    await _nightly_run(cron, tmp_path, 10, "2026-09-01T00:00:00+00:00")
    await _nightly_run(cron, tmp_path, 3, "2026-09-02T00:00:00+00:00")
    control = _effective_web_scopes(["control"])
    window = ("nightly", "2026-09-01", "2026-09-02")
    preview = await cron._dag.recover_range(*window, scopes=control)
    assert [p["params"]["limit"] for p in preview["plans"]] == [10, 3]
    before = await cron._dag.list_runs("nightly")
    with pytest.raises(params.ParamScopeError):
        await cron._dag.recover_range(
            *window,
            plan_token=preview["planToken"],
            allow_config_change=True,
            scopes=control,
        )
    # refused whole: the date that stores the defaults did not start, and
    # no batch holds the source runs back from retention
    assert await cron._dag.list_runs("nightly") == before
    batches = await cron.state_backend.list_documents("recoverybatch/nightly")
    assert batches == []
    # the date that stores the defaults stays a `control` action
    first = ("nightly", "2026-09-01", "2026-09-01")
    one = await cron._dag.recover_range(*first, scopes=control)
    done = await cron._dag.recover_range(
        *first, plan_token=one["planToken"], scopes=control
    )
    assert len(done["runs"]) == 1


async def test_a_control_caller_cannot_continue_a_batch_of_chosen_values(
    dag_cron, tmp_path, monkeypatch
):
    cron = await dag_cron(_NIGHTLY)
    await _nightly_run(cron, tmp_path, 3, "2026-09-01T00:00:00+00:00")
    window = ("nightly", "2026-09-01", "2026-09-01")
    preview = await cron._dag.recover_range(*window)
    both = _effective_web_scopes(["control", "params"])
    recover = cron._dag.recover
    handed = {}

    async def interrupted(name, key, **kwargs):
        handed.update(kwargs)
        raise recovery.RecoveryError("temporary interruption")

    # a caller with the scope records the batch and is interrupted
    monkeypatch.setattr(cron._dag, "recover", interrupted)
    with pytest.raises(recovery.RecoveryError, match="interruption"):
        await cron._dag.recover_range(
            *window, plan_token=preview["planToken"], scopes=both
        )
    # recover() checks each date again under the configuration it creates
    # the run with, so it gets the caller's scopes
    assert handed["scopes"] == both
    monkeypatch.setattr(cron._dag, "recover", recover)
    before = await cron._dag.list_runs("nightly")
    with pytest.raises(params.ParamScopeError):
        await cron._dag.recover_range(
            *window,
            plan_token=preview["planToken"],
            scopes=_effective_web_scopes(["control"]),
        )
    assert await cron._dag.list_runs("nightly") == before
    # the caller that recorded it continues it
    done = await cron._dag.recover_range(
        *window, plan_token=preview["planToken"], scopes=both
    )
    assert len(done["runs"]) == 1


async def test_http_recovery_needs_the_params_scope_for_chosen_values(
    dag_cron, tmp_path
):
    import aiohttp

    cron = await dag_cron(_NIGHTLY)
    day = "2026-09-01T00:00:00+00:00"
    succeeded = await _nightly_run(cron, tmp_path, 3, fail=False)
    await _nightly_run(cron, tmp_path, 3, day)
    seen = tmp_path / "seen.txt"
    await cron.start_stop_web_app(
        {
            "listen": ["http://127.0.0.1:0"],
            "ui": False,
            "authTokens": [
                {"value": "ctl", "scopes": ["control"], "label": "ci"},
                {
                    "value": "ops",
                    "scopes": ["control", "params"],
                    "label": "ops-laptop",
                },
            ],
        }
    )
    base = "http://127.0.0.1:{}/dags/nightly".format(
        cron.web_runner.addresses[0][1]
    )
    requests = (
        # a successful run, replayed from its task
        (
            base + "/runs/{}/recover".format(succeeded["runKey"]),
            {"mode": "from", "tasks": ["load"]},
        ),
        # a failed date
        (base + "/recover", {"from": day, "to": day}),
    )

    def bearer(token):
        return {"Authorization": "Bearer " + token}

    before = await cron._dag.list_runs("nightly")
    try:
        async with aiohttp.ClientSession() as s:
            reviewed = []
            for url, body in requests:
                # the preview stays a `control` action
                async with s.post(url, headers=bearer("ctl"), json=body) as r:
                    assert r.status == 200, url
                    plan = await r.json()
                shown = plan.get("plans") or [plan]
                assert [p["params"]["limit"] for p in shown] == [3]
                # the acknowledgement is the caller's own to give, so it
                # does not stand in for the scope
                reviewed.append(
                    {
                        **body,
                        "dryRun": False,
                        "planToken": plan["planToken"],
                        "allowConfigChange": True,
                    }
                )
                async with s.post(
                    url, headers=bearer("ctl"), json=reviewed[-1]
                ) as r:
                    assert r.status == 403, url
                    assert (await r.json())["error"] == (
                        "token 'ci' does not grant the 'params' permission "
                        "required to recover a run with parameter values "
                        "other than the declared defaults"
                    )
            assert await cron._dag.list_runs("nightly") == before
            assert seen.read_text() == ""
            # the same reviewed requests run for a token that holds both
            for (url, _body), execute in zip(requests, reviewed):
                async with s.post(
                    url, headers=bearer("ops"), json=execute
                ) as r:
                    assert r.status == 200, url
                    done = await r.json()
                for key in done.get("runs") or [done["runKey"]]:
                    finished = await _drive(cron, "nightly", key)
                    assert finished["params"]["limit"] == 3
            assert seen.read_text().split() == ["3", "3"]
    finally:
        await cron.start_stop_web_app(None)


async def test_recovery_refuses_values_the_declaration_no_longer_fits(
    dag_cron, tmp_path
):
    cron, source, _marker = await _failed_deploy(dag_cron, tmp_path)
    narrowed = _DEPLOY.replace("          - prod\n", "").replace(
        "      - name: dry\n        type: boolean\n        default: no\n", ""
    )
    reloaded = parse_config_string(_STATE + narrowed, "")
    cron.cron_dags = {d.name: d for d in reloaded.dags}
    with pytest.raises(recovery.RecoveryError) as raised:
        await cron._dag.recover("deploy", source["runKey"])
    message = str(raised.value)
    assert "do not fit the current declaration" in message
    assert "dry is not a declared parameter" in message
    assert "target must be one of: staging" in message


async def test_a_declaration_change_changes_the_revision(dag_cron, tmp_path):
    cron, source, _marker = await _failed_deploy(dag_cron, tmp_path)
    widened = _DEPLOY.replace("maximum: 10000", "maximum: 20000")
    reloaded = parse_config_string(_STATE + widened, "")
    cron.cron_dags = {d.name: d for d in reloaded.dags}
    for task in cron.cron_dags["deploy"].task_templates.values():
        task.command = [_PY, "-c", "pass"]
    preview = await cron._dag.recover("deploy", source["runKey"])
    # the stored values still fit, and the declaration is not the one the
    # run was created under
    assert preview["configurationChanged"] is True
    with pytest.raises(recovery.RecoveryError, match="configuration differs"):
        await cron._dag.recover(
            "deploy", source["runKey"], plan_token=preview["planToken"]
        )


def test_configuration_revision_reads_the_declaration():
    def revision(yaml):
        config = parse_config_string(_STATE + yaml, "")
        return recovery.configuration_revision(config.dags[0])

    base = revision(_NIGHTLY)
    assert revision(_NIGHTLY) == base
    assert revision(_NIGHTLY.replace("default: 10", "default: 11")) != base
    assert revision(_NIGHTLY.replace("          - full\n", "")) != base
    # a description is text for the forms and stays out of the digest
    described = _NIGHTLY.replace(
        "        default: 10\n",
        "        default: 10\n        description: Rows per batch\n",
    )
    assert revision(described) == base
    # so is the order of the parameters, which only arranges the forms
    mode = (
        "      - name: mode\n        default: incremental\n"
        "        allowed:\n          - incremental\n          - full\n"
    )
    limit = "      - name: limit\n        type: integer\n        default: 10\n"
    assert mode + limit in _NIGHTLY
    assert revision(_NIGHTLY.replace(mode + limit, limit + mode)) == base
    # a workflow that declares nothing digests the bare task list
    # parameters existed
    without = _NIGHTLY[: _NIGHTLY.index("    params:")] + (
        "    tasks:\n      - id: load\n        command: 'x'\n"
    )
    plain = parse_config_string(_STATE + without, "").dags[0]
    assert plain.spec.params == ()
    assert recovery.configuration_revision(plain) != base


# --------------------------------------------------------------------------
# when: a comparison on a parameter
# --------------------------------------------------------------------------

_GATED = """
dags:
  - name: gated
    params:
      - name: mode
        default: incremental
        allowed:
          - incremental
          - full
      - name: limit
        type: integer
        default: 10
    tasks:
      - id: full
        command: 'x'
        when:
          - param: mode
            equals: full
      - id: big
        command: 'x'
        when:
          - param: limit
            notIn:
              - '10'
              - '20'
"""


async def test_a_condition_decides_by_the_values_a_trigger_supplies(dag_cron):
    import aiohttp

    cron = await dag_cron(_GATED)
    for task_id in ("full", "big"):
        _set_cmd(cron, "gated", task_id, [_PY, "-c", "pass"])
    base = await _start_web(cron)

    try:
        async with aiohttp.ClientSession() as s:

            async def run(supplied):
                url = base + "/dags/gated/trigger"
                async with s.post(url, json={"params": supplied}) as r:
                    assert r.status == 200
                    key = (await r.json())["runKey"]
                await _drive(cron, "gated", key)
                async with s.get(base + "/dags/gated/runs/" + key) as r:
                    assert r.status == 200
                    return (await r.json())["tasks"]

            async with s.get(base + "/dags") as r:
                (declared,) = await r.json()
            exported = {t["id"]: t["when"] for t in declared["tasks"]}
            # a parameter's comparison values are served in its type
            assert exported == {
                "full": [{"param": "mode", "equals": "full"}],
                "big": [{"param": "limit", "notIn": [10, 20]}],
            }
            # the defaults: neither comparison holds
            tasks = await run({})
            assert tasks["full"]["state"] == tasks["big"]["state"] == "skipped"
            assert tasks["full"]["skipReason"] == {
                "kind": "condition",
                "detail": "param mode equals full: the value is incremental",
            }
            assert tasks["big"]["skipReason"] == {
                "kind": "condition",
                "detail": "param limit notIn 10, 20: the value is 10",
            }
            # the supplied values decide each task on its own
            tasks = await run({"mode": "full", "limit": 20})
            assert tasks["full"]["state"] == "success"
            assert tasks["full"]["whenMet"] is True
            assert tasks["big"]["state"] == "skipped"
            tasks = await run({"limit": 500})
            assert tasks["full"]["state"] == "skipped"
            assert tasks["big"]["state"] == "success"
    finally:
        await cron.start_stop_web_app(None)


def test_a_condition_value_is_checked_against_the_declaration():
    # a comparison is checked like a supplied value, so narrowing `allowed`
    # without updating the comparison fails the load
    parse_config_string(_STATE + _GATED, "")
    with pytest.raises(ConfigError) as err:
        parse_config_string(
            _STATE + _GATED.replace("          - full\n", "", 1), ""
        )
    assert (
        "dag 'gated': task 'full': when entry 1: param 'mode': value 'full' "
        "must be one of: incremental"
    ) in str(err.value)
