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
  * recovery: the stored map is reused, and the declaration is part of the
    configuration revision.
"""

import json
import logging
import math
import sys

import pytest

from cronstable import dag, dagrun, params, recovery
from cronstable.config import ConfigError, parse_config_string
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
        await cron._dag.trigger_run("deploy")
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
    # the UTC designator reads the same on every Python version
    zulu = await cron._dag.trigger(
        "plain", logical_date="2026-10-01T00:00:00Z"
    )
    body = await cron._dag.get_run("plain", zulu["runKey"])
    assert body["logicalDate"] == "2026-10-01T00:00:00+00:00"
    assert dagrun._parse_iso("2026-10-01T00:00:00z") == dagrun._parse_iso(
        "2026-10-01T00:00:00+00:00"
    )


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
    assert WEB_PARAMS_SCOPE == "params"
    assert WEB_PARAMS_SCOPE in _WEB_ALL_SCOPES
    # like `control` and `approve`, it implies `view` and nothing else
    assert _effective_web_scopes(["params"]) == {"params", "view"}
    # a token issued with the three earlier scopes gains nothing
    assert "params" not in _effective_web_scopes(
        ["view", "control", "approve"]
    )


async def test_web_run_params_reads_the_matched_token(dag_cron):
    cron = await dag_cron(_NIGHTLY, web=True)

    def request(scopes):
        token = _WebToken(b"t", _effective_web_scopes(scopes), "lbl")
        return Req(storage={WEB_TOKEN_REQUEST_KEY: token})

    assert cron._web_run_params(Req(), {}) is None
    # no auth middleware: every action is open, this one included
    assert cron._web_run_params(Req(), {"params": {"a": 1}}) == {"a": 1}
    assert cron._web_run_params(request(["control"]), {"params": {}}) == {}
    assert cron._web_run_params(
        request(["control", "params"]), {"params": {"a": 1}}
    ) == {"a": 1}
    from aiohttp import web

    with pytest.raises(web.HTTPForbidden):
        cron._web_run_params(request(["control"]), {"params": {"a": 1}})
    with pytest.raises(web.HTTPBadRequest):
        cron._web_run_params(request(["control", "params"]), {"params": 1})


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
    # a workflow that declares nothing digests the bare task list, as before
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
