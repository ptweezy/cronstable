"""Run parameters: the declaration model and the checks on supplied values.

A workflow declares its parameters under ``params:``. A caller supplies
values when a run starts, and the resolved map is written once into the run
document. This module holds the pure half: the normalised declaration
(:class:`ParamSpec`), the check a value has to pass, the resolution of a
supplied map against a declaration, and the text form a value takes in a
task's environment. It does no I/O and imports only the standard library, so
the configuration loader, the state machine, the driver, and the thin
clients all share it.
"""

import json
import math
import re
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

STRING = "string"
INTEGER = "integer"
NUMBER = "number"
BOOLEAN = "boolean"

#: Every ``type`` a parameter can declare, in documentation order.
PARAM_TYPES = (STRING, INTEGER, NUMBER, BOOLEAN)

#: At most this many parameters per declaration.
MAX_PARAMS = 32
#: A string value's ceiling, in bytes of UTF-8.
MAX_STRING_BYTES = 4096
#: An integer value's magnitude ceiling, the largest integer a JSON double
#: carries exactly, so a JavaScript or Swift client reads the stored value.
MAX_INTEGER = 2**53 - 1
#: The resolved map's ceiling as compact JSON. The run document is copied on
#: every state change, so the map stays small.
MAX_PARAMS_BYTES = 16 * 1024

#: A task reads parameter ``name`` as ``CRONSTABLE_PARAM_<NAME>``.
ENV_PREFIX = "CRONSTABLE_PARAM_"

#: The token scope a caller holds, on top of the action's own, to choose
#: values. A caller without it starts runs with the declared defaults.
SCOPE = "params"

#: A parameter name, matched whole. Every character is valid in an
#: environment variable name, so the exported name needs no escaping.
_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}")

#: The characters a string value cannot hold: the C0 and C1 control ranges,
#: DEL, and the Unicode line and paragraph separators. A value reaches
#: shells, logs, and line-based tools, where each of these can end a line.
CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]")

_INTEGER_TEXT = re.compile(r"[+-]?[0-9]+")
_NUMBER_TEXT = re.compile(
    r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
)

# the spellings strictyaml's Bool() accepts, so `default: yes` reads the way
# `enabled: yes` does
_TRUE_TEXT = frozenset({"yes", "true", "on", "1", "y"})
_FALSE_TEXT = frozenset({"no", "false", "off", "0", "n"})

#: ``paramErrors`` text for a name the declaration does not hold.
UNDECLARED = "is not a declared parameter"

#: The pattern parser behind :mod:`re`. It is private to the standard
#: library, so :func:`nested_repetition` answers False without it.
_PARSER: Any = getattr(re, "_parser", None)

#: The longest ``maxLength`` at which :func:`nested_repetition` answers
#: False for a pattern whose only choices are its two nested repeats.
SHORT_MAX_LENGTH = 16


@dataclass(frozen=True, slots=True)
class ParamSpec:
    """One declared parameter, normalised.

    ``default`` is the value a run gets when the caller supplies none, in
    the declared type. A parameter has a default unless it is ``required``,
    so every run of one declaration stores the same names.
    """

    name: str
    type: str = STRING
    required: bool = False
    default: Any = None
    allowed: tuple[Any, ...] = ()
    minimum: int | float | None = None
    maximum: int | float | None = None
    pattern: str | None = None
    max_length: int | None = None
    description: str = ""


class ParamError(Exception):
    """Supplied values that a run cannot take.

    ``errors`` maps each offending parameter name to the reason, phrased to
    follow the name ("must be at most 10000"). It is empty when the fault
    belongs to the map as a whole.
    """

    def __init__(
        self, message: str, errors: Mapping[str, str] | None = None
    ) -> None:
        super().__init__(message)
        self.errors: dict[str, str] = dict(errors or {})


class ParamScopeError(ParamError):
    """Values that a caller supplies or reuses although its token does not
    grant :data:`SCOPE`."""


def _echo(name: str) -> str:
    """An undeclared name as an error reports it: a valid name as it is,
    anything else escaped to ASCII and cut to the length of a valid name."""
    if valid_name(name):
        return name
    return json.dumps(name)[1:-1][:64]


def valid_name(name: str) -> bool:
    """Whether ``name`` is a usable parameter name."""
    return _NAME.fullmatch(name) is not None


def parse_text(kind: str, text: str) -> Any:
    """Convert ``text`` to a value of the declared type ``kind``.

    The conversion a configuration file and a text form need, since both
    hold every value as text. Raises :class:`ValueError` when the text is
    not a value of that type. The result still has to pass
    :func:`check_value`.
    """
    if kind == STRING:
        return text
    stripped = text.strip()
    if kind == BOOLEAN:
        lowered = stripped.lower()
        if lowered in _TRUE_TEXT:
            return True
        if lowered in _FALSE_TEXT:
            return False
        raise ValueError("must be true or false")
    if kind == INTEGER:
        if _INTEGER_TEXT.fullmatch(stripped) is None:
            raise ValueError("must be an integer")
        return int(stripped)
    if _NUMBER_TEXT.fullmatch(stripped) is None:
        raise ValueError("must be a number")
    if _INTEGER_TEXT.fullmatch(stripped) is not None:
        return int(stripped)
    return float(stripped)


def check_value(spec: ParamSpec, value: Any) -> str | None:
    """Why ``value`` is refused for ``spec``, or ``None`` when it passes.

    A value has the declared JSON type exactly: nothing is coerced, so the
    string ``"5"`` is refused for an integer and ``true`` is refused for a
    number.
    """
    kind = spec.type
    if kind == STRING:
        if type(value) is not str:
            return "must be a string"
        try:
            size = len(value.encode("utf-8"))
        except UnicodeEncodeError:
            return "must be valid Unicode text"
        if size > MAX_STRING_BYTES:
            return "must be at most {} bytes".format(MAX_STRING_BYTES)
        if CONTROL_CHARACTERS.search(value) is not None:
            return "must not contain control or line-separator characters"
        if spec.max_length is not None and len(value) > spec.max_length:
            return "must be at most {} characters".format(spec.max_length)
        if (
            spec.pattern is not None
            and re.fullmatch(spec.pattern, value) is None
        ):
            return "must match the pattern {}".format(spec.pattern)
    elif kind == BOOLEAN:
        if type(value) is not bool:
            return "must be true or false"
    else:
        if kind == INTEGER:
            if type(value) is not int:
                return "must be an integer"
        elif type(value) not in (int, float):
            return "must be a number"
        if type(value) is float and not math.isfinite(value):
            return "must be a finite number"
        if type(value) is int and abs(value) > MAX_INTEGER:
            if kind == INTEGER:
                return "must be between -{0} and {0}".format(MAX_INTEGER)
            try:
                float(value)
            except OverflowError:
                return "must be a finite number"
        if spec.minimum is not None and value < spec.minimum:
            return "must be at least {}".format(env_text(spec.minimum))
        if spec.maximum is not None and value > spec.maximum:
            return "must be at most {}".format(env_text(spec.maximum))
    if spec.allowed and value not in spec.allowed:
        return "must be one of: {}".format(
            ", ".join(env_text(v) for v in spec.allowed)
        )
    return None


def nested_repetition(pattern: str, max_length: int | None = None) -> bool:
    """Whether ``pattern`` nests repeats the way ``(a+)+`` does.

    True when a repeat with no upper bound has a body that one inner repeat
    can match alone, because every other item in the body can match the
    empty string. The inner repeat counts when it can run more than once
    and its count can vary, as ``a+`` and ``a{1,3}`` can. :func:`check_value`
    can take time that grows exponentially with the length of a value that
    such a pattern refuses.

    ``max_length`` caps the length of the values the pattern checks. At
    :data:`SHORT_MAX_LENGTH` or less, the two repeats alone refuse a value
    within a few milliseconds, so the answer is True only when the pattern
    holds a further choice: an alternative, or another repeat that counts.

    The check does not examine the contents of a possessive repeat or an
    atomic group, and a pattern can still be slow inside one.

    The answer comes from the parse tree, so the pattern never runs. It is
    False when the parser is missing or its tree has an unexpected shape.
    """
    try:
        tree = _PARSER.parse(pattern)
        if not _scan(tree)[0]:
            return False
        if max_length is None or max_length > SHORT_MAX_LENGTH:
            return True
        return _choices(tree) > 2
    except Exception:  # noqa: BLE001 - the parser is private to re
        return False


def _scan(items: Any) -> tuple[bool, bool, bool]:
    """Three answers about a parsed sequence, from one walk of its tree.

    Whether it holds the repeat that :func:`nested_repetition` reports,
    whether one repeat of the kind that function counts can match it alone,
    and whether it can match the empty string at any position. An anchor, a
    lookaround, and a back-reference depend on the position, so each counts
    as required.
    """
    sre = _PARSER
    nests = spare = False
    required: list[bool] = []
    for op, av in items:
        # the same three answers for one item
        nested = alone = empty = False
        if op in (sre.MAX_REPEAT, sre.MIN_REPEAT):
            low, high, body = av
            nested, alone, empty = _scan(body)
            unbounded = high == sre.MAXREPEAT
            nested = nested or (unbounded and alone)
            alone = high > max(low, 1) or (alone and high > 0)
            empty = empty or low == 0
        elif op is sre.POSSESSIVE_REPEAT:
            empty = av[0] == 0
        elif op is sre.SUBPATTERN:
            nested, alone, empty = _scan(av[3])
        elif op is sre.BRANCH:
            scans = [_scan(alternative) for alternative in av[1]]
            nested, alone, empty = map(any, zip(*scans, strict=True))
        elif op in (sre.ASSERT, sre.ASSERT_NOT):
            nested = _scan(av[1])[0]
        elif op is sre.GROUPREF_EXISTS:
            nested = any(_scan(sub)[0] for sub in av[1:] if sub is not None)
        nests = nests or nested
        if empty:
            spare = spare or alone
        else:
            required.append(alone)
    # with a required item, the lone repeat has to be that item
    if required:
        return nests, len(required) == 1 and required[0], False
    return nests, spare, True


def _choices(items: Any) -> int:
    """How many choices a parsed sequence holds, over the items that
    :func:`_scan` examines: each repeat of the kind that
    :func:`nested_repetition` counts, and each alternative after the
    first."""
    sre = _PARSER
    count = 0
    for op, av in items:
        if op in (sre.MAX_REPEAT, sre.MIN_REPEAT):
            low, high, body = av
            count += (high > max(low, 1)) + _choices(body)
        elif op is sre.SUBPATTERN:
            count += _choices(av[3])
        elif op is sre.BRANCH:
            count += len(av[1]) - 1 + sum(map(_choices, av[1]))
        elif op in (sre.ASSERT, sre.ASSERT_NOT):
            count += _choices(av[1])
        elif op is sre.GROUPREF_EXISTS:
            count += sum(_choices(sub) for sub in av[1:] if sub is not None)
    return count


def stored_form(spec: ParamSpec, value: Any) -> Any:
    """A checked value as a run stores it.

    The value itself, except that a ``number`` given as an integer beyond
    :data:`MAX_INTEGER` is stored as the float it equals, so every stored
    number is one a JSON double carries.
    """
    if spec.type == NUMBER and type(value) is int and abs(value) > MAX_INTEGER:
        return float(value)
    return value


def encoded_size(params: Mapping[str, Any]) -> int:
    """Bytes of ``params`` as compact JSON, the form the size limit counts."""
    return len(
        json.dumps(params, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
    )


def resolve(
    specs: Iterable[ParamSpec], supplied: Mapping[str, Any], subject: str
) -> dict[str, Any]:
    """The map a run stores: ``supplied`` checked, with defaults filled in.

    The result holds every declared name, in declaration order. ``subject``
    names what the parameters belong to in the error, for example
    ``workflow 'deploy'``. Raises :class:`ParamError` and resolves nothing
    when a supplied name is undeclared, a value is refused, a required
    parameter is absent, or the map is too large.
    """
    by_name = {spec.name: spec for spec in specs}
    if len(supplied) > MAX_PARAMS:
        raise ParamError(
            "{} takes at most {} parameters".format(subject, MAX_PARAMS)
        )
    errors: dict[str, str] = {}
    for name, value in supplied.items():
        spec = by_name.get(name)
        if spec is None:
            errors[_echo(name)] = UNDECLARED
            continue
        problem = check_value(spec, value)
        if problem is not None:
            errors[name] = problem
    if not by_name and errors:
        raise ParamError("{} declares no parameters".format(subject), errors)
    resolved: dict[str, Any] = {}
    for spec in by_name.values():
        if spec.name in supplied:
            if spec.name not in errors:
                resolved[spec.name] = stored_form(spec, supplied[spec.name])
        elif spec.required:
            errors[spec.name] = "is required"
        else:
            resolved[spec.name] = spec.default
    if errors:
        raise ParamError("invalid parameters for {}".format(subject), errors)
    if encoded_size(resolved) > MAX_PARAMS_BYTES:
        raise ParamError(
            "the parameters for {} are larger than {} bytes as JSON".format(
                subject, MAX_PARAMS_BYTES
            )
        )
    return resolved


def for_run(
    specs: Iterable[ParamSpec],
    supplied: Any,
    subject: str,
    scopes: Collection[str] | None = None,
) -> dict[str, Any] | None:
    """The map a new run stores, from the ``params`` a request carries.

    ``None`` when ``specs`` declares nothing and the request supplies
    nothing, and otherwise what :func:`resolve` returns. ``scopes`` are the
    scopes of the caller's token, or ``None`` for a caller that no token
    restricts.

    Raises :class:`ParamError` when ``supplied`` is not an object or
    :func:`resolve` refuses it, and :class:`ParamScopeError` when it holds
    values and ``scopes`` lacks :data:`SCOPE`. The scope is checked before
    the values are read, so a caller without it learns nothing about the
    declaration.
    """
    if supplied is not None and not isinstance(supplied, Mapping):
        raise ParamError("params must be an object")
    if supplied and scopes is not None and SCOPE not in scopes:
        raise ParamScopeError(
            "the token lacks the {!r} scope that supplying run parameters "
            "requires; call again without params to use the "
            "defaults".format(SCOPE)
        )
    specs = tuple(specs)
    if not specs and not supplied:
        return None
    return resolve(specs, supplied or {}, subject)


def check_stored(
    specs: Iterable[ParamSpec], stored: Mapping[str, Any]
) -> dict[str, str]:
    """Why a stored map fails a declaration, by name. Empty when it fits.

    Stricter than :func:`resolve`: a stored map is reused as it is, so no
    default fills a declared name that it lacks.
    """
    by_name = {spec.name: spec for spec in specs}
    errors: dict[str, str] = {}
    for name, value in stored.items():
        spec = by_name.get(name)
        if spec is None:
            errors[_echo(name)] = UNDECLARED
            continue
        problem = check_value(spec, value)
        if problem is not None:
            errors[name] = problem
    for name in by_name:
        if name not in stored:
            errors[name] = "has no stored value"
    return errors


def check_reuse(
    specs: Iterable[ParamSpec],
    stored: Mapping[str, Any],
    scopes: Collection[str] | None,
) -> None:
    """Refuse to reuse ``stored`` for a caller whose token lacks :data:`SCOPE`.

    A recovery run reuses its source run's map as it is. A caller without
    the scope starts runs with the declared defaults, so the only map it
    reuses is the one a run stores when nobody chooses, with every declared
    name at its default. Values compare as the run document writes them, so
    the number ``5.0`` differs from a default of ``5``. A ``required``
    parameter has no default, so its value is always a chosen one.
    ``scopes`` are the scopes of the caller's token, or ``None`` for a
    caller that no token restricts.

    Raises :class:`ParamScopeError`.
    """
    if scopes is None or SCOPE in scopes:
        return
    specs = tuple(specs)
    defaults = {spec.name: spec.default for spec in specs}
    if any(spec.required for spec in specs) or json.dumps(
        stored, sort_keys=True
    ) != json.dumps(defaults, sort_keys=True):
        raise ParamScopeError(
            "the token lacks the {!r} scope that recovering a run with "
            "parameter values other than the declared defaults "
            "requires".format(SCOPE)
        )


def env_text(value: Any) -> str:
    """A value as a task reads it: ``true`` or ``false`` for a boolean, the
    text itself for a string, and the JSON text for a number."""
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return value
    return json.dumps(value)


def environment(params: Mapping[str, Any]) -> dict[str, str]:
    """The ``CRONSTABLE_PARAM_*`` variables for a stored map.

    The name is upper-cased. Declared names are unique ignoring case, so two
    parameters never share a variable, including on Windows, where
    environment names ignore case. An entry that is not a scalar under a
    valid name is left out, so a damaged document cannot inject a variable.
    """
    env: dict[str, str] = {}
    for name, value in params.items():
        if (
            isinstance(name, str)
            and valid_name(name)
            and isinstance(value, (str, int, float, bool))
        ):
            env[ENV_PREFIX + name.upper()] = env_text(value)
    return env


def declaration(specs: Iterable[ParamSpec]) -> list[dict[str, Any]]:
    """A declaration in the shape the configuration writes it.

    The form ``GET /dags`` serves and a client builds its form from. A key
    is present only when the parameter sets it.
    """
    out = []
    for spec in specs:
        entry: dict[str, Any] = {"name": spec.name, "type": spec.type}
        if spec.required:
            entry["required"] = True
        else:
            entry["default"] = spec.default
        if spec.allowed:
            entry["allowed"] = list(spec.allowed)
        if spec.minimum is not None:
            entry["minimum"] = spec.minimum
        if spec.maximum is not None:
            entry["maximum"] = spec.maximum
        if spec.pattern is not None:
            entry["pattern"] = spec.pattern
        if spec.max_length is not None:
            entry["maxLength"] = spec.max_length
        if spec.description:
            entry["description"] = spec.description
        out.append(entry)
    return out
