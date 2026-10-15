"""Status badges: small SVG images of a job's or the fleet's health.

:func:`render_badge` draws the two-part badge that shields.io made familiar
from CI pipelines: a gray label on the left, such as a job's name, and a
colored message on the right, such as ``passing`` or ``failing``.  The
daemon serves it as ``GET /jobs/{name}/badge.svg`` (one job) and
``GET /badge.svg`` (the fleet); the "Status Badges" wiki page is the user
documentation.  :func:`job_verdict` and :func:`fleet_message` decide what a
badge says.

Rendering choices, all deliberate:

- No font travels with the image, so text widths come from a table of
  DejaVu Sans advance widths at 11px.  Verdana, the first font the image
  names, draws most of these characters within a pixel of the table, and
  a few, such as ``_``, ``/``, and ``J``, up to 2px wider.  Each text run
  also carries ``textLength``, so a viewer that draws another font still
  fits the text to its box.
- Every string is XML-escaped, and characters that XML forbids become
  U+FFFD.  :func:`label_error` also keeps control characters and line
  breaks out of a caller's label override.  The daemon serves these images
  from the dashboard's own origin, so markup in a label must never reach
  the SVG.
- The image carries ``role="img"``, an ``aria-label``, and a ``<title>``
  that read "label: message", so screen readers and hover tooltips name
  the verdict.
- Element ids carry a hash of the badge's content, so badges inlined into
  one HTML page never resolve each other's gradient or clip path.
"""

import hashlib
import html
import math
import re
import unicodedata
from collections import Counter
from collections.abc import Iterable, Mapping
from urllib.parse import quote

__all__ = [
    "BADGE_STYLES",
    "LABEL_MAX",
    "VERDICT_COLORS",
    "badge_addressable",
    "badge_markdown",
    "fleet_message",
    "job_verdict",
    "label_error",
    "render_badge",
    "text_width",
]

#: The accepted ``style`` values; the first is the default.
BADGE_STYLES = ("flat", "flat-square")

#: Longest label override a caller may pass, in characters.
LABEL_MAX = 64

# The shields.io palette, which readers already know from CI badges.
_GREEN = "#4c1"
_RED = "#e05d44"
_ORANGE = "#fe7d37"
_BLUE = "#007ec6"
_GRAY = "#9f9f9f"
_LABEL_COLOR = "#555"

#: The message color of each verdict :func:`job_verdict` returns.
VERDICT_COLORS: Mapping[str, str] = {
    "passing": _GREEN,
    "failing": _RED,
    "overdue": _ORANGE,
    "running": _BLUE,
    "paused": _GRAY,
    "disabled": _GRAY,
    "cancelled": _GRAY,
    "unknown": _GRAY,
    "pending": _GRAY,
}

#: Advance widths, in pixels at font size 11, of the printable ASCII
#: characters in DejaVu Sans, grouped by width.
_WIDTH_GROUPS = (
    (3.0, "'"),
    (3.1, "ijl"),
    (3.2, "IJ"),
    (3.5, " ,."),
    (3.7, "/:;\\|"),
    (3.9, "f"),
    (4.0, "-"),
    (4.3, "()[]t"),
    (4.4, "!"),
    (4.5, "r"),
    (5.1, '"'),
    (5.5, "*_`"),
    (5.7, "s"),
    (5.8, "?z"),
    (6.0, "c"),
    (6.1, "L"),
    (6.3, "F"),
    (6.4, "k"),
    (6.5, "vxy"),
    (6.6, "P"),
    (6.7, "TYao"),
    (6.8, "e"),
    (7.0, "$0123456789ESbdghnpqu{}"),
    (7.2, "K"),
    (7.5, "ABVXZ"),
    (7.6, "R"),
    (7.7, "C"),
    (8.1, "U"),
    (8.2, "N"),
    (8.3, "H"),
    (8.5, "DG"),
    (8.6, "&"),
    (8.7, "OQ"),
    (9.0, "w"),
    (9.2, "#+<=>^~"),
    (9.5, "M"),
    (10.5, "%"),
    (10.7, "m"),
    (10.9, "W"),
    (11.0, "@"),
)
_WIDTHS = {char: width for width, chars in _WIDTH_GROUPS for char in chars}

#: Width of a character outside the table: about one lowercase letter, or
#: a full em for an East Asian wide or fullwidth character.
_FALLBACK_WIDTH = 7.0
_WIDE_WIDTH = 11.0

#: Space between a text run and the edges of its half of the badge.
_PADDING = 5

_FONT = "Verdana,Geneva,DejaVu Sans,sans-serif"

#: Unicode categories a label override must not contain: control
#: characters, line and paragraph separators, and lone surrogates.
_LABEL_FORBIDDEN = frozenset({"Cc", "Zl", "Zp", "Cs"})

#: Unicode categories that take no space of their own: nonspacing and
#: enclosing marks, and format characters such as a zero-width space.
_ZERO_WIDTH_CATEGORIES = frozenset({"Mn", "Me", "Cf"})

#: Format characters that draw a visible glyph, such as the Arabic number
#: signs.
_VISIBLE_FORMAT = frozenset(
    chr(code)
    for code in (
        *range(0x0600, 0x0606),
        0x06DD,
        0x070F,
        0x0890,
        0x0891,
        0x08E2,
        0x110BD,
        0x110CD,
        *range(0xFFF9, 0xFFFC),
    )
)

#: Emoji skin tone modifiers, which merge into the emoji before them.
_SKIN_TONES = range(0x1F3FB, 0x1F400)

_ZWJ = "\N{ZERO WIDTH JOINER}"
_VS16 = "\N{VARIATION SELECTOR-16}"

#: The line boundaries of :meth:`str.splitlines`, CRLF first so that it
#: counts as one.  The page's ``badgeMarkdown`` matches the same set.
_LINE_BREAK = re.compile(
    r"\r\n|[\n\v\f\r\x1c-\x1e\x85\N{LINE SEPARATOR}\N{PARAGRAPH SEPARATOR}]"
)

#: The characters in a badge's alt text that Markdown could read as
#: syntax: they could end the alt text, open a code span or emphasis,
#: split a table cell, or start HTML, an entity, or a GitHub footnote.  An
#: underscore between two letters or digits opens no emphasis, so it stays
#: bare.  The page's ``badgeMarkdown`` matches the same set.
_ALT_SYNTAX = re.compile(r"[\\\[\]`*|<&~^]|(?<![A-Za-z0-9])_|_(?![A-Za-z0-9])")


def job_verdict(
    *,
    enabled: bool,
    paused: bool,
    outcome: str | None,
    overdue: bool,
    running: bool,
) -> str:
    """The word a job's badge shows, most urgent state first.

    ``outcome`` is the outcome of the job's last run, or ``None`` when the
    job has never run.  A slot that a pause held back ran nothing, so it
    is not a run.  ``overdue`` is whether an SLA check is in breach.  A run
    in progress leaves a verdict in place, so the badge of a job that runs
    every minute keeps showing its last result.
    """
    if not enabled:
        return "disabled"
    if paused:
        return "paused"
    if outcome == "failure":
        return "failing"
    if overdue:
        return "overdue"
    if outcome == "success":
        return "passing"
    if outcome is None:
        return "running" if running else "pending"
    if outcome == "cancelled":
        return "cancelled"
    return "unknown"


def fleet_message(verdicts: Iterable[str]) -> tuple[str, str]:
    """The fleet badge's message and color, from every job's verdict.

    The message counts the most urgent of ``failing``, ``overdue``, and
    ``passing`` that any job shows, so ``2 failing`` outranks
    ``1 overdue``, which outranks ``12 passing``.  A fleet where no job
    shows one of the three reads ``no status``, and a fleet with no jobs
    reads ``no jobs``.
    """
    counts = Counter(verdicts)
    if not counts:
        return "no jobs", _GRAY
    for verdict in ("failing", "overdue", "passing"):
        count = counts[verdict]
        if count:
            return "{} {}".format(count, verdict), VERDICT_COLORS[verdict]
    return "no status", _GRAY


def label_error(label: str) -> str | None:
    """Why ``label`` cannot head a badge, or ``None`` when it can.

    A label of only spaces and zero-width characters, such as a zero-width
    space or a combining mark, counts as blank.
    """
    if all(char.isspace() or _zero_width(char) for char in label):
        return "label must not be blank"
    if len(label) > LABEL_MAX:
        return "label must be at most {} characters".format(LABEL_MAX)
    if any(unicodedata.category(char) in _LABEL_FORBIDDEN for char in label):
        return "label must not contain control characters or line breaks"
    return None


def badge_addressable(name: str) -> bool:
    """Whether a URL can reach the badge of the job called ``name``.

    A browser resolves the path segments ``.`` and ``..`` before it sends
    the request, an empty segment matches no route, and the route's
    ``{name}`` segment matches no ``{`` or ``}``.
    """
    return name not in ("", ".", "..") and "{" not in name and "}" not in name


def badge_markdown(
    base_url: str, name: str | None = None, link: bool = True
) -> str:
    """Markdown that embeds a status badge and links it to the dashboard.

    ``base_url`` is the daemon's web address, such as
    ``https://cron.example.com``.  ``name`` picks one job's badge, which
    links to that job's drawer, and ``None`` picks the fleet badge, which
    links to the dashboard.  ``link=False`` leaves the link out, for a
    daemon that serves no dashboard.  The web dashboard's ``badgeMarkdown``
    builds the same text.
    """
    # an unbalanced "(" or ")" in a path prefix would end the link early
    base = base_url.rstrip("/").replace("(", "%28").replace(")", "%29")
    if name is None:
        alt, image, target = "cronstable", "/badge.svg", "/"
    else:
        # safe="" also encodes "/", and the "(" and ")" that would end a
        # Markdown link early
        quoted = quote(name, safe="")
        alt = name
        image = "/jobs/{}/badge.svg".format(quoted)
        target = "/#job/{}".format(quoted)
    # a line break in a job name could end the paragraph
    alt = _LINE_BREAK.sub(" ", alt)
    alt = _ALT_SYNTAX.sub(r"\\\g<0>", alt)
    embed = "![{}]({}{})".format(alt, base, image)
    if not link:
        return embed
    return "[{}]({}{})".format(embed, base, target)


def _zero_width(char: str) -> bool:
    """Whether ``char`` takes no space of its own."""
    if char in _VISIBLE_FORMAT:
        return False
    return (
        unicodedata.category(char) in _ZERO_WIDTH_CATEGORIES
        or unicodedata.combining(char) != 0
    )


def text_width(text: str) -> float:
    """The estimated rendered width of ``text``, in pixels at size 11.

    Zero-width characters add nothing.  Neither does a skin tone after an
    emoji, or an emoji that a zero-width joiner merges into the one before
    it.  An emoji presentation selector widens a text-style emoji to a full
    em.
    """
    total = 0.0
    prev, prev_width = "", 0.0
    for char in text:
        width = _WIDTHS.get(char)
        if width is None:
            emoji_before = bool(prev) and unicodedata.category(prev) == "So"
            if _zero_width(char):
                width = 0.0
                if (
                    char == _VS16
                    and emoji_before
                    and prev_width == _FALLBACK_WIDTH
                ):
                    width = _WIDE_WIDTH - _FALLBACK_WIDTH
            elif emoji_before and ord(char) in _SKIN_TONES:
                width = 0.0
            elif prev == _ZWJ and unicodedata.category(char) == "So":
                width = 0.0
            elif unicodedata.east_asian_width(char) in ("W", "F"):
                width = _WIDE_WIDTH
            else:
                width = _FALLBACK_WIDTH
        total += width
        prev, prev_width = char, width
    return total


def _xml_char(char: str) -> str:
    """``char``, or U+FFFD when XML 1.0 forbids it in a document."""
    code = ord(char)
    if (
        code in (0x9, 0xA, 0xD)
        or 0x20 <= code <= 0xD7FF
        or 0xE000 <= code <= 0xFFFD
        or code >= 0x10000
    ):
        return char
    return "\ufffd"


def render_badge(
    label: str, message: str, color: str, style: str = "flat"
) -> str:
    """The SVG text of a badge that reads ``label`` beside ``message``.

    ``color`` fills the message half.  ``style`` is ``flat`` (rounded
    corners and a soft gradient) or ``flat-square`` (square corners, no
    gradient); any other value raises :class:`ValueError`.
    """
    if style not in BADGE_STYLES:
        raise ValueError("unknown badge style {!r}".format(style))
    label = "".join(_xml_char(char) for char in label)
    message = "".join(_xml_char(char) for char in message)
    uid = hashlib.sha256(
        "\0".join((label, message, color, style)).encode("utf-8")
    ).hexdigest()[:12]
    # Text sits at font size 110 under scale(.1), so every later coordinate
    # is in tenths of a pixel: the shields.io idiom for sub-pixel centering
    # in integer attributes.
    label_text = math.ceil(text_width(label))
    message_text = math.ceil(text_width(message))
    label_width = label_text + 2 * _PADDING
    message_width = message_text + 2 * _PADDING
    width = label_width + message_width
    flat = style == "flat"
    title = html.escape(label + ": " + message)
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="20" '
        'role="img" aria-label="{}">'.format(width, title),
        "<title>{}</title>".format(title),
    ]
    if flat:
        parts.append(
            '<linearGradient id="s-{uid}" x2="0" y2="100%">'
            '<stop offset="0" stop-color="#bbb" stop-opacity=".1"/>'
            '<stop offset="1" stop-opacity=".1"/>'
            "</linearGradient>"
            '<clipPath id="r-{uid}"><rect width="{width}" height="20" '
            'rx="3" fill="#fff"/></clipPath>'
            '<g clip-path="url(#r-{uid})">'.format(uid=uid, width=width)
        )
    else:
        parts.append('<g shape-rendering="crispEdges">')
    parts.append(
        '<rect width="{}" height="20" fill="{}"/>'
        '<rect x="{}" width="{}" height="20" fill="{}"/>'.format(
            label_width,
            _LABEL_COLOR,
            label_width,
            message_width,
            html.escape(color),
        )
    )
    if flat:
        parts.append(
            '<rect width="{}" height="20" fill="url(#s-{})"/>'.format(
                width, uid
            )
        )
    parts.append(
        '</g><g fill="#fff" text-anchor="middle" font-family="{}" '
        'text-rendering="geometricPrecision" font-size="110">'.format(_FONT)
    )
    runs = (
        (label, label_width * 5, label_text * 10),
        (message, label_width * 10 + message_width * 5, message_text * 10),
    )
    for text, center, length in runs:
        safe = html.escape(text)
        if flat:
            # the one-pixel drop shadow that lifts the text off the fill
            parts.append(
                '<text aria-hidden="true" x="{}" y="150" fill="#010101" '
                'fill-opacity=".3" transform="scale(.1)" '
                'textLength="{}">{}</text>'.format(center, length, safe)
            )
        parts.append(
            '<text x="{}" y="140" transform="scale(.1)" '
            'textLength="{}">{}</text>'.format(center, length, safe)
        )
    parts.append("</g></svg>")
    return "".join(parts)
