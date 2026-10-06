"""QR Code encoder and terminal renderer for the pairing code.

``cronstable pair`` and the terminal dashboard draw the pairing link that
the web dashboard renders with its bundled JavaScript encoder. This module
is the Python counterpart, written against ISO/IEC 18004: byte mode,
versions 1 to 40, and the four error correction levels.

It imports only the standard library, so the clients that use it never load
aiohttp or the scheduler. ``tests/test_qr.py`` compares its symbols with the
ones the dashboard's encoder produces.
"""

from collections.abc import Callable, Sequence
from itertools import groupby

Matrix = list[list[bool]]

#: Error correction levels and their two format-information bits.
LEVELS = {"L": 1, "M": 0, "Q": 3, "H": 2}

MAX_VERSION = 40

#: Black on white from the 256-color cube. Terminal themes restyle colors 0
#: to 15 and leave the cube alone, so the symbol keeps its contrast on any
#: theme.
INK_ON_PAPER = "\x1b[38;5;16;48;5;231m"
SGR_RESET = "\x1b[0m"


def _by_version(row: str) -> tuple[int, ...]:
    """A table row of 40 numbers as a tuple indexed by version (1 to 40)."""
    return (0, *map(int, row.split()))


# ISO/IEC 18004 table 9. Each row lists versions 1 to 20, then 21 to 40.
# Error correction codewords in each block:
_EC_PER_BLOCK = {
    "L": _by_version(
        "7 10 15 20 26 18 20 24 30 18 20 24 26 30 22 24 28 30 28 28 "
        "28 28 30 30 26 28 30 30 30 30 30 30 30 30 30 30 30 30 30 30"
    ),
    "M": _by_version(
        "10 16 26 18 24 16 18 22 22 26 30 22 22 24 24 28 28 26 26 26 "
        "26 28 28 28 28 28 28 28 28 28 28 28 28 28 28 28 28 28 28 28"
    ),
    "Q": _by_version(
        "13 22 18 26 18 24 18 22 20 24 28 26 24 20 30 24 28 28 26 30 "
        "28 30 30 30 30 28 30 30 30 30 30 30 30 30 30 30 30 30 30 30"
    ),
    "H": _by_version(
        "17 28 22 16 22 28 26 26 24 28 24 28 22 24 24 30 28 28 26 28 "
        "30 24 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30"
    ),
}
# Number of error correction blocks:
_BLOCK_COUNT = {
    "L": _by_version(
        "1 1 1 1 1 2 2 2 2 4 4 4 4 4 6 6 6 6 7 8 "
        "8 9 9 10 12 12 12 13 14 15 16 17 18 19 19 20 21 22 24 25"
    ),
    "M": _by_version(
        "1 1 1 2 2 4 4 4 5 5 5 8 9 9 10 10 11 13 14 16 "
        "17 17 18 20 21 23 25 26 28 29 31 33 35 37 38 40 43 45 47 49"
    ),
    "Q": _by_version(
        "1 1 2 2 4 4 6 6 8 8 8 10 12 16 12 17 16 18 21 20 "
        "23 23 25 27 29 34 34 35 38 40 43 45 48 51 53 56 59 62 65 68"
    ),
    "H": _by_version(
        "1 1 2 4 4 4 5 6 8 8 11 11 16 16 18 16 19 21 25 25 "
        "25 34 30 32 35 37 40 42 45 48 51 54 57 60 63 66 70 74 77 81"
    ),
}

# The eight data mask conditions, each taking (row, column).
_MASKS: tuple[Callable[[int, int], bool], ...] = (
    lambda r, c: (r + c) % 2 == 0,
    lambda r, c: r % 2 == 0,
    lambda r, c: c % 3 == 0,
    lambda r, c: (r + c) % 3 == 0,
    lambda r, c: (r // 2 + c // 3) % 2 == 0,
    lambda r, c: r * c % 2 + r * c % 3 == 0,
    lambda r, c: (r * c % 2 + r * c % 3) % 2 == 0,
    lambda r, c: ((r + c) % 2 + r * c % 3) % 2 == 0,
)

# GF(256) antilog and log tables over the QR polynomial x^8+x^4+x^3+x^2+1.
# The antilog table repeats, so a sum of two logs indexes it directly.
_EXP = [0] * 510
_LOG = [0] * 256
_value = 1
for _power in range(255):
    _EXP[_power] = _EXP[_power + 255] = _value
    _LOG[_value] = _power
    _value <<= 1
    if _value & 0x100:
        _value ^= 0x11D
del _value, _power


def symbol_size(version: int) -> int:
    """Modules along one side of a ``version`` symbol."""
    return version * 4 + 17


def _alignment_centers(version: int) -> list[int]:
    """Row and column coordinates of the alignment pattern centers."""
    if version == 1:
        return []
    count = version // 7 + 2
    last = version * 4 + 10
    if version == 32:
        # The one version whose spacing the even-step rule does not give.
        step = 26
    else:
        step = -(-(last - 6) // (count - 1))
        step += step % 2
    return [6] + [last - i * step for i in range(count - 2, -1, -1)]


def _data_modules(version: int) -> int:
    """Modules left for codewords after the function patterns are drawn."""
    size = symbol_size(version)
    # Three finders with separators, both format copies with the dark
    # module, and the two timing lines between the finders.
    free = size * size - 192 - 31 - 2 * (size - 16)
    if version > 1:
        count = version // 7 + 2
        # Every center pair except the three finder corners holds a 5x5
        # pattern. The patterns on a timing line share five modules with it.
        free -= 25 * (count * count - 3) - 10 * (count - 2)
    if version >= 7:
        free -= 36
    return free


def data_capacity(version: int, level: str) -> int:
    """Bytes of byte-mode data a ``version`` symbol holds at ``level``."""
    codewords = (
        _data_modules(version) // 8
        - _EC_PER_BLOCK[level][version] * _BLOCK_COUNT[level][version]
    )
    # The mode indicator, character count, and terminator take 2 codewords
    # through version 9 and 3 from version 10.
    return codewords - (2 if version < 10 else 3)


def _rs_generator(degree: int) -> list[int]:
    """Coefficients of the Reed-Solomon generator, highest power first."""
    poly = [1]
    for power in range(degree):
        grown = poly + [0]
        for i, coeff in enumerate(poly):
            grown[i + 1] ^= _EXP[_LOG[coeff] + power]
        poly = grown
    return poly


def _rs_remainder(data: Sequence[int], generator: Sequence[int]) -> list[int]:
    """The error correction codewords for one block of ``data``."""
    degree = len(generator) - 1
    rem = [0] * degree
    for byte in data:
        factor = byte ^ rem[0]
        rem = rem[1:] + [0]
        if factor:
            shift = _LOG[factor]
            for i in range(degree):
                rem[i] ^= _EXP[_LOG[generator[i + 1]] + shift]
    return rem


def _codewords(data: bytes, version: int, level: str) -> list[int]:
    """The symbol's codeword sequence: interleaved data, then interleaved
    error correction."""
    total = _data_modules(version) // 8
    blocks = _BLOCK_COUNT[level][version]
    ec_len = _EC_PER_BLOCK[level][version]

    count_bits = 8 if version < 10 else 16
    # Mode indicator, character count, data, and the 4-bit terminator. The
    # header is 12 or 20 bits, so the terminator ends on a codeword boundary.
    bits = (0b0100 << count_bits | len(data)) << 8 * len(data)
    bits = (bits | int.from_bytes(data, "big")) << 4
    stream = list(bits.to_bytes(count_bits // 8 + 1 + len(data), "big"))
    pad = (0xEC, 0x11)
    stream += [
        pad[i % 2] for i in range(total - ec_len * blocks - len(stream))
    ]

    # The last (total % blocks) blocks carry one more data codeword.
    short_len = total // blocks - ec_len
    short_blocks = blocks - total % blocks
    generator = _rs_generator(ec_len)
    data_blocks: list[list[int]] = []
    ec_blocks: list[list[int]] = []
    start = 0
    for index in range(blocks):
        length = short_len + (index >= short_blocks)
        block = stream[start : start + length]
        start += length
        data_blocks.append(block)
        ec_blocks.append(_rs_remainder(block, generator))

    out: list[int] = []
    for i in range(short_len + 1):
        out += [block[i] for block in data_blocks if i < len(block)]
    for i in range(ec_len):
        out += [block[i] for block in ec_blocks]
    return out


def _function_patterns(version: int) -> tuple[Matrix, Matrix]:
    """A blank symbol: its modules, and which of them are function modules.

    The format information area is reserved here and filled for each mask
    by :func:`_draw_format`.
    """
    size = symbol_size(version)
    dark = [[False] * size for _ in range(size)]
    fixed = [[False] * size for _ in range(size)]

    def put(row: int, col: int, value: bool) -> None:
        dark[row][col] = value
        fixed[row][col] = True

    for i in range(size):
        put(6, i, i % 2 == 0)
        put(i, 6, i % 2 == 0)
    # Finder patterns with their separators: rings 2 and 4 are light.
    for center_row, center_col in ((3, 3), (3, size - 4), (size - 4, 3)):
        for row in range(center_row - 4, center_row + 5):
            for col in range(center_col - 4, center_col + 5):
                if 0 <= row < size and 0 <= col < size:
                    ring = max(abs(row - center_row), abs(col - center_col))
                    put(row, col, ring not in (2, 4))
    centers = _alignment_centers(version)
    last = len(centers) - 1
    for i, center_row in enumerate(centers):
        for j, center_col in enumerate(centers):
            if (i, j) in ((0, 0), (0, last), (last, 0)):
                continue  # a finder pattern occupies this corner
            for row in range(center_row - 2, center_row + 3):
                for col in range(center_col - 2, center_col + 3):
                    ring = max(abs(row - center_row), abs(col - center_col))
                    put(row, col, ring != 1)
    for i in range(9):
        fixed[8][i] = fixed[i][8] = True
    for i in range(8):
        fixed[8][size - 1 - i] = fixed[size - 1 - i][8] = True
    put(size - 8, 8, True)
    if version >= 7:
        # Version information: the version number and its BCH(18,6) check
        # bits, in a 6x3 block beside each of the two outer finders.
        rem = version
        for _ in range(12):
            rem = rem << 1 ^ (rem >> 11) * 0x1F25
        bits = version << 12 | rem
        for i in range(18):
            bit = bool(bits >> i & 1)
            put(i // 3, size - 11 + i % 3, bit)
            put(size - 11 + i % 3, i // 3, bit)
    return dark, fixed


def _place(dark: Matrix, fixed: Matrix, codewords: Sequence[int]) -> None:
    """Lay the codeword bits along the zigzag path from the bottom right.

    The path climbs and descends two-module columns and steps over the
    vertical timing line. Modules past the last codeword stay light.
    """
    size = len(dark)
    bit_count = len(codewords) * 8
    index = 0
    upward = True
    right = size - 1
    while right > 0:
        if right == 6:
            right -= 1
        for row in range(size - 1, -1, -1) if upward else range(size):
            for col in (right, right - 1):
                if fixed[row][col]:
                    continue
                if index < bit_count:
                    byte = codewords[index >> 3]
                    dark[row][col] = bool(byte >> (7 - (index & 7)) & 1)
                index += 1
        upward = not upward
        right -= 2


def _skip_timing(index: int) -> int:
    """Map a format bit position past the timing line at coordinate 6."""
    return index if index < 6 else index + 1


def _draw_format(dark: Matrix, level: str, mask: int) -> None:
    """Write both copies of the format information for ``mask``."""
    size = len(dark)
    data = LEVELS[level] << 3 | mask
    rem = data
    for _ in range(10):
        rem = rem << 1 ^ (rem >> 9) * 0x537
    bits = (data << 10 | rem) ^ 0x5412
    for i in range(15):
        bit = bool(bits >> i & 1)
        if i < 8:
            dark[_skip_timing(i)][8] = bit
            dark[8][size - 1 - i] = bit
        else:
            dark[8][_skip_timing(14 - i)] = bit
            dark[size - 15 + i][8] = bit


def _masked(dark: Matrix, fixed: Matrix, mask: int) -> Matrix:
    """A copy of the symbol with ``mask`` applied to its data modules."""
    condition = _MASKS[mask]
    return [
        [
            cell != (not fixed[r][c] and condition(r, c))
            for c, cell in enumerate(row)
        ]
        for r, row in enumerate(dark)
    ]


def _penalty(dark: Matrix) -> int:
    """The standard's four-part score for a masked symbol; lower is better."""
    size = len(dark)
    score = 0
    for line in dark + [list(column) for column in zip(*dark, strict=True)]:
        text = "".join("1" if cell else "0" for cell in line)
        # Runs of five or more modules of one color.
        for _, run in groupby(text):
            length = len(list(run))
            if length >= 5:
                score += length - 2
        # A finder's 1:1:3:1:1 signature beside four light modules.
        for pattern in ("10111010000", "00001011101"):
            at = text.find(pattern)
            while at >= 0:
                score += 40
                at = text.find(pattern, at + 1)
    # 2x2 blocks of one color.
    for r in range(size - 1):
        row, below = dark[r], dark[r + 1]
        for c in range(size - 1):
            if row[c] == row[c + 1] == below[c] == below[c + 1]:
                score += 3
    # Each full 5% of imbalance between dark and light modules.
    total = size * size
    darks = sum(map(sum, dark))
    score += 10 * (abs(darks * 20 - total * 10) // total)
    return score


def encode(data: bytes, level: str = "M", mask: int | None = None) -> Matrix:
    """Encode ``data`` in byte mode as a QR Code symbol.

    Returns rows of booleans, ``True`` for a dark module, in the smallest
    version that holds the data at ``level``. ``mask`` pins one of the
    eight mask patterns; by default the encoder picks the pattern with the
    lowest penalty score.

    Raises :exc:`ValueError` for an unknown level and for data longer than
    version 40 holds at that level.
    """
    if level not in LEVELS:
        raise ValueError("unknown error correction level {!r}".format(level))
    for version in range(1, MAX_VERSION + 1):
        if len(data) <= data_capacity(version, level):
            break
    else:
        raise ValueError(
            "{} bytes exceed the {} a QR Code holds at level {}".format(
                len(data), data_capacity(MAX_VERSION, level), level
            )
        )
    dark, fixed = _function_patterns(version)
    _place(dark, fixed, _codewords(data, version, level))
    best: Matrix = []
    best_score = -1
    for candidate in range(8) if mask is None else (mask,):
        symbol = _masked(dark, fixed, candidate)
        _draw_format(symbol, level, candidate)
        score = _penalty(symbol) if mask is None else 0
        if best_score < 0 or score < best_score:
            best, best_score = symbol, score
    return best


def encode_for_screen(text: str) -> Matrix:
    """Encode ``text`` as UTF-8 for a symbol that a screen shows.

    The lowest correction level gives the smallest symbol, and a screen
    has no print damage to correct.
    """
    return encode(text.encode("utf-8"), "L")


def half_block_rows(matrix: Matrix, quiet: int = 4) -> list[str]:
    """The symbol as text rows of half-block characters.

    Each text row carries two module rows, which keeps modules square in a
    terminal cell twice as tall as it is wide. A block marks a dark module,
    so paint the rows dark on light, for example with
    :data:`INK_ON_PAPER`. ``quiet`` is the light margin on every side, in
    modules; the standard asks for 4.
    """
    width = len(matrix) + 2 * quiet
    blank = [False] * width
    side = [False] * quiet
    rows = (
        [blank] * quiet
        + [side + row + side for row in matrix]
        + [blank] * quiet
    )
    if len(rows) % 2:
        rows.append(blank)
    # Indexed by (top module, bottom module).
    glyphs = " ▄▀█"
    return [
        "".join(
            glyphs[top * 2 + bottom]
            for top, bottom in zip(rows[i], rows[i + 1], strict=True)
        )
        for i in range(0, len(rows), 2)
    ]


#: The quiet zones :func:`fit_half_blocks` tries, in modules: the standard
#: 4 first, and the narrowest last.
QUIET_ZONES = (4, 3, 2, 1)


def fit_half_blocks(matrix: Matrix, cols: int, lines: int) -> list[str] | None:
    """:func:`half_block_rows` with the widest quiet zone that fits.

    Tries each of :data:`QUIET_ZONES` for a space of ``cols`` by ``lines``
    cells, and returns ``None`` when the symbol is too large for it even
    at the narrowest.
    """
    for quiet in QUIET_ZONES:
        width = len(matrix) + 2 * quiet
        if width <= cols and (width + 1) // 2 <= lines:
            return half_block_rows(matrix, quiet)
    return None


def smallest_fit(matrix: Matrix) -> tuple[int, int]:
    """The ``(cols, lines)`` :func:`fit_half_blocks` needs at the least."""
    width = len(matrix) + 2 * QUIET_ZONES[-1]
    return width, (width + 1) // 2


def too_small(
    matrix: Matrix, cols: int, lines: int, frame: tuple[int, int] = (0, 0)
) -> str:
    """What to tell the operator of a terminal too small for the symbol.

    ``cols`` and ``lines`` are the terminal's size. ``frame`` is the
    columns and lines that the caller draws around the symbol.
    """
    need_cols, need_lines = smallest_fit(matrix)
    return (
        "The code needs a terminal of at least {} columns by {} lines, and "
        "this one is {} by {}. Enlarge the window or reduce the font "
        "size.".format(
            need_cols + frame[0], need_lines + frame[1], cols, lines
        )
    )
