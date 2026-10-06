"""Tests for the QR Code encoder (cronstable.qr).

The web dashboard draws its pairing code with the JavaScript encoder bundled
in ``cronstable/web/index.html``; the terminal clients draw the same link
with :mod:`cronstable.qr`. ``tests/data/qr_golden.json`` records what the
bundled encoder produces for every version and error correction level, and
the replay here asserts the Python encoder produces the same modules. The
vectors are committed, so the suite needs no Node.js;
``tests/gen_qr_golden.py`` regenerates them.

The rest covers what the vectors cannot: mask selection, which the two
encoders score differently, and the terminal rendering.
"""

import hashlib
import json
import os

import pytest

from cronstable import qr

HERE = os.path.dirname(os.path.abspath(__file__))
GOLDEN = os.path.join(HERE, "data", "qr_golden.json")
PAGE = os.path.join(os.path.dirname(HERE), "cronstable", "web", "index.html")


def golden_data(length: int) -> bytes:
    """The fixed input of ``length`` bytes that a golden vector encodes.

    Shared with ``tests/gen_qr_golden.py``. Every byte value occurs, and
    no two lengths share a prefix.
    """
    return bytes((i * 167 + length * 13 + 1) % 256 for i in range(length))


def modules_digest(matrix) -> str:
    """SHA-256 of a symbol's modules, as rows of ``0`` and ``1``."""
    text = "\n".join(
        "".join("1" if cell else "0" for cell in row) for row in matrix
    )
    return hashlib.sha256(text.encode("ascii")).hexdigest()


def _golden():
    with open(GOLDEN, encoding="utf-8") as fh:
        return json.load(fh)


def _art(*rows):
    """A matrix from rows of ``#`` (dark) and ``.`` (light)."""
    return [[cell == "#" for cell in row] for row in rows]


# ---------------------------------------------------------------------------
# golden replay: the dashboard's encoder
# ---------------------------------------------------------------------------


def test_golden_vectors_cover_every_version_and_level():
    vectors = _golden()["vectors"]
    seen = {(v["level"], v["version"]) for v in vectors}
    assert seen == {
        (level, version)
        for level in qr.LEVELS
        for version in range(1, qr.MAX_VERSION + 1)
    }


def test_golden_vectors_describe_the_bundled_encoder():
    """A changed encoder in the page needs regenerated vectors."""
    with open(PAGE, encoding="utf-8") as fh:
        page = fh.read()
    marker = page.index(
        "QR Code Generator for JavaScript (vendored, verbatim)"
    )
    start = page.rindex("<script>", 0, marker) + len("<script>")
    block = page[start : page.index("</script>", marker)]
    digest = hashlib.sha256(block.encode("utf-8")).hexdigest()
    assert digest == _golden()["encoder_sha256"], (
        "the QR encoder in cronstable/web/index.html changed; run "
        "python tests/gen_qr_golden.py"
    )


@pytest.mark.parametrize("level", list(qr.LEVELS))
def test_encode_matches_the_dashboards_encoder(level):
    vectors = [v for v in _golden()["vectors"] if v["level"] == level]
    assert len(vectors) == 2 * qr.MAX_VERSION
    for vector in vectors:
        matrix = qr.encode(
            golden_data(vector["length"]), level, vector["mask"]
        )
        assert len(matrix) == qr.symbol_size(vector["version"]), vector
        assert modules_digest(matrix) == vector["sha256"], vector


# ---------------------------------------------------------------------------
# symbol structure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "version, level, capacity",
    [
        (1, "L", 17),
        (1, "M", 14),
        (1, "Q", 11),
        (1, "H", 7),
        (9, "L", 230),
        (10, "L", 271),
        (10, "M", 213),
        (40, "L", 2953),
        (40, "H", 1273),
    ],
)
def test_data_capacity_matches_the_standards_table(version, level, capacity):
    assert qr.data_capacity(version, level) == capacity


def test_data_module_count_matches_the_drawn_function_patterns():
    for version in range(1, qr.MAX_VERSION + 1):
        _dark, fixed = qr._function_patterns(version)
        free = sum(not cell for row in fixed for cell in row)
        assert free == qr._data_modules(version), version


def test_encode_picks_the_smallest_version_that_fits():
    assert len(qr.encode(b"x" * 17, "L")) == qr.symbol_size(1)
    assert len(qr.encode(b"x" * 18, "L")) == qr.symbol_size(2)
    # the character count widens to 16 bits at version 10
    assert len(qr.encode(b"x" * 230, "L")) == qr.symbol_size(9)
    assert len(qr.encode(b"x" * 231, "L")) == qr.symbol_size(10)


def test_encode_draws_the_three_finder_patterns():
    matrix = qr.encode(b"cronstable", "M")
    size = len(matrix)
    finder = _art(
        "#######",
        "#.....#",
        "#.###.#",
        "#.###.#",
        "#.###.#",
        "#.....#",
        "#######",
    )
    for top, left in ((0, 0), (0, size - 7), (size - 7, 0)):
        assert [row[left : left + 7] for row in matrix[top : top + 7]] == (
            finder
        )
    # the bottom-right corner holds data, never a fourth finder
    assert [row[size - 7 :] for row in matrix[size - 7 :]] != finder


def test_encode_rejects_unknown_level_and_oversized_data():
    with pytest.raises(ValueError, match="unknown error correction level"):
        qr.encode(b"x", "Z")
    with pytest.raises(ValueError, match="2954 bytes exceed the 2953"):
        qr.encode(b"x" * 2954, "L")


def test_format_information_survives_a_single_copy():
    """Both copies carry the same 15 bits: level, mask, and check bits."""
    size = qr.symbol_size(1)
    for level, level_bits in qr.LEVELS.items():
        for mask in range(8):
            matrix = qr.encode(b"x", level, mask)
            first = [matrix[i if i < 6 else i + 1][8] for i in range(8)] + [
                matrix[8][j if j < 6 else j + 1] for j in range(6, -1, -1)
            ]
            second = [matrix[8][size - 1 - i] for i in range(8)] + [
                matrix[size - 7 + i][8] for i in range(7)
            ]
            assert first == second
            bits = sum(bit << i for i, bit in enumerate(first)) ^ 0x5412
            assert bits >> 10 == level_bits << 3 | mask


# ---------------------------------------------------------------------------
# mask selection
# ---------------------------------------------------------------------------


def test_penalty_scores_runs_blocks_and_balance():
    # An all-light 5x5: ten runs of five (3 each), sixteen 2x2 blocks
    # (3 each), and a 50-point imbalance (10 per full 5%).
    blank = _art(".....", ".....", ".....", ".....", ".....")
    assert qr._penalty(blank) == 10 * 3 + 16 * 3 + 10 * 10
    # A checkerboard has no run, no block, and 13 dark modules of 25:
    # 2% off balance, under the first 5% step.
    board = _art("#.#.#", ".#.#.", "#.#.#", ".#.#.", "#.#.#")
    assert qr._penalty(board) == 0


@pytest.mark.parametrize("pattern", ["#.###.#....", "....#.###.#"])
def test_penalty_scores_a_finder_like_run(pattern):
    # An 11x11 checkerboard scores 0. Writing a finder's 1:1:3:1:1 beside
    # four light modules into its first row creates no run of five, no 2x2
    # block, and no 5% imbalance, so the 40 points are the pattern's alone.
    board = [[(r + c) % 2 == 0 for c in range(11)] for r in range(11)]
    assert qr._penalty(board) == 0
    board[0] = [cell == "#" for cell in pattern]
    assert qr._penalty(board) == 40
    # the rule reads columns as it reads rows
    assert qr._penalty([list(col) for col in zip(*board, strict=True)]) == 40


@pytest.mark.parametrize("data", [b"A", b"cronstable", bytes(range(64))])
def test_default_mask_has_the_lowest_penalty(data):
    chosen = qr.encode(data, "M")
    candidates = [qr.encode(data, "M", mask) for mask in range(8)]
    scores = [qr._penalty(candidate) for candidate in candidates]
    assert chosen == candidates[scores.index(min(scores))]


def test_masks_differ_only_in_data_and_format_modules():
    first, second = (qr.encode(b"mask", "M", mask) for mask in (0, 1))
    assert first != second
    size = len(first)
    _dark, fixed = qr._function_patterns((size - 17) // 4)
    for r in range(size):
        for c in range(size):
            # row 8 and column 8 hold the format information
            if fixed[r][c] and 8 not in (r, c):
                assert first[r][c] == second[r][c], (r, c)


# ---------------------------------------------------------------------------
# terminal rendering
# ---------------------------------------------------------------------------


def test_half_block_rows_pair_module_rows_into_glyphs():
    matrix = _art("##", ".#")
    assert qr.half_block_rows(matrix, quiet=0) == ["▀█"]
    assert qr.half_block_rows(_art(".#", "##"), quiet=0) == ["▄█"]
    # an odd height leaves the last text row's lower half light
    assert qr.half_block_rows(_art("#..", "#..", ".#."), quiet=0) == [
        "█  ",
        " ▀ ",
    ]


def test_half_block_rows_add_a_light_margin_on_every_side():
    matrix = qr.encode(b"margin", "L")
    size = len(matrix)
    for quiet in (1, 2, 4):
        rows = qr.half_block_rows(matrix, quiet)
        assert {len(row) for row in rows} == {size + 2 * quiet}
        assert len(rows) == (size + 2 * quiet + 1) // 2
        # the margin columns are blank, and so are the module rows above
        # the symbol: the first text row shows at most lower halves
        assert all(row[:quiet].strip() == "" for row in rows)
        assert all(row[-quiet:].strip() == "" for row in rows)
        assert rows[0].strip(" ▄") == ""


def test_half_block_rows_default_to_the_standard_quiet_zone():
    matrix = qr.encode(b"x", "L")
    assert len(qr.half_block_rows(matrix)[0]) == len(matrix) + 8


def test_fit_half_blocks_narrows_the_quiet_zone_to_fit():
    matrix = qr.encode(b"x", "L")  # version 1: 21 modules
    assert len(qr.fit_half_blocks(matrix, 80, 40)[0]) == 29
    # 15 lines hold 29 module rows; 14 hold 27
    assert len(qr.fit_half_blocks(matrix, 80, 15)[0]) == 29
    assert len(qr.fit_half_blocks(matrix, 80, 14)[0]) == 27
    assert len(qr.fit_half_blocks(matrix, 26, 40)[0]) == 25
    assert len(qr.fit_half_blocks(matrix, 23, 12)[0]) == 23
    assert qr.fit_half_blocks(matrix, 22, 40) is None
    assert qr.fit_half_blocks(matrix, 80, 11) is None


def test_smallest_fit_is_the_size_fit_half_blocks_accepts():
    matrix = qr.encode(b"x", "L")
    cols, lines = qr.smallest_fit(matrix)
    assert (cols, lines) == (23, 12)
    assert qr.fit_half_blocks(matrix, cols, lines) is not None
    assert qr.fit_half_blocks(matrix, cols - 1, lines) is None
    assert qr.fit_half_blocks(matrix, cols, lines - 1) is None


def test_too_small_names_the_size_the_symbol_needs_with_its_frame():
    matrix = qr.encode(b"x", "L")
    assert qr.too_small(matrix, 20, 10) == (
        "The code needs a terminal of at least 23 columns by 12 lines, and "
        "this one is 20 by 10. Enlarge the window or reduce the font size."
    )
    # the caller's frame adds to what the symbol needs
    framed = qr.too_small(matrix, 20, 10, (8, 4))
    assert "at least 31 columns by 16 lines" in framed
    assert "this one is 20 by 10" in framed


def test_ink_on_paper_is_black_on_white_from_the_color_cube():
    assert qr.INK_ON_PAPER == "\x1b[38;5;16;48;5;231m"
    assert qr.SGR_RESET == "\x1b[0m"


def test_encode_for_screen_is_the_smallest_symbol_for_the_utf8_text():
    text = "https://relay.example.test/pair#büro"
    assert qr.encode_for_screen(text) == qr.encode(text.encode("utf-8"), "L")
    # a higher correction level needs a larger symbol for the same text
    long = "x" * 100
    assert len(qr.encode_for_screen(long)) < len(qr.encode(long.encode(), "H"))
