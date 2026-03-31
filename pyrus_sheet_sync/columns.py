"""A1 column letter helpers (0-based row indices for in-memory arrays)."""


def col_letters_to_index(col: str) -> int:
    col = col.strip().upper()
    if not col or not col.isalpha():
        raise ValueError(f"Invalid column letter: {col!r}")
    n = 0
    for c in col:
        n = n * 26 + (ord(c) - ord("A") + 1)
    return n - 1


def index_to_col_letters(index: int) -> str:
    if index < 0:
        raise ValueError("column index must be non-negative")
    letters: list[str] = []
    n = index + 1
    while n:
        n, r = divmod(n - 1, 26)
        letters.append(chr(ord("A") + r))
    return "".join(reversed(letters))


def max_column_index(*indices: int) -> int:
    return max(indices) if indices else 0
