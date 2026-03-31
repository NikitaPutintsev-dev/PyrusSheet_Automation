import pytest

from pyrus_sheet_sync.columns import col_letters_to_index, index_to_col_letters


@pytest.mark.parametrize(
    "letter,idx",
    [("A", 0), ("B", 1), ("Z", 25), ("AA", 26), ("AB", 27)],
)
def test_col_roundtrip(letter: str, idx: int) -> None:
    assert col_letters_to_index(letter) == idx
    assert index_to_col_letters(idx) == letter
