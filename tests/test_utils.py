from datetime import date

import pytest

from celus_nigiri.utils import parse_counter_month


@pytest.mark.parametrize(
    ["text", "output"],
    [
        ("Jun-2020", date(2020, 6, 1)),
        ("Jun-20", date(2020, 6, 1)),
        ("Jul-89", date(1989, 7, 1)),
        ("Apr-1234", date(1234, 4, 1)),
        ("Dec-2080", date(2080, 12, 1)),
        ("Foo-2020", None),
        ("adfjldskfj", None),
    ],
)
def test_parse_counter_month(text, output):
    assert parse_counter_month(text) == output
