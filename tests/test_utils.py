from datetime import date

import pytest

from celus_nigiri.utils import get_date_range, parse_counter_month


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


@pytest.mark.parametrize(
    ["begin", "end", "months"],
    [
        (date(2020, 1, 1), date(2019, 12, 1), []),
        (date(2020, 1, 1), date(2020, 1, 1), [date(2020, 1, 1)]),
        (date(2020, 1, 1), date(2020, 2, 1), [date(2020, 1, 1), date(2020, 2, 1)]),
        (date(2020, 1, 1), date(2020, 12, 1), [date(2020, i, 1) for i in range(1, 13)]),
        (
            date(2020, 1, 1),
            date(2021, 12, 1),
            [*(date(2020, i, 1) for i in range(1, 13)), *(date(2021, i, 1) for i in range(1, 13))],
        ),
    ],
)
def test_get_date_range(begin, end, months):
    assert get_date_range(begin, end) == months
