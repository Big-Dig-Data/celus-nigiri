import csv
from datetime import date
from pathlib import Path

import pytest

from celus_nigiri import CounterRecord
from celus_nigiri.celus import custom_data_to_records


def test_custom_data_to_records_1():
    data = [
        {'Metric': 'M1', 'Jan 2019': 10, 'Feb 2019': 7, 'Mar 2019': 11},
        {'Metric': 'M2', 'Jan 2019': 1, 'Feb 2019': 2, 'Mar 2019': 3},
    ]
    records = [e for e in custom_data_to_records(data)]
    assert len(records) == 6
    for record in records:
        assert record.value in (1, 2, 3, 7, 10, 11)
        assert record.start in (date(2019, 1, 1), date(2019, 2, 1), date(2019, 3, 1))
        if record.value in (10, 7, 11):
            assert record.metric == 'M1'
            if record.start == date(2019, 1, 1):
                assert record.value == 10
        else:
            assert record.metric == 'M2'


def test_custom_data_to_records_with_column_map():
    data = [
        {'MetricXX': 'M1', 'Jan 2019': 10, 'Feb 2019': 7, 'Mar 2019': 11},
        {'MetricXX': 'M2', 'Jan 2019': 1, 'Feb 2019': 2, 'Mar 2019': 3},
    ]
    records = custom_data_to_records(data, column_map={'MetricXX': 'metric'})
    records = [e for e in records]  # convert generator to a list
    assert len(records) == 6
    for record in records:
        assert record.value in (1, 2, 3, 7, 10, 11)
        assert record.start in (date(2019, 1, 1), date(2019, 2, 1), date(2019, 3, 1))
        if record.value in (10, 7, 11):
            assert record.metric == 'M1'
            if record.start == date(2019, 1, 1):
                assert record.value == 10
        else:
            assert record.metric == 'M2'


def test_custom_data_to_records_no_metric():
    data = [
        {'Jan 2019': 10, 'Feb 2019': 7, 'Mar 2019': 11},
        {'Jan 2019': 1, 'Feb 2019': 2, 'Mar 2019': 3, 'Metric': 'MX'},
    ]
    records = custom_data_to_records(data, initial_data={'metric': 'MD'})
    records = [e for e in records]  # convert generator to a list
    assert len(records) == 6
    for record in records:
        assert record.value in (1, 2, 3, 7, 10, 11)
        assert record.start in (date(2019, 1, 1), date(2019, 2, 1), date(2019, 3, 1))
        if record.value in (10, 7, 11):
            assert record.metric == 'MD'
        else:
            assert record.metric == 'MX'


@pytest.mark.parametrize(
    ['filename', 'output'],
    [
        (
            'custom_data-2d-3x2x3-endate.csv',
            [
                CounterRecord(start=date(2019, 1, 1), title="Title 1", metric="Metric 1", value=1),
                CounterRecord(start=date(2019, 2, 1), title="Title 1", metric="Metric 1", value=4),
                CounterRecord(start=date(2019, 3, 1), title="Title 1", metric="Metric 1", value=7),
                CounterRecord(start=date(2019, 1, 1), title="Title 1", metric="Metric 2", value=10),
                CounterRecord(start=date(2019, 2, 1), title="Title 1", metric="Metric 2", value=20),
                CounterRecord(start=date(2019, 3, 1), title="Title 1", metric="Metric 2", value=30),
                CounterRecord(start=date(2019, 1, 1), title="Title 2", metric="Metric 1", value=2),
                CounterRecord(start=date(2019, 2, 1), title="Title 2", metric="Metric 1", value=5),
                CounterRecord(start=date(2019, 3, 1), title="Title 2", metric="Metric 1", value=8),
                CounterRecord(start=date(2019, 1, 1), title="Title 2", metric="Metric 2", value=20),
                CounterRecord(start=date(2019, 2, 1), title="Title 2", metric="Metric 2", value=30),
                CounterRecord(start=date(2019, 3, 1), title="Title 2", metric="Metric 2", value=40),
                CounterRecord(start=date(2019, 1, 1), title="Title 3", metric="Metric 1", value=3),
                CounterRecord(start=date(2019, 2, 1), title="Title 3", metric="Metric 1", value=6),
                CounterRecord(start=date(2019, 3, 1), title="Title 3", metric="Metric 1", value=9),
                CounterRecord(start=date(2019, 1, 1), title="Title 3", metric="Metric 2", value=30),
                CounterRecord(start=date(2019, 2, 1), title="Title 3", metric="Metric 2", value=40),
                CounterRecord(start=date(2019, 3, 1), title="Title 3", metric="Metric 2", value=50),
            ],
        ),
        (
            'custom_data-2d-3x2x3-isodate.csv',
            [
                CounterRecord(start=date(2019, 1, 1), title="Title 1", metric="Metric 1", value=1),
                CounterRecord(start=date(2019, 2, 1), title="Title 1", metric="Metric 1", value=4),
                CounterRecord(start=date(2019, 3, 1), title="Title 1", metric="Metric 1", value=7),
                CounterRecord(start=date(2019, 1, 1), title="Title 1", metric="Metric 2", value=10),
                CounterRecord(start=date(2019, 2, 1), title="Title 1", metric="Metric 2", value=20),
                CounterRecord(start=date(2019, 3, 1), title="Title 1", metric="Metric 2", value=30),
                CounterRecord(start=date(2019, 1, 1), title="Title 2", metric="Metric 1", value=2),
                CounterRecord(start=date(2019, 2, 1), title="Title 2", metric="Metric 1", value=5),
                CounterRecord(start=date(2019, 3, 1), title="Title 2", metric="Metric 1", value=8),
                CounterRecord(start=date(2019, 1, 1), title="Title 2", metric="Metric 2", value=20),
                CounterRecord(start=date(2019, 2, 1), title="Title 2", metric="Metric 2", value=30),
                CounterRecord(start=date(2019, 3, 1), title="Title 2", metric="Metric 2", value=40),
                CounterRecord(start=date(2019, 1, 1), title="Title 3", metric="Metric 1", value=3),
                CounterRecord(start=date(2019, 2, 1), title="Title 3", metric="Metric 1", value=6),
                CounterRecord(start=date(2019, 3, 1), title="Title 3", metric="Metric 1", value=9),
                CounterRecord(start=date(2019, 1, 1), title="Title 3", metric="Metric 2", value=30),
                CounterRecord(start=date(2019, 2, 1), title="Title 3", metric="Metric 2", value=40),
                CounterRecord(start=date(2019, 3, 1), title="Title 3", metric="Metric 2", value=50),
            ],
        ),
        (
            'custom_data-simple-3x3-endate.csv',
            [
                CounterRecord(start=date(2019, 1, 1), title="Title 1", value=1),
                CounterRecord(start=date(2019, 2, 1), title="Title 1", value=4),
                CounterRecord(start=date(2019, 3, 1), title="Title 1", value=7),
                CounterRecord(start=date(2019, 1, 1), title="Title 2", value=2),
                CounterRecord(start=date(2019, 2, 1), title="Title 2", value=5),
                CounterRecord(start=date(2019, 3, 1), title="Title 2", value=8),
                CounterRecord(start=date(2019, 1, 1), title="Title 3", value=3),
                CounterRecord(start=date(2019, 2, 1), title="Title 3", value=6),
                CounterRecord(start=date(2019, 3, 1), title="Title 3", value=9),
            ],
        ),
        (
            'custom_data-simple-3x3-isodate.csv',
            [
                CounterRecord(start=date(2019, 1, 1), title="Title 1", value=1),
                CounterRecord(start=date(2019, 2, 1), title="Title 1", value=4),
                CounterRecord(start=date(2019, 3, 1), title="Title 1", value=7),
                CounterRecord(start=date(2019, 1, 1), title="Title 2", value=2),
                CounterRecord(start=date(2019, 2, 1), title="Title 2", value=5),
                CounterRecord(start=date(2019, 3, 1), title="Title 2", value=8),
                CounterRecord(start=date(2019, 1, 1), title="Title 3", value=3),
                CounterRecord(start=date(2019, 2, 1), title="Title 3", value=6),
                CounterRecord(start=date(2019, 3, 1), title="Title 3", value=9),
            ],
        ),
    ],
)
def test_file_parsing(filename, output):
    path = Path(__file__).parent / 'data/custom' / filename
    with path.open() as f:
        reader = csv.DictReader(f)
        records = list(custom_data_to_records(reader))

    assert records == output
