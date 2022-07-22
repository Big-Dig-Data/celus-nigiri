""" Celus format for non-counter data """
import typing
from datetime import date
from functools import lru_cache

from celus_nigiri import CounterRecord
from celus_nigiri.utils import parse_date_fuzzy

DEFAULT_COLUMN_MAP = {
    'Metric': 'metric',
    'Organization': 'organization',
    'Source': 'title',
    'Platform': 'platform_name',
    'Title': 'title',
}


@lru_cache
def col_name_to_month(row_name: str) -> typing.Optional[date]:
    """
    >>> col_name_to_month('Jan 2019')
    datetime.date(2019, 1, 1)
    >>> col_name_to_month('2019-02')
    datetime.date(2019, 2, 1)
    >>> col_name_to_month('prase') is None
    True
    """
    _date = parse_date_fuzzy(row_name)
    if _date:
        return _date.replace(day=1)
    return None


def custom_data_to_records(
    records: typing.Generator[dict, None, None], column_map=None, extra_dims=None, initial_data=None
) -> typing.Generator[CounterRecord, None, None]:
    # prepare the keyword arguments
    if initial_data is None:
        initial_data = {}
    if column_map is None:
        column_map = DEFAULT_COLUMN_MAP
    if extra_dims is None:
        extra_dims = []
    # process the records
    result = []
    for record in records:
        implicit_dimensions = {}
        explicit_dimensions = {}
        monthly_values = {}
        for key, value in record.items():
            month = col_name_to_month(key)
            if month:
                monthly_values[month] = int(value)
            else:
                if key in column_map:
                    implicit_dimensions[column_map[key]] = value
                elif key in extra_dims:
                    explicit_dimensions[key] = value
                else:
                    raise KeyError(f'We don\'t know how to interpret the column "{key}"')
        # we put initial data into the data we read - these are usually dimensions that are fixed
        # for the whole import and are not part of the data itself
        for key, value in initial_data.items():
            if key not in implicit_dimensions:
                implicit_dimensions[key] = value  # only update if the value is not present
        for month, value in monthly_values.items():
            result.append(
                CounterRecord(
                    value=value,
                    start=month,
                    dimension_data=explicit_dimensions,
                    **implicit_dimensions,
                )
            )
    return (e for e in result)  # TODO convert this into a propper generator
