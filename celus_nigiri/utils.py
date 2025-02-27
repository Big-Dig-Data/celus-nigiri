import calendar
import datetime
import re
from typing import List, Optional

import dateutil

counter_month_matcher = re.compile(r"^(?P<month>\w{3})-(?P<year>\d{2}(\d{2})?)$")


def parse_counter_month(text: str) -> Optional[datetime.date]:
    """
    Returns a date object extracted from date formatted according to the specification of COUNTER 5
    i. e. Mmm-YYYY. It also allows Mmm-YY to be more flexible
    :param text:
    :return:
    """
    if not text:
        return None
    m = counter_month_matcher.match(text)
    if m:
        year = int(m.group("year"))
        month = m.group("month")
        if year < 50:
            year += 2000
        elif 50 <= year < 100:
            year += 1900
        month_abbrs = list(calendar.month_abbr)
        if month not in month_abbrs:
            return None
        try:
            return datetime.date(year, month_abbrs.index(month), 1)
        except ValueError:
            return None


def parse_date_fuzzy(date_str: str) -> Optional[datetime.date]:
    """
    Uses dateutil.parser to try to parse a date in US format.
    """
    parser_info = dateutil.parser.parserinfo(dayfirst=False)  # US date formats 1/31/2020
    try:
        return dateutil.parser.parse(date_str, parser_info).date()
    except dateutil.parser.ParserError:
        return None


def get_date_range(start_date: datetime.date, end_date: datetime.date) -> List[datetime.date]:
    months = []
    start_date = start_date.replace(day=1)
    while start_date <= end_date:
        months.append(start_date)
        start_date = start_date + datetime.timedelta(days=45)
        start_date = start_date.replace(day=1)

    return months


def begin_month(in_date: datetime.date) -> datetime.date:
    return in_date.replace(day=1)


def end_month(in_date: datetime.date) -> datetime.date:
    last = calendar.monthrange(in_date.year, in_date.month)[1]
    return datetime.date(year=in_date.year, month=in_date.month, day=last)
