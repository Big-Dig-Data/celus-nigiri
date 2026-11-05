import calendar
import datetime
import re
from enum import StrEnum
from typing import Dict, List, Mapping, Optional, Tuple

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


class AntibotProvider(StrEnum):
    CLOUDFLARE = "cloudflare"

    @property
    def label(self) -> str:
        return {AntibotProvider.CLOUDFLARE: "Cloudflare"}[self]


# Markers which only appear in pages served by the anti-bot system itself (block pages
# and browser challenges), not in pages which merely use other services of the provider
# (e.g. Cloudflare CDN). Markers shared with the provider's ordinary error pages
# (e.g. Cloudflare's "cf-error-details" or "Cloudflare Ray ID" on a 522 timeout) must not
# be used - such a page means the server is down, not that the request was blocked.
ANTIBOT_MATCHERS: Dict[AntibotProvider, re.Pattern] = {
    AntibotProvider.CLOUDFLARE: re.compile(
        rb"attention required! \| cloudflare"
        rb"|sorry, you have been blocked"
        rb"|cf-browser-verification"
        rb"|_cf_chl_opt"
        rb"|challenges\.cloudflare\.com"
        rb"|/cdn-cgi/challenge-platform/",
        re.IGNORECASE,
    ),
}
ANTIBOT_DETECTION_SIZE = 64 * 1024


def detect_antibot(content: bytes) -> Optional[AntibotProvider]:
    """
    Returns the anti-bot provider if `content` looks like a page returned by an anti-bot
    system when it blocks the request or requires a browser challenge, None otherwise
    """
    content = content[:ANTIBOT_DETECTION_SIZE]
    for provider, matcher in ANTIBOT_MATCHERS.items():
        if matcher.search(content):
            return provider
    return None


# Response headers (name, value) which an anti-bot system sets on its own responses.
# Cloudflare sets `cf-mitigated: challenge` on all Challenge Page types, see
# https://developers.cloudflare.com/cloudflare-challenges/challenge-types/challenge-pages/detect-response/
ANTIBOT_HEADERS: Dict[AntibotProvider, Tuple[str, str]] = {
    AntibotProvider.CLOUDFLARE: ("cf-mitigated", "challenge"),
}


def detect_antibot_headers(headers: Mapping[str, str]) -> Optional[AntibotProvider]:
    """
    Returns the anti-bot provider if the response `headers` show that the response was
    produced by an anti-bot system (e.g. a browser challenge), None otherwise
    """
    normalized = {name.lower(): value.strip().lower() for name, value in headers.items()}
    for provider, (name, value) in ANTIBOT_HEADERS.items():
        if normalized.get(name) == value:
            return provider
    return None
