from datetime import date
from pathlib import Path

import pytest
from requests.structures import CaseInsensitiveDict

from celus_nigiri.utils import (
    AntibotProvider,
    detect_antibot,
    detect_antibot_headers,
    get_date_range,
    parse_counter_month,
)


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


CF = AntibotProvider.CLOUDFLARE


@pytest.mark.parametrize(
    ["content", "provider"],
    [
        (b"<title>Attention Required! | Cloudflare</title>", CF),
        (b"<h1>Sorry, you have been blocked</h1>", CF),
        # shared with Cloudflare's ordinary error pages (e.g. 522), so not a block on their own
        (b'<div id="cf-error-details">', None),
        (b"<span>Cloudflare Ray ID: <strong>8a1b2c3d4e5f6789</strong></span>", None),
        (b"<script>window._cf_chl_opt={cType: 'managed'};</script>", CF),
        (b'<script src="/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1">', CF),
        (b"", None),
        (b"Internal Server Error", None),
        (b"<html><body><h1>403 Forbidden</h1></body></html>", None),
        # page which only uses Cloudflare CDN is not a block
        (b'<script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.min.js"></script>', None),
    ],
)
def test_detect_antibot(content, provider):
    assert detect_antibot(content) is provider


@pytest.mark.parametrize(
    ["headers", "provider"],
    [
        ({"cf-mitigated": "challenge"}, CF),
        ({"CF-Mitigated": "Challenge"}, CF),
        ({"cf-mitigated": " challenge "}, CF),
        (CaseInsensitiveDict({"Cf-Mitigated": "challenge"}), CF),
        ({}, None),
        ({"cf-mitigated": "something-else"}, None),
        # Cloudflare CDN headers present on any response going through Cloudflare
        ({"cf-ray": "8a1b2c3d4e5f6789-PRG", "server": "cloudflare"}, None),
    ],
)
def test_detect_antibot_headers(headers, provider):
    assert detect_antibot_headers(headers) is provider


@pytest.mark.parametrize(
    ["filename", "provider"],
    [("blocked.html", CF), ("challenge.html", CF), ("error_522.html", None)],
)
def test_detect_antibot_pages(filename, provider):
    content = (Path(__file__).parent / "data/cloudflare" / filename).read_bytes()
    assert detect_antibot(content) is provider
