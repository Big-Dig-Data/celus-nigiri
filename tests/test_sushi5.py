import json
import re
from datetime import date, datetime
from io import BytesIO
from pathlib import Path

import pytest

from celus_nigiri.client import Sushi5Client
from celus_nigiri.counter5 import CounterError, TransportError


class TestSushi5:
    data_dir = Path(__file__).parent / "data/counter5/"

    def test_successful_request(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "counter5_tr_test1.json", "r").read()

        responses.get(
            url_re,
            body=content,
        )
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        records = list(report.fd_to_dicts(buffer)[1])
        assert len(records) == 3

    def test_request_400_with_data(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_error_3000.json", "r").read()

        responses.get(
            url_re,
            body=content,
            status=400,
        )
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert report.errors[0].code == 3000
        assert report.http_status_code == 400

    def test_request_exception_in_lowercase(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_error_lowercase.json", "r").read()

        responses.get(
            url_re,
            body=content,
        )
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert report.errors[0].code == 1001
        assert report.http_status_code == 200

    def test_request_400_without_data(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = "HTTP 400: Bad request"

        responses.get(
            url_re,
            body=content,
            status=400,
        )
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        error = report.errors[0]
        assert isinstance(error, TransportError)
        assert error.http_status == 400
        assert report.http_status_code == 400

    def test_request_500_with_sushi_exception(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_error_3000.json", "r").read()

        responses.get(
            url_re,
            body=content,
            status=500,
        )
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert isinstance(report.errors[0], CounterError)
        assert report.errors[0].code == 3000
        assert report.http_status_code == 500

    def test_request_500_plain_text(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")

        responses.get(
            url_re,
            body="Internal Server Error",
            status=500,
        )
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert isinstance(report.errors[0], TransportError)
        assert report.http_status_code == 500

    @pytest.mark.now
    @pytest.mark.parametrize(
        "fail_short,fail_long,long_date_format_first,in_start_date,in_end_date,params",
        (
            (True, False, True, "2020-01-01", "2020-01-01", [("2020-01-01", "2020-01-31")]),
            (
                False,
                True,
                True,
                "2020-01-01",
                "2020-01-01",
                [("2020-01-01", "2020-01-31"), ("2020-01", "2020-01")],
            ),
            (True, False, True, "2020-02-01", "2020-02-29", [("2020-02-01", "2020-02-29")]),
            (
                False,
                True,
                True,
                "2020-02-01",
                "2020-02-29",
                [("2020-02-01", "2020-02-29"), ("2020-02", "2020-02")],
            ),
            (True, False, True, "2020-01", "2020-01", [("2020-01-01", "2020-01-31")]),
            (
                False,
                True,
                True,
                "2020-01",
                "2020-01",
                [("2020-01-01", "2020-01-31"), ("2020-01", "2020-01")],
            ),
            (True, False, True, date(2020, 1, 1), date(2020, 1, 1), [("2020-01-01", "2020-01-31")]),
            (
                False,
                True,
                True,
                date(2020, 1, 1),
                date(2020, 1, 1),
                [("2020-01-01", "2020-01-31"), ("2020-01", "2020-01")],
            ),
            (
                True,
                False,
                True,
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                [("2020-01-01", "2020-01-31")],
            ),
            (
                False,
                True,
                True,
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                [("2020-01-01", "2020-01-31"), ("2020-01", "2020-01")],
            ),
            (
                True,
                False,
                False,
                "2020-01-01",
                "2020-01-01",
                [
                    ("2020-01", "2020-01"),
                    ("2020-01-01", "2020-01-31"),
                ],
            ),
            (
                False,
                True,
                False,
                "2020-01-01",
                "2020-01-01",
                [("2020-01", "2020-01")],
            ),
            (
                True,
                False,
                False,
                "2020-02-01",
                "2020-02-29",
                [("2020-02", "2020-02"), ("2020-02-01", "2020-02-29")],
            ),
            (
                False,
                True,
                False,
                "2020-02-01",
                "2020-02-29",
                [("2020-02", "2020-02")],
            ),
            (
                True,
                False,
                False,
                "2020-01",
                "2020-01",
                [("2020-01", "2020-01"), ("2020-01-01", "2020-01-31")],
            ),
            (
                False,
                True,
                False,
                "2020-01",
                "2020-01",
                [("2020-01", "2020-01")],
            ),
            (
                True,
                False,
                False,
                date(2020, 1, 1),
                date(2020, 1, 1),
                [("2020-01", "2020-01"), ("2020-01-01", "2020-01-31")],
            ),
            (
                False,
                True,
                False,
                date(2020, 1, 1),
                date(2020, 1, 1),
                [("2020-01", "2020-01")],
            ),
            (
                True,
                False,
                False,
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                [("2020-01", "2020-01"), ("2020-01-01", "2020-01-31")],
            ),
            (
                False,
                True,
                False,
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                [("2020-01", "2020-01")],
            ),
            (
                True,
                True,
                False,
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                [("2020-01", "2020-01") ,("2020-01-01", "2020-01-31")]
            ),
            (
                True,
                True,
                True,
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                [("2020-01-01", "2020-01-31"), ("2020-01", "2020-01")]
            ),
        ),
    )
    def test_data_formats(
        self,
        fail_short,
        fail_long,
        long_date_format_first,
        in_start_date,
        in_end_date,
        params,
        responses,
        monkeypatch,
    ):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "counter5_tr_test1.json", "r").read()
        date_params = []

        if long_date_format_first:
            monkeypatch.setenv("NIGIRI_LONG_DATE_FORMAT_FIRST", "1")

        def callback(request):
            date_params.append((request.params["begin_date"], request.params["end_date"]))
            if (
                len(request.params["begin_date"]) == 7 or len(request.params["end_date"]) == 7
            ) and fail_short:
                return 200, {}, json.dumps({"Code": "3020", "Message": "Invalid Date Arguments"})
            if (
                len(request.params["begin_date"]) == 10 or len(request.params["end_date"]) == 10
            ) and fail_long:
                return 200, {}, json.dumps({"Code": "3020", "Message": "Invalid Date Arguments"})
            return (200, {}, content)

        responses.add_callback(
            responses.GET,
            url_re,
            callback=callback,
        )

        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        assert client.get_report_data("tr", in_start_date, in_end_date, output_content=buffer)
        assert params == date_params
