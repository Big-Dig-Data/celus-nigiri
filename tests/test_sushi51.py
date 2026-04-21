import json
import re
from datetime import date, datetime
from io import BytesIO
from pathlib import Path

import pytest

from celus_nigiri.client import Sushi51Client
from celus_nigiri.counter5 import CounterError, TransportError
from celus_nigiri.exceptions import SushiException


class TestSushi51:
    data_dir = Path(__file__).parent / "data/counter51/"

    @pytest.mark.parametrize(
        "report_type,filename,record_found,exception,strict",
        (
            ("dr", "DR_sample_r51.json", True, False, True),
            ("dr", "DR_sample_r51.json", True, False, False),
            ("dr", "DR_sample_r51_wrong_months_all.json", None, True, True),
            ("dr", "DR_sample_r51_wrong_months_all.json", False, False, False),
            ("dr", "DR_sample_r51_wrong_months_some.json", None, True, True),
            ("dr", "DR_sample_r51_wrong_months_some.json", True, False, False),
        ),
    )
    def test_wrong_date(self, filename, report_type, record_found, exception, strict, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = (self.data_dir / filename).open().read()

        responses.get(
            url_re,
            body=content,
        )
        client = Sushi51Client(url, "foo")
        buffer = BytesIO()

        if exception:
            with pytest.raises(SushiException):
                client.get_report_data(
                    report_type, "2022-01", "2022-12", output_content=buffer, strict=strict
                )
        else:
            report = client.get_report_data(
                report_type, "2022-01", "2022-12", output_content=buffer, strict=strict
            )
            assert report.record_found == record_found

    @pytest.mark.parametrize(
        "rt,count",
        (
            ("DR", 3),
            ("IR", 7),
            ("PR", 1),
            ("TR", 11),
        ),
    )
    def test_successful_request(self, rt, count, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / f"{rt}_sample_r51.json", "r").read()

        responses.get(
            url_re,
            body=content,
        )

        client = Sushi51Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data(rt.lower(), "2022-01", "2022-12", output_content=buffer)
        records = list(report.fd_to_dicts(buffer)[1])
        assert len(records) == count, "RecordItems count in json"

    def test_request_400_with_data(self, responses):
        url = "mock://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_error_3000.json", "r").read()

        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")

        responses.get(
            url_re,
            body=content,
            status=400,
        )

        client = Sushi51Client(url, "foo")
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

        client = Sushi51Client(url, "foo")
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

        client = Sushi51Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)

        assert len(report.errors) == 1
        error = report.errors[0]
        assert isinstance(error, TransportError)
        assert error.http_status == 400
        assert report.http_status_code == 400

    def test_request_stringified_error(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "stringified_error.json", "r").read()

        responses.get(
            url_re,
            body=content,
            status=200,
        )
        client = Sushi51Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert report.errors[0].code == 2090
        assert report.http_status_code == 200

    def test_request_errors_in_a_list(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_errors.json", "r").read()

        responses.get(
            url_re,
            body=content,
            status=200,
        )
        client = Sushi51Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert report.http_status_code == 200
        assert len(report.warnings) == 2
        assert report.warnings[0].code == 1011
        assert report.warnings[1].code == 3060

    def test_request_500_with_sushi_exception(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_error_3000.json", "r").read()

        responses.get(
            url_re,
            body=content,
            status=500,
        )

        client = Sushi51Client(url, "foo")
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

        client = Sushi51Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert isinstance(report.errors[0], TransportError)
        assert report.http_status_code == 500

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
        content = open(self.data_dir / "TR_sample_r51.json", "r").read()
        date_params = []

        if long_date_format_first:
            monkeypatch.setenv("NIGIRI_LONG_DATE_FORMAT_FIRST", "1")

        def callback(request):
            date_params.append((request.params["begin_date"], request.params["end_date"]))
            if (
                len(request.params["begin_date"]) == 7 or len(request.params["end_date"]) == 7
            ) and fail_short:
                return 200, {}, json.dumps({"Code": 3020, "Message": "Invalid Date Arguments"})
            if (
                len(request.params["begin_date"]) == 10 or len(request.params["end_date"]) == 10
            ) and fail_long:
                return 200, {}, json.dumps({"Code": 3020, "Message": "Invalid Date Arguments"})
            return (200, {}, content)

        responses.add_callback(
            responses.GET,
            url_re,
            callback=callback,
        )

        client = Sushi51Client(url, "foo")
        buffer = BytesIO()
        response = client.get_report_data("tr", in_start_date, in_end_date, output_content=buffer)
        assert params == date_params
        assert response.url.endswith(f"&begin_date={params[-1][0]}&end_date={params[-1][1]}")

    @pytest.mark.parametrize(
        "in_url,out_url",
        (
            ("https://example.com/", "https://example.com/r51/reports/tr"),
            ("https://example.com/r51", "https://example.com/r51/reports/tr"),
            ("https://example.com/r51/", "https://example.com/r51/reports/tr"),
            ("https://example.com/r5/", "https://example.com/r5/r51/reports/tr"),
            ("https://example.com/r5", "https://example.com/r5/r51/reports/tr"),
        ),
    )
    def test_r51_r51(
        self,
        in_url,
        out_url,
        responses,
    ):
        url_re = re.compile(in_url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "TR_sample_r51.json", "r").read()

        def callback(request):
            assert request.url.startswith(f"{out_url}?")
            return (200, {}, content)

        responses.add_callback(
            responses.GET,
            url_re,
            callback=callback,
        )

        client = Sushi51Client(in_url, "foo")
        buffer = BytesIO()
        client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)

    @pytest.mark.parametrize(
        "in_url,out_url",
        (
            ("https://example.com/", "https://example.com/r51/reports"),
            ("https://example.com/r51", "https://example.com/r51/reports"),
            ("https://example.com/r51/", "https://example.com/r51/reports"),
            ("https://example.com/r5/", "https://example.com/r5/r51/reports"),
            ("https://example.com/r5", "https://example.com/r5/r51/reports"),
        ),
    )
    def test_make_download_url_no_report_type(self, in_url, out_url):
        client = Sushi51Client(in_url, "foo")
        assert client.make_download_url() == out_url

    def test_get_reports_success(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "reports_r51.json", "r").read()

        responses.get(url_re, body=content)
        client = Sushi51Client(url, "foo")
        status_code, data = client.get_reports()
        assert status_code == 200
        assert isinstance(data, list)
        assert len(data) == 12

    def test_get_reports_non_json(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")

        responses.get(url_re, body="Internal Server Error", status=500)
        client = Sushi51Client(url, "foo")
        status_code, data = client.get_reports()
        assert status_code == 500
        assert data is None

    def test_get_reports_uses_reports_endpoint(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "reports_r51.json", "r").read()
        called_urls = []

        def callback(request):
            called_urls.append(request.url)
            return (200, {}, content)

        responses.add_callback(responses.GET, url_re, callback=callback)
        client = Sushi51Client(url, "foo")
        client.get_reports()
        assert len(called_urls) == 1
        assert "/r51/reports?" in called_urls[0]
        assert "/r51/reports/" not in called_urls[0]

    def test_short_format_bad_filter_dates_retries_with_long_format(self, responses, monkeypatch):
        """When short date format returns Report_Filters where Begin_Date==End_Date
        (provider sets end to first of month), the client retries with long date format."""
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        bad_content = open(self.data_dir / "DR_sample_r51_bad_filter_dates.json", "r").read()
        good_content = open(self.data_dir / "DR_sample_r51.json", "r").read()
        date_params = []

        def callback(request):
            params = (request.params["begin_date"], request.params["end_date"])
            date_params.append(params)
            if len(request.params["begin_date"]) == 7:
                return (200, {}, bad_content)
            return (200, {}, good_content)

        responses.add_callback(responses.GET, url_re, callback=callback)
        client = Sushi51Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("dr", "2022-01", "2022-01", output_content=buffer)
        assert report is not None
        assert date_params == [("2022-01", "2022-01"), ("2022-01-01", "2022-01-31")]

    def test_long_format_bad_filter_dates_does_not_retry(self, responses, monkeypatch):
        """When long date format is tried first (NIGIRI_LONG_DATE_FORMAT_FIRST=1),
        a response with Begin_Date==End_Date in filters does NOT trigger a retry
        because the skip check only applies to short format requests."""
        monkeypatch.setenv("NIGIRI_LONG_DATE_FORMAT_FIRST", "1")
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        bad_content = open(self.data_dir / "DR_sample_r51_bad_filter_dates.json", "r").read()
        date_params = []

        def callback(request):
            date_params.append((request.params["begin_date"], request.params["end_date"]))
            return (200, {}, bad_content)

        responses.add_callback(responses.GET, url_re, callback=callback)
        client = Sushi51Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data("dr", "2022-01", "2022-01", output_content=buffer)
        assert report is not None
        assert date_params == [("2022-01-01", "2022-01-31")]
