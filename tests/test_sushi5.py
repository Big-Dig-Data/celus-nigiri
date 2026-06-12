import re
from datetime import date, datetime
from io import BytesIO
from pathlib import Path

import pytest
from requests import HTTPError

from celus_nigiri.client import Sushi5Client
from celus_nigiri.counter5 import CounterError, TransportError
from celus_nigiri.exceptions import SushiException


class TestSushi5:
    data_dir = Path(__file__).parent / "data/counter5/"

    @pytest.mark.parametrize(
        "report_type,filename,record_found,exception,strict",
        (
            ("dr", "DR_wrong_date_none.json", True, False, True),
            ("dr", "DR_wrong_date_none.json", True, False, False),
            ("dr", "DR_wrong_date_all.json", None, True, True),
            ("dr", "DR_wrong_date_all.json", False, False, False),
            ("dr", "DR_wrong_date_some.json", None, True, True),
            ("dr", "DR_wrong_date_some.json", True, False, False),
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
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        if exception:
            with pytest.raises(SushiException):
                client.get_report_data(
                    report_type, "2016-01", "2016-03", output_content=buffer, strict=strict
                )
        else:
            report = client.get_report_data(
                report_type, "2016-01", "2016-03", output_content=buffer, strict=strict
            )
            assert report.record_found == record_found

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
        report = client.get_report_data("tr", "2019-02", "2019-02", output_content=buffer)
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

    @pytest.mark.parametrize(
        "long_date_format,in_start_date,in_end_date,expected_begin,expected_end",
        (
            (False, "2019-02-01", "2019-02-01", "2019-02", "2019-02"),
            (False, "2019-02-01", "2019-02-28", "2019-02", "2019-02"),
            (False, "2019-02", "2019-02", "2019-02", "2019-02"),
            (False, date(2019, 2, 1), date(2019, 2, 1), "2019-02", "2019-02"),
            (False, datetime(2019, 2, 1), datetime(2019, 2, 1), "2019-02", "2019-02"),
            (True, "2019-02-01", "2019-02-01", "2019-02-01", "2019-02-28"),
            (True, "2019-02-01", "2019-02-28", "2019-02-01", "2019-02-28"),
            (True, "2019-02", "2019-02", "2019-02-01", "2019-02-28"),
            (True, date(2019, 2, 1), date(2019, 2, 1), "2019-02-01", "2019-02-28"),
            (True, datetime(2019, 2, 1), datetime(2019, 2, 1), "2019-02-01", "2019-02-28"),
        ),
    )
    def test_data_formats(
        self,
        long_date_format,
        in_start_date,
        in_end_date,
        expected_begin,
        expected_end,
        responses,
    ):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "counter5_tr_test1.json", "r").read()
        date_params = []

        def callback(request):
            date_params.append((request.params["begin_date"], request.params["end_date"]))
            return (200, {}, content)

        responses.add_callback(
            responses.GET,
            url_re,
            callback=callback,
        )

        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        assert client.get_report_data(
            "tr",
            in_start_date,
            in_end_date,
            output_content=buffer,
            long_date_format=long_date_format,
        )
        assert date_params == [(expected_begin, expected_end)]

    def test_make_download_url_no_report_type(self):
        client = Sushi5Client("http://foo.bar.baz/", "foo")
        assert client.make_download_url() == "http://foo.bar.baz/reports"

    def test_get_reports_success(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "reports.json", "r").read()

        responses.get(url_re, body=content)
        client = Sushi5Client(url, "foo")
        status_code, data = client.get_reports()
        assert status_code == 200
        assert isinstance(data, list)
        assert len(data) == 12

    def test_get_reports_non_json(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")

        responses.get(url_re, body="Internal Server Error", status=500)
        client = Sushi5Client(url, "foo")
        status_code, data = client.get_reports()
        assert status_code == 500
        assert data is None

    def test_get_reports_uses_reports_endpoint(self, responses):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "reports.json", "r").read()
        called_urls = []

        def callback(request):
            called_urls.append(request.url)
            return (200, {}, content)

        responses.add_callback(responses.GET, url_re, callback=callback)
        client = Sushi5Client(url, "foo")
        client.get_reports()
        assert len(called_urls) == 1
        assert "/reports?" in called_urls[0]
        assert "/reports/" not in called_urls[0]

    def test_short_format_bad_filter_dates_raises_exception(self, responses):
        """When short date format returns Report_Filters where Begin_Date==End_Date
        (provider sets end to first of month), a SushiException is raised."""
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        bad_content = open(self.data_dir / "counter5_tr_bad_filter_dates.json", "r").read()
        date_params = []

        def callback(request):
            date_params.append((request.params["begin_date"], request.params["end_date"]))
            return (200, {}, bad_content)

        responses.add_callback(responses.GET, url_re, callback=callback)
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        with pytest.raises(SushiException):
            client.get_report_data("tr", "2019-02", "2019-02", output_content=buffer)
        assert date_params == [("2019-02", "2019-02")]

    def test_request_3xx_raises_http_error(self, responses):
        """An exotic status code outside the 2xx/4xx-5xx ranges (e.g. an unfollowed
        3xx) raises requests.HTTPError carrying the response and a clear message."""
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")

        responses.get(url_re, body="Multiple Choices", status=300)
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        with pytest.raises(HTTPError) as exc_info:
            client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert exc_info.value.response.status_code == 300
        assert "300" in str(exc_info.value)
        assert "Unexpected HTTP status" in str(exc_info.value)

    def test_long_format_bad_filter_dates_ignored(self, responses):
        """When long date format is used, a response with Begin_Date==End_Date in filters
        does not raise an exception - the check only applies to short format requests."""
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        bad_content = open(self.data_dir / "counter5_tr_bad_filter_dates.json", "r").read()
        date_params = []

        def callback(request):
            date_params.append((request.params["begin_date"], request.params["end_date"]))
            return (200, {}, bad_content)

        responses.add_callback(responses.GET, url_re, callback=callback)
        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        report = client.get_report_data(
            "tr", "2019-02", "2019-02", output_content=buffer, long_date_format=True
        )
        assert report is not None
        assert date_params == [("2019-02-01", "2019-02-28")]
