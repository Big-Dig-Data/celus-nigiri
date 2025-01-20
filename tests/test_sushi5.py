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

    @pytest.mark.parametrize(
        "env_var,in_start_date,in_end_date,begin_date_param,end_date_param",
        (
            (None, "2020-01-01", "2020-01-01", "2020-01-01", "2020-01-31"),
            ("0", "2020-01-01", "2020-01-01", "2020-01-01", "2020-01-31"),
            ("1", "2020-01-01", "2020-01-01", "2020-01", "2020-01"),
            ("0", "2020-02-01", "2020-02-29", "2020-02-01", "2020-02-29"),
            ("0", "2020-02-01", "2020-02-01", "2020-02-01", "2020-02-29"),
            (None, "2020-01", "2020-01", "2020-01-01", "2020-01-31"),
            ("0", "2020-01", "2020-01", "2020-01-01", "2020-01-31"),
            ("1", "2020-01", "2020-01", "2020-01", "2020-01"),
            ("1", "2020-02", "2020-02", "2020-02", "2020-02"),
            (None, date(2020, 1, 1), date(2020, 1, 1), "2020-01-01", "2020-01-31"),
            ("0", date(2020, 1, 1), date(2020, 1, 1), "2020-01-01", "2020-01-31"),
            ("1", date(2020, 1, 1), date(2020, 1, 1), "2020-01", "2020-01"),
            (None, datetime(2020, 1, 1), datetime(2020, 1, 1), "2020-01-01", "2020-01-31"),
            ("0", datetime(2020, 1, 1), datetime(2020, 1, 1), "2020-01-01", "2020-01-31"),
            ("1", datetime(2020, 1, 1), datetime(2020, 1, 1), "2020-01", "2020-01"),
        ),
    )
    def test_data_formats(
        self,
        env_var,
        in_start_date,
        in_end_date,
        begin_date_param,
        end_date_param,
        monkeypatch,
        responses,
    ):
        url = "http://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "counter5_tr_test1.json", "r").read()

        if env_var:
            monkeypatch.setenv("NIGIRI_SHORT_DATE_FORMAT", env_var)

        def callback(request):
            assert request.params["begin_date"] == begin_date_param
            assert request.params["end_date"] == end_date_param
            return (200, {}, content)

        responses.add_callback(
            responses.GET,
            url_re,
            callback=callback,
        )

        client = Sushi5Client(url, "foo")
        buffer = BytesIO()
        client.get_report_data("tr", in_start_date, in_end_date, output_content=buffer)
