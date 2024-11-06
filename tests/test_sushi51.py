import re
from io import BytesIO
from pathlib import Path

import pytest
import requests_mock

from celus_nigiri.client import Sushi51Client
from celus_nigiri.counter5 import CounterError, TransportError


class TestSushi51:
    data_dir = Path(__file__).parent / "data/counter51/"

    @pytest.mark.parametrize(
        "rt,count",
        (
            ("DR", 3),
            ("IR", 7),
            ("PR", 1),
            ("TR", 11),
        ),
    )
    def test_successful_request(self, rt, count):
        url = "mock://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / f"{rt}_sample_r51.json", "r").read()

        with requests_mock.Mocker() as mock:
            mock.register_uri("GET", url_re, text=content)
            client = Sushi51Client(url, "foo")
            buffer = BytesIO()
            report = client.get_report_data(rt.lower(), "2020-01", "2020-01", output_content=buffer)
        records = list(report.fd_to_dicts(buffer)[1])
        assert len(records) == count, "RecordItems count in json"

    def test_request_400_with_data(self):
        url = "mock://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_error_3000.json", "r").read()

        with requests_mock.Mocker() as mock:
            mock.register_uri("GET", url_re, text=content, status_code=400)
            client = Sushi51Client(url, "foo")
            buffer = BytesIO()
            report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert report.errors[0].code == 3000
        assert report.http_status_code == 400

    def test_request_exception_in_lowercase(self):
        url = "mock://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_error_lowercase.json", "r").read()

        with requests_mock.Mocker() as mock:
            mock.register_uri("GET", url_re, text=content, status_code=200)
            client = Sushi51Client(url, "foo")
            buffer = BytesIO()
            report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert report.errors[0].code == 1001
        assert report.http_status_code == 200

    def test_request_400_without_data(self):
        url = "mock://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = "HTTP 400: Bad request"

        with requests_mock.Mocker() as mock:
            mock.register_uri("GET", url_re, text=content, status_code=400)
            client = Sushi51Client(url, "foo")
            buffer = BytesIO()
            report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        error = report.errors[0]
        assert isinstance(error, TransportError)
        assert error.http_status == 400
        assert report.http_status_code == 400

    def test_request_stringified_error(self):
        url = "mock://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "stringified_error.json", "r").read()

        with requests_mock.Mocker() as mock:
            mock.register_uri("GET", url_re, text=content, status_code=200)
            client = Sushi51Client(url, "foo")
            buffer = BytesIO()
            report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert report.errors[0].code == 2090
        assert report.http_status_code == 200

    def test_request_errors_in_a_list(self):
        url = "mock://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_errors.json", "r").read()

        with requests_mock.Mocker() as mock:
            mock.register_uri("GET", url_re, text=content, status_code=200)
            client = Sushi51Client(url, "foo")
            buffer = BytesIO()
            report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert report.http_status_code == 200
        assert len(report.warnings) == 2
        assert report.warnings[0].code == 1011
        assert report.warnings[1].code == 3060

    def test_request_500_with_sushi_exception(self):
        url = "mock://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")
        content = open(self.data_dir / "naked_error_3000.json", "r").read()

        with requests_mock.Mocker() as mock:
            mock.register_uri("GET", url_re, text=content, status_code=500)
            client = Sushi51Client(url, "foo")
            buffer = BytesIO()
            report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert isinstance(report.errors[0], CounterError)
        assert report.errors[0].code == 3000
        assert report.http_status_code == 500

    def test_request_500_plain_text(self):
        url = "mock://foo.bar.baz/"
        url_re = re.compile(url.replace(".", r"\.") + ".*")

        with requests_mock.Mocker() as mock:
            mock.register_uri("GET", url_re, text="Internal Server Error", status_code=500)
            client = Sushi51Client(url, "foo")
            buffer = BytesIO()
            report = client.get_report_data("tr", "2020-01", "2020-01", output_content=buffer)
        assert len(report.errors) == 1
        assert isinstance(report.errors[0], TransportError)
        assert report.http_status_code == 500
