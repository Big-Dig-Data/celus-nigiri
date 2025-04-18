import csv
import enum
import json
import logging
import os
import traceback
import typing
import urllib
from datetime import date, datetime
from io import StringIO
from urllib.parse import urlparse

import requests
import xmltodict
from celus_pycounter import report, sushi
from decouple import Csv, config
from requests import Response

from .counter5 import (
    Counter5DRReport,
    Counter5IRM1Report,
    Counter5IRReport,
    Counter5PRReport,
    Counter5ReportBase,
    Counter5TRReport,
    CounterError,
)
from .counter51 import (
    Counter51DRReport,
    Counter51IRReport,
    Counter51PRReport,
    Counter51ReportBase,
    Counter51TRReport,
)
from .error_codes import ErrorCode, error_code_to_severity
from .exceptions import SushiException
from .utils import begin_month, end_month, parse_date_fuzzy

logger = logging.getLogger(__name__)

ReportClass = typing.TypeVar("ReportClass", "Counter5ReportBase", "Counter51ReportBase")


class SushiError:
    def __init__(self, code="", text="", full_log="", raw_data=None, severity=None, data=None):
        self.code = code
        self.severity = severity if isinstance(severity, str) else error_code_to_severity(code)
        self.text = text
        self.full_log = full_log
        self.data = data
        self.raw_data = raw_data

    def __str__(self):
        return self.full_log

    @property
    def is_warning(self):
        if self.severity:
            return self.severity.lower() == "warning"

        return False

    @property
    def is_info(self):
        if self.severity:
            return self.severity.lower() == "info"

        return False


def convert_key(key: str) -> str:
    # remove namespace
    key = key.split(":", 1)[-1]

    # to lower
    key = key.lower()

    return key


def recursive_finder(
    data: typing.Any, names: typing.List[str]
) -> typing.Generator[typing.Any, None, None]:
    lower_names = [e.lower() for e in names]

    if isinstance(data, list):
        for e in data:
            for found in recursive_finder(e, names):
                yield found
    elif isinstance(data, dict):
        for key, data in data.items():
            if convert_key(key) in lower_names:
                yield data

            if isinstance(data, (list, dict)):
                for found in recursive_finder(data, names):
                    yield found


class SushiClientBase:
    def __init__(self, url, customer_id, requestor_id=None, extra_params=None, auth=None):
        self.url = url
        self.requestor_id = requestor_id
        self.customer_id = customer_id
        self.extra_params = extra_params
        self.auth = auth

    @classmethod
    def extract_errors_from_data(cls, report_data) -> [SushiError]:
        raise NotImplementedError()

    def report_to_string(self, report_data):
        raise NotImplementedError()

    def get_report_data(
        self,
        report_type,
        begin_date,
        end_date,
        output_content: typing.Optional[typing.IO] = None,
        params=None,
    ):
        raise NotImplementedError()


class CounterVersion(str, enum.Enum):
    C5 = "5"
    C51 = "51"

    @property
    def sushi_client_class(self) -> typing.Type[SushiClientBase]:
        if self == CounterVersion.C5:
            return Sushi5Client
        elif self == CounterVersion.C51:
            return Sushi51Client
        else:
            raise NotImplementedError()

    def get_report_class(
        self, report_type: str
    ) -> typing.Union[Counter5ReportBase, Counter51ReportBase]:
        report_type = report_type.lower()
        if self == CounterVersion.C51:
            if report_type == "tr":
                report_class = Counter51TRReport
            elif report_type == "dr":
                report_class = Counter51DRReport
            elif report_type == "pr":
                report_class = Counter51PRReport
            elif report_type == "ir":
                report_class = Counter51IRReport
            else:
                raise NotImplementedError()
        elif self == CounterVersion.C5:
            if report_type == "tr":
                report_class = Counter5TRReport
            elif report_type == "dr":
                report_class = Counter5DRReport
            elif report_type == "pr":
                report_class = Counter5PRReport
            elif report_type == "ir":
                report_class = Counter5IRReport
            elif report_type == "ir_m1":
                report_class = Counter5IRM1Report
            else:
                raise NotImplementedError()
        return report_class


class Sushi5Client(SushiClientBase):
    """
    Client for SUSHI and COUNTER 5 protocol
    """

    CUSTOMER_ID_PARAM = "customer_id"
    REQUESTOR_ID_PARAM = "requestor_id"

    report_types = {
        "tr": {
            "name": "Title report",
            "subreports": {
                "b1": "Book requests excluding OA_Gold",
                "b2": "Books - access denied",
                "b3": "Book Usage by Access Type",
                "j1": "Journal requests excluding OA_Gold",
                "j2": "Journal articles - access denied",
                "j3": "Journal usage by Access Type",
                "j4": "Journal Requests by YOP (Excluding OA_Gold)",
            },
        },
        "dr": {
            "name": "Database report",
            "subreports": {
                "d1": "Search and Item usage",
                "d2": "Database Access Denied",
            },
        },
        "ir": {
            "name": "Item report",
            "subreports": {
                "a1": "Journal article requests",
                "m1": "Multimedia item requests",
            },
        },
        "pr": {"name": "Platform report", "subreports": {"p1": "View by Metric_Type"}},
    }

    def __init__(self, url, customer_id, requestor_id=None, extra_params=None, auth=None):
        super().__init__(url, customer_id, requestor_id, extra_params, auth)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0"
                )
            }
        )
        proxy = self._get_proxy(url)
        if proxy:
            logger.debug("Using proxy %s for server %s", proxy["proxy"], url)
            proxy_rec = "http://{username}:{password}@{proxy}:{port}/".format(**proxy)
            self.session.proxies.update({"http": proxy_rec, "https": proxy_rec})

    def _get_proxy(self, url):
        parsed = urlparse(url)
        proxy_settings = config(
            "SUSHI_PROXIES",
            cast=Csv(cast=Csv(post_process=tuple), delimiter=";"),
            default="",
        )
        for rec in proxy_settings:
            if len(rec) != 5:
                logger.warning("Incorrect proxy definition: [%s]", rec)
                continue
            if not rec[2].isdigit():
                logger.warning("Incorrect proxy port: [%s]", rec[2])
                continue
            if rec[0] == parsed.netloc:
                return {
                    "proxy": rec[1],
                    "port": int(rec[2]),
                    "username": rec[3],
                    "password": rec[4],
                }

    @classmethod
    def _to_date(cls, value) -> date:
        if isinstance(value, str):
            return parse_date_fuzzy(value)
        elif isinstance(value, datetime):
            return value.date()
        elif isinstance(value, date):
            return value
        else:
            raise TypeError()

    @classmethod
    def _encode_date(cls, value: date, long_date_format: bool) -> str:
        """
        >>> Sushi5Client._encode_date('2018-02-30')
        '2018-02'
        >>> Sushi5Client._encode_date(datetime(2018, 7, 6, 12, 25, 30))
        '2018-07'
        """
        if long_date_format:
            # 2020-01-01
            return value.isoformat()[:10]
        else:
            # 2020-01
            return value.isoformat()[:7]

    def _construct_url_params(self, extra=None):
        result = {
            self.CUSTOMER_ID_PARAM: self.customer_id if self.customer_id else self.requestor_id
        }
        if self.requestor_id:
            result[self.REQUESTOR_ID_PARAM] = self.requestor_id
        if self.extra_params:
            result.update(self.extra_params)
        if extra:
            result.update(extra)
        return result

    def _make_request(self, url, params, stream=False):
        logger.debug("Making request to :%s?%s", url, urllib.parse.urlencode(params))
        kwargs = {}
        if self.auth:
            kwargs["auth"] = self.auth
        return self.session.get(url, params=params, stream=stream, **kwargs)

    def make_download_url(self, report_type):
        """
        Prepare download url of a sushi server
        """
        report_type = self._check_report_type(report_type)
        return "/".join([self.url.rstrip("/"), "reports", report_type])

    def make_download_params(
        self, extra_params, begin_date, end_date, long_date_format: bool = True
    ):
        """
        Prepare download params which are used to query sushi server
        """
        params = self._construct_url_params(extra=extra_params)
        params["begin_date"] = self._encode_date(
            begin_month(self._to_date(begin_date)), long_date_format
        )
        params["end_date"] = self._encode_date(end_month(self._to_date(end_date)), long_date_format)
        return params

    def fetch_report_data(
        self,
        report_type,
        begin_date,
        end_date,
        dump_file: typing.Optional[typing.IO] = None,
        params: typing.Optional[dict] = None,
        long_date_format: bool = True,
    ) -> Response:
        """
        Makes a request for the data, stores the resulting data into `dump_file` and returns
        the response object for further inspection
        :param report_type:
        :param begin_date:
        :param end_date:
        :param dump_file: where to put file output
        :param params:
        :return:
        """
        url = self.make_download_url(report_type)
        params = self.make_download_params(
            params, begin_date, end_date, long_date_format=long_date_format
        )
        response = self._make_request(url, params, stream=bool(dump_file))
        if dump_file is not None:
            for data in response.iter_content(1024 * 1024):
                dump_file.write(data)
            dump_file.seek(0)
        return response

    def _get_report_data(
        self,
        report_class: typing.Type[ReportClass],
        report_type,
        begin_date,
        end_date,
        output_content: typing.Optional[typing.IO] = None,
        params=None,
    ) -> ReportClass:
        if os.environ.get("NIGIRI_LONG_DATE_FORMAT_FIRST", "0") == "1":
            long_date_format_tries = (True, False)
        else:
            long_date_format_tries = (False, True)
        for i, long_date_format in enumerate(long_date_format_tries):
            response = self.fetch_report_data(
                report_type,
                begin_date,
                end_date,
                params=params,
                dump_file=output_content,
                long_date_format=long_date_format,
            )
            if 200 <= response.status_code < 300 or 400 <= response.status_code < 600:
                # status codes in the 4xx range may be OK and just provide additional signal
                # about an issue - we need to parse the result in case there is more info
                # in the body
                if output_content:
                    output_content.seek(0)
                report = report_class(
                    output_content, http_status_code=response.status_code, url=response.url
                )
                if any(
                    str(getattr(e, "code", None)) == str(ErrorCode.INVALID_DATE_ARGS.value)
                    for e in report.errors
                ):
                    if i < len(long_date_format_tries) - 1:
                        continue

                return report

        # for other response codes we raise an error - it should be only exotic ones
        response.raise_for_status()

    def get_report_data(
        self,
        report_type,
        begin_date,
        end_date,
        output_content: typing.Optional[typing.IO] = None,
        params=None,
    ) -> Counter5ReportBase:
        report_class = CounterVersion.C5.get_report_class(report_type)
        params = params or {}
        params.update(report_class.extra_params)

        return self._get_report_data(
            report_class, report_type, begin_date, end_date, output_content, params
        )

    @classmethod
    def validate_data(cls, errors: typing.List[CounterError], warnings: typing.List[CounterError]):
        """Checks that the parsed erorrs and warings are not fatal.
            If so, it will raise SushiException
        :param errors: list of errors
        :param warnings: list of warnings
        :return:
        """
        if len(errors) == 1:
            errors[0].raise_me()
        elif len(errors) >= 1:
            message = "; ".join(
                cls._format_error(error.to_sushi_dict()).full_log for error in errors
            )
            raise SushiException(message, content=[e.data for e in errors])

        # log warnings
        for warning in warnings:
            logging.warning(
                "Warning Exception in report: %s",
                cls._format_error(warning.to_sushi_dict()),
            )

    @classmethod
    def extract_errors_from_data(cls, report_data: typing.Union[dict, list]):
        errors: typing.List[SushiError] = []

        # extract exceptions from list
        if isinstance(report_data, list):
            errors = []
            for item in report_data:
                errors += cls.extract_errors_from_data(item)
            return errors

        # exception in Exception object
        for exception in recursive_finder(report_data, ["Exception"]):
            errors.append(cls._format_error(exception))

        # exceptions in Exceptions object
        for exceptions in recursive_finder(report_data, ["Exceptions"]):
            for exception in exceptions:
                errors.append(cls._format_error(exception))

        if not errors:
            # naked exception in root
            naked = cls._format_error(report_data)

            # at least severity and code needs to be present
            if naked.code and naked.severity:
                errors.append(naked)

        return errors

    @classmethod
    def _format_error(cls, exc: dict):
        code = next(recursive_finder(exc, ["Code"]), None)
        severity = error_code_to_severity(code)
        text = next(recursive_finder(exc, ["Message"]), "")
        data = next(recursive_finder(exc, ["Data"]), "")

        if code is None:
            # Some responses contains "number" instead of "code"
            code = next(recursive_finder(exc, ["number"]), "")

        message = f"{severity} error {code}: {text}"
        if data:
            message += f"; {data}"

        error = SushiError(
            code=code,
            text=text,
            full_log=message,
            severity=severity,
            raw_data=exc,
            data=data,
        )
        return error

    def _check_report_type(self, report_type):
        report_type = report_type.lower()
        if "_" in report_type:
            main_type, subtype = report_type.split("_", 1)
        else:
            main_type = report_type
            subtype = None
        if main_type not in self.report_types:
            raise ValueError(f"Report type {main_type} is not supported.")
        if subtype and subtype not in self.report_types[main_type]["subreports"]:
            raise ValueError(f"Report subtype {subtype} is not supported for type {main_type}.")
        return report_type

    def report_to_string(self, report_data):
        return json.dumps(report_data, ensure_ascii=False, indent=2)


class Sushi51Client(Sushi5Client):
    """
    Client for SUSHI and COUNTER 5.1 protocol
    """

    def make_download_url(self, report_type):
        """
        Prepare download url of a sushi server
        """
        report_type = self._check_report_type(report_type)
        # Some platforms (ProQuest) in COUNTER registry contain following urls for C5 and C5.1
        # C5 https://example.com/r5
        # C5.1 https://example.com/r51
        # resulted url for C5.1 should not contain /r51/r51
        stripped_url = self.url.rstrip("/")
        if stripped_url.endswith("/r51"):
            return "/".join([stripped_url, "reports", report_type])
        else:
            return "/".join([stripped_url, "r51/reports", report_type])

    def get_report_data(
        self,
        report_type,
        begin_date,
        end_date,
        output_content: typing.Optional[typing.IO] = None,
        params=None,
    ) -> Counter51ReportBase:
        report_class = CounterVersion.C51.get_report_class(report_type)
        params = params or {}
        params.update(report_class.extra_params)

        return self._get_report_data(
            report_class, report_type, begin_date, end_date, output_content, params
        )


class Sushi4Client(SushiClientBase):
    """
    Client for SUSHI and COUNTER 4 protocol - a simple proxy for the celus_pycounter.sushi
    implementation
    """

    @staticmethod
    def to_pycounter_report_type(celus_report_type: str) -> str:
        """Celus and pycounter report types may differ"""
        CONVERSIONS = {"JR1GOA": "JR1GOA"}
        return CONVERSIONS.get(celus_report_type, celus_report_type)

    # remap of extra params into names that have special meaning for the pycounter client
    extra_params_remap = {"Name": "requestor_name", "Email": "requestor_email"}

    def __init__(self, url, customer_id, requestor_id=None, extra_params=None, auth=None):
        super().__init__(url, requestor_id, customer_id, extra_params, auth)

    @classmethod
    def _encode_date(cls, value) -> str:
        if hasattr(value, "isoformat"):
            return value.isoformat()[:7]
        return value[:7]

    def get_report_data(
        self,
        report_type,
        begin_date,
        end_date,
        output_content: typing.Optional[typing.IO] = None,
        params=None,
    ) -> report.CounterReport:
        kwargs = {"customer_reference": self.customer_id}
        if self.requestor_id:
            kwargs["requestor_id"] = self.requestor_id
        if params:
            # recode params that have special meaning
            for orig, new in self.extra_params_remap.items():
                if orig in params:
                    kwargs[new] = params.pop(orig)
            # put the rest in as it is
            kwargs.update(params)
        if self.auth:
            kwargs["auth"] = self.auth

        report_type = Sushi4Client.to_pycounter_report_type(report_type)
        report = sushi.get_report(
            self.url,
            begin_date,
            end_date,
            report=report_type,
            dump_file=output_content,
            **kwargs,
        )
        return report

    @classmethod
    def extract_errors_from_data(cls, report_data: typing.IO[bytes]):
        try:
            report_data.seek(0)  # set to start
            content = report_data.read().decode("utf8", "ignore")
            parsed = xmltodict.parse(content)
        except Exception as e:
            log = f"Exception: {e}\nTraceback: {traceback.format_exc()}"
            return [SushiError(code="non-sushi", text=str(e), full_log=log, severity="Exception")]
        else:
            errors: typing.List[SushiError] = []
            for exception in recursive_finder(parsed, ["Exception"]):
                if not isinstance(exception, dict):
                    # skip non-object exceptions
                    continue
                code = next(recursive_finder(exception, ["Number"]), "")
                code = code.get("#text", "") if isinstance(code, dict) else code
                message = next(recursive_finder(exception, ["Message"]), "")
                message = message.get("#text", "") if isinstance(message, dict) else message
                severity = error_code_to_severity(code)

                full_log = f"{severity}: #{code}; {message}"
                errors.append(
                    SushiError(
                        code=code,
                        text=message,
                        severity=severity,
                        full_log=full_log,
                        raw_data=exception,
                    )
                )

            if not errors:
                errors.append(
                    SushiError(
                        code="non-sushi",
                        text="Could not find Exception data in XML, probably wrong format",
                        full_log="Could not find Exception data in XML, probably wrong format",
                        severity="Exception",
                    )
                )
            return errors

    def report_to_string(self, report_data: report.CounterReport):
        lines = report_data.as_generic()
        out = StringIO()
        writer = csv.writer(out, dialect="excel", delimiter="\t")
        writer.writerows(lines)
        return out.getvalue()
