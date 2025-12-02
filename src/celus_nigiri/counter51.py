"""
Module dealing with data in the COUNTER51 format.
"""

import json
import typing
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from datetime import date, datetime

import ijson.backends.yajl2_c as ijson

from celus_nigiri.record import Author, CounterRecord

from .counter5 import ALLOWED_ITEM_IDS, CounterError, TransportError
from .error_codes import ErrorCode
from .exceptions import SushiException
from .utils import get_date_range, parse_date_fuzzy


class Counter51ReportBase(metaclass=ABCMeta):
    extra_params: typing.Dict[str, str] = {}

    @property
    @abstractmethod
    def dimensions(self) -> typing.List[str]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def allowed_item_ids(self) -> typing.Dict[str, str]:
        raise NotImplementedError()

    def __init__(
        self,
        report: typing.Optional[typing.IO[bytes]] = None,
        http_status_code=None,
        url: typing.Optional[str] = None,
    ):
        self.url = url
        self.records = []
        self.queued = False
        self.record_found: bool = False  # is populated once `fd_to_dicts` is called
        self.header = {}
        self.errors: typing.List[typing.Union[CounterError, TransportError]] = []
        self.warnings: typing.List[CounterError] = []
        self.infos: typing.List[CounterError] = []
        self.http_status_code = http_status_code

        # Parse it for the first time to extract errors, warnings and infos
        if report:
            self.fd_to_dicts(report)

    def read_report(
        self, header: dict, report_items: typing.Generator[dict, None, None]
    ) -> typing.Generator[CounterRecord, None, None]:
        """
        Reads in the report as returned by the API using Sushi51Client
        """
        for report_item in report_items:
            self.check_item(report_item)
            title = self._item_get_title(report_item)
            title_ids = self._item_get_title_ids(report_item)
            attribute_performances = report_item.get("Attribute_Performance", [])
            for ap in attribute_performances:
                dimension_data = self._extract_dimension_data(self.dimensions, report_item, ap)
                performances = ap.get("Performance", {})
                for metric, data in performances.items():
                    for month, value in data.items():
                        start = datetime.strptime(month, "%Y-%m").date().replace(day=1)

                        yield CounterRecord(
                            value=int(value),
                            metric=metric,
                            start=start,
                            title=title,
                            title_ids=title_ids,
                            dimension_data=dimension_data,
                        )

    def check_header(self, header, fd):
        lower_keys = [e.lower() for e in header]
        for field in ["release", "report_id", "report_name"]:  # mandatory header fields
            if field not in lower_keys:
                raise SushiException("Incorrect format", content=fd.read())

    def check_item(self, item: dict):
        """Check whether the item is valid"""
        self.check_item_root(item)
        if isinstance(item["Attribute_Performance"], list):
            for ap in item["Attribute_Performance"]:
                self.check_performance_extra_ids(ap)

    def check_item_root(self, item: dict):
        if "Attribute_Performance".lower() not in (e.lower() for e in item.keys()):
            raise SushiException(
                "Missing 'Attribute_Performance' field",
                content=json.dumps(item),
            )

    def check_performance_extra_ids(self, item: dict):
        """Check that there are no extra title IDs"""
        allowed = {e.lower() for e in self.allowed_item_ids}
        ids_keys = {e.lower() for e in (item.get("Item_ID", {}) or {}).keys()}
        extra = ids_keys - allowed
        if extra:
            raise SushiException(f"Extra IDs not allowed {extra}", content=json.dumps(item))

    def extract_errors(self, data):
        from .client import Sushi51Client

        sushi_errors = Sushi51Client.extract_errors_from_data(data)

        if any(str(e.code) == str(ErrorCode.PREPARING_DATA.value) for e in sushi_errors):
            # special status telling us we should retry later
            self.queued = True

        for sushi_error in sushi_errors:
            counter_error = CounterError.from_sushi_error(sushi_error)
            if sushi_error.is_warning:
                self.warnings.append(counter_error)
            elif sushi_error.is_info:
                self.infos.append(counter_error)
            else:
                self.errors.append(counter_error)

    def fd_to_dicts(
        self, fd: typing.IO[bytes]
    ) -> typing.Tuple[dict, typing.Generator[dict, None, None]]:
        def empty_generator() -> typing.Generator[dict, None, None]:
            empty: typing.List[dict] = []
            return (e for e in empty)

        # make sure that fd is at the beginning
        fd.seek(0)

        # first check whether it is not an error report
        # skip potential leading whitespace
        first_character = fd.read(1)  # type: bytes
        while first_character.isspace():
            first_character = fd.read(1)
        fd.seek(0)

        if first_character == b"[":
            # entire json can be a list of errors
            self.extract_errors(json.load(fd))
            return {}, empty_generator()

        elif first_character == b'"':
            # stringified header with an error received
            json_string = json.load(fd)
            data = json.loads(json_string)
            self.extract_errors(data)
            header = data.get("Report_Header", {})
            self.header = deepcopy(header)
            return data, empty_generator()

        elif first_character != b"{":
            self.errors.append(
                TransportError(http_status=self.http_status_code, message="Non SUSHI data returned")
            )
            return {}, empty_generator()

        # Try to read the data
        self.record_found = bool(next(ijson.items(fd, "Report_Items.item"), None))
        fd.seek(0)
        if self.record_found:
            items = ijson.items(fd, "Report_Items.item")
        else:
            self.record_found = bool(next(ijson.items(fd, "body.Report_Items.item"), None))
            if self.record_found:
                items = ijson.items(fd, "body.Report_Items.item")
            else:
                items = empty_generator()
        fd.seek(0)

        # try to read the header
        header = next(ijson.items(fd, "Report_Header"), None)
        fd.seek(0)
        # check whether the header is not located in 'body' element
        if not header:
            header = dict(ijson.kvitems(fd, "body.Report_Header"))
            fd.seek(0)

        if not self.record_found and not header:
            # not data and header not found entire json could be a header
            header = json.load(fd)
        fd.seek(0)

        # Extract errors
        if header:
            # Try to extract exceptions from header
            self.extract_errors(header)
            if not any((self.errors, self.warnings, self.infos)):
                # no  error/warning/info = > file has to contain a valid header
                self.check_header(header, fd)
                fd.seek(0)
            self.header = deepcopy(header)
        elif not self.record_found:
            # Header is missing and no data
            raise SushiException("Incorrect format", content=fd.read())
        else:
            # Header is empty, but data are present
            pass

        # error can be placed outside of header
        # look for them only if the header is missing or there are no data records
        # - if we look for them in each case, it can add quite some overhead for large files
        #   (like 20s for a 50 MB file)
        if (
            (not header or not self.record_found)
            and not self.errors
            and not self.warnings
            and not self.infos
        ):
            # Extract exceptions from root
            self.extract_errors(list(ijson.items(fd, "Exceptions.item")))  # In <root>
            fd.seek(0)
            self.extract_errors(dict(ijson.kvitems(fd, "Exception")))  # Single exception in <root>
            fd.seek(0)
            # Extract exception(s) from body
            self.extract_errors(list(ijson.items(fd, "body.Exceptions.item")))
            fd.seek(0)
            self.extract_errors(dict(ijson.kvitems(fd, "body.Exception")))
            fd.seek(0)

        return header, items

    def get_months(self, fd: typing.IO[bytes]) -> typing.List[date]:
        pos = fd.tell()

        header, _ = self.fd_to_dicts(fd)

        # Try to obtain End_Date and Begin_Date from report filter
        date_start_str = None
        date_end_str = None
        if report_filters := header.get("Report_Filters", {}):
            date_start_str = report_filters.get("Begin_Date")
            date_end_str = report_filters.get("End_Date")

        fd.seek(pos)

        date_start = parse_date_fuzzy(date_start_str or "")
        date_end = parse_date_fuzzy(date_end_str or "")
        if date_start and date_end:
            return get_date_range(date_start, date_end)
        else:
            return []

    def file_to_records(self, filename: str) -> typing.Generator[CounterRecord, None, None]:
        f = open(filename, "rb")  # file will be closed later (once generator struct is discarded)
        header, items = self.fd_to_dicts(f)
        return self.read_report(header, items)

    @classmethod
    def file_to_input(cls, filename: str):
        with open(filename, "r", encoding="utf-8") as infile:
            return json.load(infile)

    @classmethod
    def _item_get_title(cls, item):
        return item.get("Title")

    @classmethod
    def _item_get_title_ids(cls, item) -> typing.Dict[str, str]:
        return cls._extract_title_ids(item.get("Item_ID", {}) or {})

    @classmethod
    def _item_get_item_ids(cls, item) -> typing.Dict[str, str]:
        return {}

    @classmethod
    def _item_get_publication_date(cls, item) -> typing.Optional[str]:
        return None

    @classmethod
    def _item_get_authors(cls, item) -> typing.Optional[typing.List[Author]]:
        return None

    @classmethod
    def _item_get_item(self, item) -> typing.Optional[str]:
        return None

    @classmethod
    def _extract_title_ids(cls, values: dict) -> dict:
        ret = {}
        for name, value in values.items():
            if id_type := cls.allowed_item_ids.get(name):
                ret[id_type] = value
        return ret

    @classmethod
    def _extract_dimension_data(cls, dimensions: list, *parts: dict):
        ret = {}
        for dim in dimensions:
            for part in parts:
                if dim in part:
                    ret[dim] = part[dim]
                    break
        return ret


class Counter51DRReport(Counter51ReportBase):
    extra_params: typing.Dict[str, str] = {"attributes_to_show": "Access_Method"}
    dimensions = ["Access_Method", "Data_Type", "Publisher", "Platform"]
    allowed_item_ids = ALLOWED_ITEM_IDS["DR"]

    @classmethod
    def _item_get_title(cls, item):
        return item.get("Database")


class Counter51TRReport(Counter51ReportBase):
    extra_params: typing.Dict[str, str] = {
        "attributes_to_show": "YOP|Access_Method|Access_Type",
    }
    dimensions = [
        "Access_Type",
        "Access_Method",
        "Data_Type",
        "YOP",
        "Publisher",
        "Platform",
    ]
    allowed_item_ids = ALLOWED_ITEM_IDS["TR"]


class Counter51PRReport(Counter51ReportBase):
    extra_params: typing.Dict[str, str] = {"attributes_to_show": "Access_Method"}
    dimensions = ["Access_Method", "Data_Type", "Platform"]
    allowed_item_ids = ALLOWED_ITEM_IDS["PR"]


class Counter51IRReport(Counter51ReportBase):
    extra_params: typing.Dict[str, str] = {
        "attributes_to_show": "Authors|Publication_Date|YOP|Access_Method"
        "|Access_Type|Article_Version",
        "include_parent_details": True,
    }
    dimensions = [
        "Access_Type",
        "Access_Method",
        "Data_Type",
        "YOP",
        "Publisher",
        "Platform",
        "Article_Version",
        "Parent_Data_Type",
    ]
    allowed_item_ids = ALLOWED_ITEM_IDS["IR"]

    def read_report(
        self, header: dict, report_items: typing.Generator[dict, None, None]
    ) -> typing.Generator[CounterRecord, None, None]:
        """
        Reads in the report as returned by the API using Sushi51Client
        """
        for report_item in report_items:
            title = self._item_get_title(report_item)
            title_ids = self._item_get_title_ids(report_item)
            # Data_Type can be present here for Title and
            # in nested data for Item we need to preserve the one for the title
            parent_data_type = report_item.get("Data_Type")
            for item in report_item.get("Items", []):
                self.check_item(item)
                item_name = self._item_get_item(item)
                item_ids = self._item_get_item_ids(item)
                item_publication_date = self._item_get_publication_date(item)
                item_authors = self._item_get_authors(item)
                attribute_performances = item.get("Attribute_Performance", [])
                for ap in attribute_performances:
                    dimension_data = self._extract_dimension_data(
                        self.dimensions, ap, item, report_item
                    )
                    if parent_data_type:
                        dimension_data["Parent_Data_Type"] = parent_data_type
                    performances = ap.get("Performance", {})
                    for metric, data in performances.items():
                        for month, value in data.items():
                            start = datetime.strptime(month, "%Y-%m").date().replace(day=1)

                            yield CounterRecord(
                                value=int(value),
                                metric=metric,
                                start=start,
                                title=title,
                                title_ids=title_ids,
                                item=item_name,
                                item_ids=item_ids,
                                item_publication_date=item_publication_date,
                                item_authors=item_authors,
                                dimension_data=dimension_data,
                            )

    def _item_get_item(self, data: dict):
        return data.get("Item", data)

    @classmethod
    def _item_get_item_ids(cls, item):
        return cls._extract_title_ids(item.get("Item_ID", []) or [])

    @classmethod
    def _item_get_publication_date(cls, item) -> typing.Optional[str]:
        return item.get("Publication_Date")

    @classmethod
    def _item_get_authors(cls, item) -> typing.Optional[typing.List[Author]]:
        res = []
        for author in item.get("Authors", []):
            res.append(
                Author(name=author.get("Name"), ISNI=author.get("ISNI"), ORCID=author.get("ORCID"))
            )
        return res or None


class Counter51IRM1Report(Counter51IRReport):
    extra_params: typing.Dict[str, str] = {}  # no extra params, removes the inherited ones
    # IR_M1 for C51 has Data_Type dimension, which is not present in C5
    dimensions = ["Publisher", "Platform", "Data_Type"]
    allowed_item_ids = ALLOWED_ITEM_IDS["IR_M1"]

    def read_report(
        self, header: dict, report_items: typing.Generator[dict, None, None]
    ) -> typing.Generator[CounterRecord, None, None]:
        """
        Reads in the report as returned by the API using Sushi51Client
        Overrides parent to prevent adding Parent_Data_Type (IR_M1 doesn't use parent data)
        """
        for report_item in report_items:
            title = self._item_get_title(report_item)
            title_ids = self._item_get_title_ids(report_item)
            for item in report_item.get("Items", []):
                self.check_item(item)
                item_name = self._item_get_item(item)
                item_ids = self._item_get_item_ids(item)
                item_publication_date = self._item_get_publication_date(item)
                item_authors = self._item_get_authors(item)
                attribute_performances = item.get("Attribute_Performance", [])
                for ap in attribute_performances:
                    dimension_data = self._extract_dimension_data(
                        self.dimensions, ap, item, report_item
                    )
                    # Note: Parent_Data_Type is NOT added here (unlike Counter51IRReport)
                    performances = ap.get("Performance", {})
                    for metric, data in performances.items():
                        for month, value in data.items():
                            start = datetime.strptime(month, "%Y-%m").date().replace(day=1)

                            yield CounterRecord(
                                value=int(value),
                                metric=metric,
                                start=start,
                                title=title,
                                title_ids=title_ids,
                                item=item_name,
                                item_ids=item_ids,
                                item_publication_date=item_publication_date,
                                item_authors=item_authors,
                                dimension_data=dimension_data,
                            )
