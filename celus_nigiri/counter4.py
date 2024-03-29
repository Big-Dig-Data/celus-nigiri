"""
Module dealing with data in the COUNTER4 format as provided by the pycounter library
"""

import typing

from celus_pycounter import report
from celus_pycounter.report import CounterEresource, CounterReport

from .counter5 import CounterRecord


class Counter4ReportBase:
    dimensions = []  # this should be redefined in subclasses
    allowed_item_ids = ["DOI", "Online_ISSN", "Print_ISSN", "ISBN", "Proprietary"]

    dimension_to_attr = {
        "Publisher": "publisher",
        "Content Provider": "content_provider",
        "Platform": "platform",
    }
    title_id_to_attr = {
        "Print_ISSN": "issn",
        "Online_ISSN": "eissn",
        "ISBN": "isbn",
        "DOI": "doi",
        "Proprietary": "proprietary_id",
    }

    def __init__(self):
        self.records = []
        self.header: dict = {}
        self.record_found: bool = False

    def read_report(self, report: CounterReport) -> typing.Generator[CounterRecord, None, None]:
        """
        Reads in the report as returned by the API using Sushi5Client
        :param report:
        :return:
        """
        self.record_found = len(report.pubs) > 0
        return self._read_report(report)

    def _read_report(self, report: CounterReport) -> typing.Generator[CounterRecord, None, None]:
        for journal in report:  # type: CounterEresource
            title = self._extract_title(journal)
            title_ids = self._extract_title_ids(journal)
            dimension_data = self._extract_dimension_data(self.dimensions, journal)
            for start, metric, value in journal:
                yield CounterRecord(
                    start=start,
                    metric=metric,
                    title=title,
                    title_ids=title_ids,
                    dimension_data=dimension_data,
                    value=value,
                )

    def file_to_records(self, filename: str) -> typing.Generator[CounterRecord, None, None]:
        data = self.file_to_input(filename)

        return self.read_report(data)

    @classmethod
    def file_to_input(cls, filename: str):
        return report.parse(filename)

    def _extract_title(self, record: CounterEresource) -> str:
        return record.collection if hasattr(record, "collection") else record.title

    def _extract_title_ids(self, record) -> dict:
        ret = {}
        for title_id in self.allowed_item_ids:
            attr = self.title_id_to_attr.get(title_id, title_id)
            if hasattr(record, attr):
                ret[title_id] = getattr(record, attr)
        return ret

    @classmethod
    def _extract_dimension_data(cls, dimensions: list, record: CounterEresource) -> dict:
        ret = {}
        for dimension in dimensions:
            attr_name = cls.dimension_to_attr.get(dimension, dimension)
            if hasattr(record, attr_name):
                ret[dimension] = getattr(record, attr_name)
        return ret


class Counter4JR1Report(Counter4ReportBase):
    dimensions = ["Publisher", "Platform"]


class Counter4JR2Report(Counter4ReportBase):
    dimensions = ["Publisher", "Access Denied Category", "Platform"]


class Counter4BR1Report(Counter4ReportBase):
    dimensions = ["Publisher", "Platform"]


class Counter4BR2Report(Counter4ReportBase):
    dimensions = ["Publisher", "Platform"]


class Counter4BR3Report(Counter4ReportBase):
    dimensions = ["Publisher", "Platform"]


class Counter4DB1Report(Counter4ReportBase):
    dimensions = ["Publisher", "Platform"]


class Counter4DB2Report(Counter4ReportBase):
    dimensions = ["Publisher", "Access Denied Category", "Platform"]


class Counter4PR1Report(Counter4ReportBase):
    dimensions = ["Publisher", "Platform"]


class Counter4MR1Report(Counter4ReportBase):
    dimensions = ["Content Provider", "Platform"]
