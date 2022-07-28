import datetime
from pathlib import Path

import pytest

from celus_nigiri import CounterRecord
from celus_nigiri.client import Sushi4Client
from celus_nigiri.counter4 import Counter4BR2Report


class TestErrors:
    @pytest.mark.parametrize(
        "filename,code,severity",
        (
            ("4_BR2_invalid_location1.xml", 1110, "Error"),
            ("4_BR2_not_supported1.xml", 3000, "Error"),
            ("4_BR2_service_not_available1.xml", 1000, "Error"),  # "Fatal" -> "Error"
            ("4_BR2_unauthorized1.xml", 2000, "Error"),
            ("4_BR2_unauthorized2.xml", 2010, "Error"),
            ("4_PR1_invalid_requestor.xml", 2000, "Error"),
            ("sushi_1111.xml", 1111, "Error"),
            ("sushi_1111-severity-number.xml", 1111, "Error"),  # "4" -> "Error"
            ("sushi_1111-severity-missing.xml", 1111, "Error"),
            ("sushi_3030.xml", 3030, "Error"),
        ),
    )
    def test_error_extraction(self, filename, code, severity):

        client = Sushi4Client("https://example.com/", "user")

        with (Path(__file__).parent / "data/counter4/" / filename).open("rb") as f:
            errors = client.extract_errors_from_data(f)
            assert len(errors) == 1
            error = errors[0]
            assert int(error.code) == code
            assert error.severity == severity


class TestSuccess:
    @pytest.mark.parametrize(
        "filename,report,data",
        (
            (
                "counter4_br2_one_month.tsv",
                Counter4BR2Report,
                [
                    CounterRecord(
                        value=0,
                        start=datetime.date(2018, 2, 1),
                        end=None,
                        title='Title1',
                        title_ids={
                            'DOI': '',
                            'ISBN': '111-1-1111-1111-1',
                            'Online_ISSN': None,
                            'Print_ISSN': '',
                            'Proprietary': '111111',
                        },
                        dimension_data={'Platform': 'My Platform', 'Publisher': 'Publisher1'},
                        metric='Book Section Requests',
                        organization=None,
                    ),
                    CounterRecord(
                        value=10,
                        start=datetime.date(2018, 2, 1),
                        end=None,
                        title='Title2',
                        title_ids={
                            'DOI': '',
                            'ISBN': '222-2-2222-2222-2',
                            'Online_ISSN': None,
                            'Print_ISSN': '',
                            'Proprietary': '222222',
                        },
                        dimension_data={'Platform': 'My Platform', 'Publisher': 'Publisher2'},
                        metric='Book Section Requests',
                        organization=None,
                    ),
                    CounterRecord(
                        value=0,
                        start=datetime.date(2018, 2, 1),
                        end=None,
                        title='Title3',
                        title_ids={
                            'DOI': '',
                            'ISBN': '',
                            'Online_ISSN': None,
                            'Print_ISSN': '',
                            'Proprietary': '',
                        },
                        dimension_data={'Platform': 'My Platform', 'Publisher': ''},
                        metric='Book Section Requests',
                        organization=None,
                    ),
                    CounterRecord(
                        value=1,
                        start=datetime.date(2018, 2, 1),
                        end=None,
                        title='Title4',
                        title_ids={
                            'DOI': '',
                            'ISBN': '333-333-33333-3-3',
                            'Online_ISSN': None,
                            'Print_ISSN': '',
                            'Proprietary': '333333',
                        },
                        dimension_data={'Platform': 'My Platform', 'Publisher': 'Publisher3'},
                        metric='Book Section Requests',
                        organization=None,
                    ),
                    CounterRecord(
                        value=1,
                        start=datetime.date(2018, 2, 1),
                        end=None,
                        title='Title5',
                        title_ids={
                            'DOI': '',
                            'ISBN': '444-444-44444-4-4',
                            'Online_ISSN': None,
                            'Print_ISSN': '',
                            'Proprietary': '444444',
                        },
                        dimension_data={'Platform': 'My Platform', 'Publisher': 'Publisher3'},
                        metric='Book Section Requests',
                        organization=None,
                    ),
                ],
            ),
        ),
    )
    def test_tsv_parsing(self, filename, report, data):
        path = Path(__file__).parent / "data/counter4/" / filename
        report_inst = report()
        assert list(report_inst.file_to_records(str(path))) == data
