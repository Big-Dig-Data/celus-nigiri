from datetime import date
from pathlib import Path

import pytest

from celus_nigiri.counter5 import (
    Counter5DRReport,
    Counter5IRM1Report,
    Counter5IRReport,
    Counter5TableReport,
    Counter5TRReport,
)
from celus_nigiri.csv_detect import detect_csv_dialect
from celus_nigiri.exceptions import SushiException


class TestCounter5Reading:
    def test_record_simple_tr(self):
        reader = Counter5TRReport()
        path = Path(__file__).parent / "data/counter5/data_simple.json"
        records = [e for e in reader.file_to_records(path)]
        assert len(records) == 2
        assert records[0].title == "Title1"
        assert records[0].metric == "Total_Item_Investigations"
        assert records[0].value == 10
        assert records[0].start == date(2019, 5, 1)
        assert records[0].end == date(2019, 5, 31)
        assert records[0].value == 10
        assert records[0].dimension_data == {
            "Access_Type": "OA_Gold",
            "Publisher": "Pub1",
            "Access_Method": "Regular",
            "YOP": "2009",
            "Section_Type": "Chapter",
            "Data_Type": "Book",
            "Platform": "PlOne",
        }
        # same item in the data
        for attr in ("title", "start", "end"):
            assert getattr(records[0], attr) == getattr(records[1], attr)
        assert records[1].value == 8

        with path.open("rb") as f:
            assert reader.get_months(f) == []

    def test_reading_incorrect_data(self):
        """
        Test that data that do not have the proper format are not imported and raise an error
        """
        reader = Counter5TRReport()
        with pytest.raises(SushiException):
            [
                e
                for e in reader.file_to_records(
                    Path(__file__).parent / "data/counter5/data_incorrect.json"
                )
            ]

    def test_reading_wrapped_in_body(self):
        """
        Some data come messed up by being wrapped in an extra
        element 'body'.
        Check that we can properly parse this type of data.
        """
        reader = Counter5TRReport()
        path = Path(__file__).parent / "data/counter5/extra_body_wrap.json"
        records = [e for e in reader.file_to_records(path)]
        assert len(records) == 30  # 7 titles, metrics - 1, 5, 5, 2, 6, 5, 6
        with path.open("rb") as f:
            assert reader.get_months(f) == [date(2019, 11, 1)]

    def test_reading_wrapped_in_body_exception(self):
        """
        Some data come messed up by being wrapped in an extra
        element 'body'.
        Check that we can properly parse this type of data.
        """
        reader = Counter5TRReport()
        records = [
            e
            for e in reader.file_to_records(
                Path(__file__).parent / "data/counter5/extra_body_wrap-exception.json"
            )
        ]
        assert len(records) == 0
        assert len(reader.errors) == 1
        error = reader.errors[0]
        assert error.code == "3030"

    def test_reading_wrapped_in_body_exception2(self):
        """
        Another way to mess up the data - body is null and there are exceptions somewhere else :(
        """
        reader = Counter5DRReport()
        records = [
            e
            for e in reader.file_to_records(
                Path(__file__).parent / "data/counter5/extra_body_wrap-exception2.json"
            )
        ]
        assert len(records) == 0
        assert len(reader.errors) == 1
        error = reader.errors[0]
        assert str(error.code) == "1001"

    def test_no_exception_no_data(self):
        """
        There is no exception in the header, but also no data (no data found for such period)
        """
        path = Path(__file__).parent / "data/counter5/no_data.json"
        reader = Counter5TRReport()
        records = [e for e in reader.file_to_records(path)]
        assert len(records) == 0
        assert len(reader.warnings) == 0
        assert not reader.queued

        with path.open("rb") as f:
            assert reader.get_months(f) == [date(2018, 11, 1), date(2018, 12, 1)]

    def test_reading_messed_up_data_error_directly_in_data(self):
        """
        There is no header, just the error in the json
        """
        reader = Counter5TRReport()
        records = [
            e
            for e in reader.file_to_records(
                Path(__file__).parent / "data/counter5/naked_error.json"
            )
        ]
        assert len(records) == 0
        assert len(reader.warnings) == 1
        assert reader.queued

    def test_reading_messed_up_data_error_list_directly_in_data(self):
        """
        There is no header, just the error in the json
        """
        reader = Counter5TRReport()
        records = [
            e
            for e in reader.file_to_records(
                Path(__file__).parent / "data/counter5/naked_errors.json"
            )
        ]
        assert len(records) == 0
        assert len(reader.warnings) == 2
        assert len(reader.errors) == 0
        assert reader.queued

    def test_reading_strigified_exception(self):
        """
        Body is stringified json - header containing error
        """
        reader = Counter5TRReport()
        records = [
            e
            for e in reader.file_to_records(
                Path(__file__).parent / "data/counter5/stringified_error.json"
            )
        ]
        assert len(records) == 0
        assert len(reader.errors) == 1
        error = reader.errors[0]
        assert str(error.code) == "2090"

    def test_severity_as_number(self):
        """
        Severity of error is a number
        """
        reader = Counter5TRReport()
        records = [
            e
            for e in reader.file_to_records(
                Path(__file__).parent / "data/counter5/severity-number.json"
            )
        ]
        assert len(records) == 0
        assert len(reader.errors) == 1
        error = reader.errors[0]
        assert str(error.code) == "3010"
        assert str(error.severity.lower()) == "error"

    def test_severity_is_missing(self):
        reader = Counter5TRReport()
        records = [
            e
            for e in reader.file_to_records(
                Path(__file__).parent / "data/counter5/severity-missing.json"
            )
        ]
        assert len(records) == 0
        assert len(reader.errors) == 1
        error = reader.errors[0]
        assert str(error.code) == "3010"
        assert str(error.severity.lower()) == "error"

    def test_no_extra_ids(self):
        reader = Counter5DRReport()
        counter = 0
        with pytest.raises(SushiException):
            for e in reader.file_to_records(
                Path(__file__).parent / "data/counter5/dr-extra-ids.json"
            ):
                counter += 1
                pass

        assert counter == 7, "Some data were processed"

    def test_bloomsbery_strange_irm1_with_parents(self):
        """
        This is a test for a regression in the parsing of the IR_M1 report.
        In the past, we were able to ingest this data without any issues,
        but after introduction of parents, it crashed on an unexpected list
        in parents. But in IRM1, there should be no parents anyway, so we need
        to make sure that we can handle this case.
        """
        reader = Counter5IRM1Report()
        records = [
            e
            for e in reader.file_to_records(
                Path(__file__).parent / "data/counter5/bloomsbery-parent.IR_M1.json"
            )
        ]
        assert len(records) > 0
        assert all(r.title == "" for r in records)


class TestCounter5TableReports:
    def test_dr_csv(self):
        reader = Counter5TableReport()
        records = list(
            reader.file_to_records(Path(__file__).parent / "data/counter5/counter5_table_dr.csv")
        )
        assert len(records) == 121
        assert records[0].value == 42
        assert records[-1].value == 1

    def test_dr_tsv(self):
        reader = Counter5TableReport()
        records = list(
            reader.file_to_records(Path(__file__).parent / "data/counter5/counter5_table_dr.tsv")
        )
        assert len(records) == 121
        assert records[0].value == 42
        assert records[-1].value == 1

    @pytest.mark.parametrize(
        ["rt", "count", "first_number", "months"],
        [
            (
                "PR",
                42,
                10,
                [
                    date(2017, 1, 1),
                    date(2017, 2, 1),
                    date(2017, 3, 1),
                    date(2017, 4, 1),
                    date(2017, 5, 1),
                    date(2017, 6, 1),
                ],
            ),
            (
                "TR",
                24,
                100,
                [
                    date(2017, 1, 1),
                    date(2017, 2, 1),
                    date(2017, 3, 1),
                    date(2017, 4, 1),
                    date(2017, 5, 1),
                    date(2017, 6, 1),
                ],
            ),
            (
                "DR",
                30,
                1,
                [
                    date(2017, 1, 1),
                    date(2017, 2, 1),
                    date(2017, 3, 1),
                    date(2017, 4, 1),
                    date(2017, 5, 1),
                    date(2017, 6, 1),
                ],
            ),
        ],
    )
    def test_official_example(self, rt, count, first_number, months):
        """
        Takes slightly modified version of the official example reports from COUNTER and tries
        to read them.
        """
        path = Path(__file__).parent / f"data/counter5/COUNTER_R5_Report_Examples_{rt}.csv"
        reader = Counter5TableReport()

        records = list(reader.file_to_records(path))
        assert len(records) == count
        assert records[0].value == first_number

        with path.open("r") as f:
            assert reader.get_months(f, detect_csv_dialect(f)) == months

    def test_mismatched_report_types(self):
        reader = Counter5TableReport()
        with pytest.raises(ValueError):
            records = reader.file_to_records(
                Path(__file__).parent / "data/counter5/counter5_table_mismatched_rt.csv"
            )

            for record in records:
                pass

    def test_item_without_performance(self):
        """
        Test that if the `Performance` key is missing in some of the items, the file will be
        parsed correctly. Added to guard against a regression.
        """
        reader = Counter5TRReport()
        records = list(
            reader.file_to_records(Path(__file__).parent / "data/counter5/no_performance.json")
        )
        assert len(records) == 4
        assert all(r.dimension_data["YOP"] == "2020" for r in records), (
            "all items are from the record for 2020"
        )

    def test_record_ir(self):
        reader = Counter5IRReport()
        path = Path(__file__).parent / "data/counter5/counter5_ir.json"
        records = [e for e in reader.file_to_records(path)]
        assert len(records) == 24
        assert [e.as_csv() for e in records] == [
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal1",
                "Online_ISSN:1111-1111|Print_ISSN:1111-1111|Proprietary:XXXX:11111111",
                "Article1",
                "DOI:10.1111/111.1111111111111111|Online_ISSN:1111-1111|Print_ISSN:1111-1111|Proprietary:XXXX:11111111-111111111-11111",
                "2020-01-01",
                "Author1 (ORCID:0000-0000-0000-0000)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2019",
                "Total_Item_Investigations",
                "1",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal1",
                "Online_ISSN:1111-1111|Print_ISSN:1111-1111|Proprietary:XXXX:11111111",
                "Article1",
                "DOI:10.1111/111.1111111111111111|Online_ISSN:1111-1111|Print_ISSN:1111-1111|Proprietary:XXXX:11111111-111111111-11111",
                "2020-01-01",
                "Author1 (ORCID:0000-0000-0000-0000)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2019",
                "Total_Item_Requests",
                "1",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal1",
                "Online_ISSN:1111-1111|Print_ISSN:1111-1111|Proprietary:XXXX:11111111",
                "Article1",
                "DOI:10.1111/111.1111111111111111|Online_ISSN:1111-1111|Print_ISSN:1111-1111|Proprietary:XXXX:11111111-111111111-11111",
                "2020-01-01",
                "Author1 (ORCID:0000-0000-0000-0000)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2019",
                "Unique_Item_Investigations",
                "1",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal1",
                "Online_ISSN:1111-1111|Print_ISSN:1111-1111|Proprietary:XXXX:11111111",
                "Article1",
                "DOI:10.1111/111.1111111111111111|Online_ISSN:1111-1111|Print_ISSN:1111-1111|Proprietary:XXXX:11111111-111111111-11111",
                "2020-01-01",
                "Author1 (ORCID:0000-0000-0000-0000)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2019",
                "Unique_Item_Requests",
                "1",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article2",
                "DOI:10.2222/222.2222222222222222|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-22222",
                "2021-01-01",
                "Author2 (ISNI:1234567891234567)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Investigations",
                "2",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article2",
                "DOI:10.2222/222.2222222222222222|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-22222",
                "2021-01-01",
                "Author2 (ISNI:1234567891234567)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Requests",
                "2",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article2",
                "DOI:10.2222/222.2222222222222222|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-22222",
                "2021-01-01",
                "Author2 (ISNI:1234567891234567)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Investigations",
                "2",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article2",
                "DOI:10.2222/222.2222222222222222|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-22222",
                "2021-01-01",
                "Author2 (ISNI:1234567891234567)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Requests",
                "2",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article2",
                "DOI:10.2222/222.2222222222222222|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-22222",
                "2021-01-01",
                "Author2 (ISNI:1234567891234567)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform2|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Investigations",
                "3",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article2",
                "DOI:10.2222/222.2222222222222222|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-22222",
                "2021-01-01",
                "Author2 (ISNI:1234567891234567)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform2|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Requests",
                "3",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article2",
                "DOI:10.2222/222.2222222222222222|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-22222",
                "2021-01-01",
                "Author2 (ISNI:1234567891234567)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform2|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Investigations",
                "3",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article2",
                "DOI:10.2222/222.2222222222222222|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-22222",
                "2021-01-01",
                "Author2 (ISNI:1234567891234567)",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform2|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Requests",
                "3",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article3",
                "DOI:10.2222/222.3333333333333333|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Investigations",
                "4",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article3",
                "DOI:10.2222/222.3333333333333333|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Requests",
                "4",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article3",
                "DOI:10.2222/222.3333333333333333|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Investigations",
                "4",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article3",
                "DOI:10.2222/222.3333333333333333|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Requests",
                "4",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article3",
                "DOI:10.2222/222.3333333333333333|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform2|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Investigations",
                "5",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article3",
                "DOI:10.2222/222.3333333333333333|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform2|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Requests",
                "5",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article3",
                "DOI:10.2222/222.3333333333333333|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform2|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Investigations",
                "5",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal2",
                "Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222",
                "Article3",
                "DOI:10.2222/222.3333333333333333|Online_ISSN:2222-2222|Print_ISSN:2222-2222|Proprietary:XXXX:22222222-222222222-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform2|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Requests",
                "5",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal3",
                "Online_ISSN:3333-3333|Print_ISSN:3333-3333|Proprietary:XXXX:33333333",
                "Article4",
                "DOI:10.3333/33.333.3333333333.33333.33|Online_ISSN:3333-3333|Print_ISSN:3333-3333|Proprietary:XXXX:33333333-333333333-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Investigations",
                "6",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal3",
                "Online_ISSN:3333-3333|Print_ISSN:3333-3333|Proprietary:XXXX:33333333",
                "Article4",
                "DOI:10.3333/33.333.3333333333.33333.33|Online_ISSN:3333-3333|Print_ISSN:3333-3333|Proprietary:XXXX:33333333-333333333-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Total_Item_Requests",
                "6",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal3",
                "Online_ISSN:3333-3333|Print_ISSN:3333-3333|Proprietary:XXXX:33333333",
                "Article4",
                "DOI:10.3333/33.333.3333333333.33333.33|Online_ISSN:3333-3333|Print_ISSN:3333-3333|Proprietary:XXXX:33333333-333333333-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Investigations",
                "6",
            ),
            (
                "2020-01-01",
                "2020-01-31",
                "",
                "Journal3",
                "Online_ISSN:3333-3333|Print_ISSN:3333-3333|Proprietary:XXXX:33333333",
                "Article4",
                "DOI:10.3333/33.333.3333333333.33333.33|Online_ISSN:3333-3333|Print_ISSN:3333-3333|Proprietary:XXXX:33333333-333333333-33333",
                "",
                "",
                "Access_Method:Regular|Access_Type:Controlled|Article_Version:VoR|Data_Type:Article|Parent_Data_Type:Journal|Platform:Platform1|Publisher:MyPublisher|YOP:2018",
                "Unique_Item_Requests",
                "6",
            ),
        ]
