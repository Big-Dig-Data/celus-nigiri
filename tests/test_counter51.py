from datetime import date
from pathlib import Path

import pytest

from celus_nigiri.counter51 import (
    Counter51DRReport,
    Counter51IRM1Report,
    Counter51IRReport,
    Counter51PRReport,
    Counter51TRReport,
)
from celus_nigiri.record import Author


class TestCounter51Json:
    @pytest.mark.parametrize(
        "report,file",
        (
            (Counter51DRReport, "DR_sample_r51.json"),
            (Counter51IRReport, "IR_sample_r51.json"),
            (Counter51PRReport, "PR_sample_r51.json"),
            (Counter51TRReport, "TR_sample_r51.json"),
        ),
    )
    def test_sample_get_months(self, report, file):
        reader = report()
        path = Path(__file__).parent / "data/counter51" / file
        with path.open("rb") as f:
            assert set(reader.get_months(f)) == {
                date(2022, 1, 1),
                date(2022, 2, 1),
                date(2022, 3, 1),
                date(2022, 4, 1),
                date(2022, 5, 1),
                date(2022, 6, 1),
                date(2022, 7, 1),
                date(2022, 8, 1),
                date(2022, 9, 1),
                date(2022, 10, 1),
                date(2022, 11, 1),
                date(2022, 12, 1),
            }

    def test_sample_pr(self):
        reader = Counter51PRReport()
        path = Path(__file__).parent / "data/counter51/PR_sample_r51.json"
        records = [e for e in reader.file_to_records(path)]
        assert len(records) == 2424

        assert records[0].title is None
        assert not records[0].title_ids
        assert records[0].item is None
        assert not records[0].item_ids
        assert records[0].metric == "Total_Item_Investigations"
        assert records[0].value == 1461
        assert records[0].start == date(2022, 1, 1)
        assert records[0].end is None
        assert records[0].dimension_data == {
            "Access_Method": "Regular",
            "Data_Type": "Article",
            "Platform": "Platform 1",
        }

        assert records[-1].title is None
        assert not records[0].title_ids
        assert records[-1].item is None
        assert not records[0].item_ids
        assert records[-1].metric == "Unique_Item_Requests"
        assert records[-1].value == 833
        assert records[-1].start == date(2022, 12, 1)
        assert records[-1].end is None
        assert records[-1].dimension_data == {
            "Access_Method": "TDM",
            "Data_Type": "Unspecified",
            "Platform": "Platform 1",
        }

    def test_sample_dr(self):
        reader = Counter51DRReport()

        path = Path(__file__).parent / "data/counter51/DR_sample_r51.json"
        records = [e for e in reader.file_to_records(path)]

        assert len(records) == 1824
        assert records[0].title == "Database 1"
        assert records[0].title_ids.Proprietary == "P1:DB1"
        assert records[0].item is None
        assert not records[0].item_ids
        assert records[0].metric == "Total_Item_Investigations"
        assert records[0].value == 1104
        assert records[0].start == date(2022, 1, 1)
        assert records[0].end is None
        assert records[0].dimension_data == {
            "Access_Method": "Regular",
            "Data_Type": "Book",
            "Platform": "Platform 1",
            "Publisher": "Sample Publisher",
        }

        assert records[-1].title == "Database 3"
        assert records[-1].title_ids.Proprietary == "P1:DB3"
        assert records[-1].item is None
        assert not records[0].item_ids
        assert records[-1].metric == "Unique_Item_Requests"
        assert records[-1].value == 545
        assert records[-1].start == date(2022, 12, 1)
        assert records[-1].end is None
        assert records[-1].dimension_data == {
            "Access_Method": "TDM",
            "Data_Type": "Database_Full_Item",
            "Platform": "Platform 1",
            "Publisher": "Sample Publisher",
        }

    def test_sample_tr(self):
        reader = Counter51TRReport()

        path = Path(__file__).parent / "data/counter51/TR_sample_r51.json"
        records = [e for e in reader.file_to_records(path)]

        assert len(records) == 1872
        assert records[0].title == "Title 1"
        assert records[0].title_ids.DOI == "10.9999/xxxxt01"
        assert records[0].title_ids.Proprietary == "P1:T01"
        assert records[0].title_ids.ISBN == "979-8-88888-888-8"
        assert records[0].title_ids.URI == "https://doi.org/10.9999/xxxxt01"
        assert records[0].item is None
        assert not records[0].item_ids
        assert records[0].metric == "Limit_Exceeded"
        assert records[0].value == 49
        assert records[0].start == date(2022, 1, 1)
        assert records[0].end is None
        assert records[0].dimension_data == {
            "Access_Method": "Regular",
            "Access_Type": "Controlled",
            "YOP": "2022",
            "Data_Type": "Book",
            "Platform": "Platform 1",
            "Publisher": "Sample Publisher",
        }

        assert records[-1].title == "Title 11"
        assert records[-1].title_ids.DOI == "10.9999/xxxxt11"
        assert records[-1].title_ids.Proprietary == "P1:T11"
        assert records[-1].title_ids.URI == "https://doi.org/10.9999/xxxxt11"
        assert records[-1].item is None
        assert not records[0].item_ids
        assert records[-1].metric == "Unique_Item_Requests"
        assert records[-1].value == 833
        assert records[-1].start == date(2022, 12, 1)
        assert records[-1].end is None
        assert records[-1].dimension_data == {
            "Access_Method": "TDM",
            "Access_Type": "Controlled",
            "Data_Type": "Unspecified",
            "Platform": "Platform 1",
            "Publisher": "Sample Publisher",
            "YOP": "2018",
        }

    def test_sample_ir(self):
        reader = Counter51IRReport()
        path = Path(__file__).parent / "data/counter51/IR_sample_r51.json"
        records = [e for e in reader.file_to_records(path)]

        assert len(records) == 3120
        assert records[0].title == "Title 1"
        assert records[0].title_ids.DOI == "10.9999/xxxxt01"
        assert records[0].title_ids.Proprietary == "P1:T01"
        assert records[0].title_ids.ISBN == "979-8-88888-888-8"
        assert records[0].title_ids.URI == "https://doi.org/10.9999/xxxxt01"
        assert records[0].item == "Item 3"
        assert records[0].item_ids.DOI == "10.9999/xxxxi03"
        assert records[0].item_ids.Proprietary == "P1:I03"
        assert records[0].item_ids.URI == "https://doi.org/10.9999/xxxxi03"
        assert records[0].item_publication_date == "2022-07-24"
        assert records[0].item_authors == [Author(name="Author 3")]
        assert records[0].metric == "Limit_Exceeded"
        assert records[0].value == 49
        assert records[0].start == date(2022, 1, 1)
        assert records[0].end is None
        assert records[0].dimension_data == {
            "Access_Method": "Regular",
            "Access_Type": "Controlled",
            "YOP": "2022",
            "Data_Type": "Book_Segment",
            "Parent_Data_Type": "Book",
            "Platform": "Platform 1",
            "Publisher": "Sample Publisher",
            # "Article_Version": "VoR",
        }

        assert records[-1].title is None
        assert not records[-1].title_ids
        assert records[-1].item == "Item 25"
        assert records[-1].item_ids.DOI == "10.9999/xxxxi25"
        assert records[-1].item_ids.Proprietary == "P1:I25"
        assert records[-1].item_ids.URI == "https://doi.org/10.9999/xxxxi25"
        assert records[-1].item_publication_date == "2018-02-25"
        assert records[-1].item_authors == [Author(name="Author 25")]
        assert records[-1].metric == "Unique_Item_Requests"
        assert records[-1].value == 833
        assert records[-1].start == date(2022, 12, 1)
        assert records[-1].end is None
        assert records[-1].dimension_data == {
            "Access_Method": "TDM",
            "Access_Type": "Controlled",
            "Data_Type": "Unspecified",
            "Platform": "Platform 1",
            "Publisher": "Sample Publisher",
            "YOP": "2018",
            # "Article_Version": "VoR",
        }

    def test_sample_ir_m1(self):
        """
        Test proper parsing of an IR_M1 report from Counter 5.1 CoP.
        """
        reader = Counter51IRM1Report()
        path = Path(__file__).parent / "data/counter51/IRM1_sample_r51.json"
        records = [e for e in reader.file_to_records(path)]

        assert (
            len(records) == 5 * 12 * 2
        )  # 5 items * 12 months * 2 metrics (Total_Item_Requests, Unique_Item_Requests)

        # Check first record
        assert records[0].title is None
        assert records[0].title_ids.DOI is None
        assert records[0].item == "Item 2"
        assert records[0].item_ids.DOI == "10.9999/xxxxi02"
        assert records[0].item_ids.Proprietary == "P1:I02"
        assert records[0].item_publication_date is None
        assert records[0].item_authors is None
        assert records[0].metric == "Total_Item_Requests"
        assert records[0].value == 767
        assert records[0].start == date(2022, 1, 1)
        assert records[0].end is None
        assert records[0].dimension_data == {
            "Data_Type": "Audiovisual",
            "Platform": "Platform 1",
            "Publisher": "Sample Publisher",
        }
        # Explicitly verify Parent_Data_Type is NOT present
        assert "Parent_Data_Type" not in records[0].dimension_data

        # Check a record with different Data_Type
        image_records = [r for r in records if r.dimension_data.get("Data_Type") == "Image"]
        assert len(image_records) > 0
        image_record = image_records[0]
        assert image_record.dimension_data == {
            "Data_Type": "Image",
            "Platform": "Platform 1",
            "Publisher": "Sample Publisher",
        }
        # Explicitly verify Parent_Data_Type is NOT present
        assert "Parent_Data_Type" not in image_record.dimension_data

        # Check last record
        assert records[-1].title is None
        assert records[-1].title_ids.DOI is None
        assert records[-1].item == "Item 22"
        assert records[-1].item_ids.DOI == "10.9999/xxxxi22"
        assert records[-1].item_ids.Proprietary == "P1:I22"
        assert records[-1].item_publication_date is None
        assert records[-1].item_authors is None
        assert records[-1].metric == "Unique_Item_Requests"
        assert records[-1].value == 804
        assert records[-1].start == date(2022, 12, 1)
        assert records[-1].end is None
        assert records[-1].dimension_data == {
            "Data_Type": "Sound",
            "Platform": "Platform 1",
            "Publisher": "Sample Publisher",
        }
        # Explicitly verify Parent_Data_Type is NOT present
        assert "Parent_Data_Type" not in records[-1].dimension_data
